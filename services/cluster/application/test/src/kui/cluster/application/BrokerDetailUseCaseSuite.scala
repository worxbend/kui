package kui.cluster.application

import scala.concurrent.duration.*

import cats.effect.IO
import cats.effect.testkit.TestControl

import kui.cluster.domain.*
import kui.kernel.error.{ApplicationError, ErrorCode, InfrastructureError, KuiError}
import kui.kernel.{BrokerId, ClusterId}

/** The broker pages: what comes from the snapshot, what is read live, and what happens when the live read
  * fails.
  *
  * Two tests carry the design. `brokerListComesFromTheSnapshotAndMakesNoAdminCall` is why a thirty-broker
  * page is one memory read; `configsReturnUnsupportedAndNotAnEmptyList` is why a cluster that refuses to
  * expose its configuration does not look like a broker with no settings.
  */
final class BrokerDetailUseCaseSuite extends munit.CatsEffectSuite {

  private val prod = ClusterProfileFixtures.plaintext("prod", "Production")

  private val broker1 = BrokerId.unsafe(1)
  private val broker2 = BrokerId.unsafe(2)
  private val missing = BrokerId.unsafe(99)

  private val unreachable: KuiError = InfrastructureError.Unreachable("the cluster", "no route")

  private val bigger = TopologyFixtures.replica("orders", 0, 900L)
  private val smaller = TopologyFixtures.replica("orders", 1, 100L)
  private val future = TopologyFixtures.replica("orders", 2, 500L, isFuture = true)

  private val dirs: List[LogDir] =
    List(TopologyFixtures.logDir("/data", List(smaller, bigger, future)))

  private val everyBroker: PartialResult[BrokerId, List[LogDir]] =
    PartialResult.complete(
      Map(broker1 -> dirs, broker2 -> dirs, BrokerId.unsafe(3) -> dirs)
    )

  private val sensitiveEntry = ConfigEntry(
    name = "listener.name.internal.ssl.key.password",
    value = None,
    source = ConfigSource.StaticBroker,
    isSensitive = true,
    isReadOnly = true,
    isDefault = false,
    documentation = None,
    synonyms = Nil
  )

  private val plainEntry = sensitiveEntry.copy(
    name = "advertised.listeners",
    value = Some("PLAINTEXT://broker-1:9092"),
    isSensitive = false,
    source = ConfigSource.DynamicBroker
  )

  private def rig(
      features: ClusterFeatures = TopologyFixtures.allFeatures,
      logDirs: Either[KuiError, PartialResult[BrokerId, List[LogDir]]] = Right(everyBroker),
      configs: Map[BrokerId, Either[KuiError, List[ConfigEntry]]] = Map.empty,
      sweep: Either[KuiError, TopicSweep] = Right(TopicSweep.emptyCluster)
  ) =
    ClusterRig
      .resource(
        List(prod),
        features = features,
        setup = _.set(_.copy(logDirs = logDirs, configs = configs, sweep = sweep))
      )
      .evalTap(ClusterRig.settled)

  /** Four partitions across the three brokers of the default description: broker 1 leads two, brokers 2 and 3
    * one each, and every partition is replicated everywhere.
    */
  private val fourPartitions: TopicSweep = TopologyFixtures.sweep(
    List(
      TopologyFixtures.placement(leader = 1),
      TopologyFixtures.placement(leader = 1),
      TopologyFixtures.placement(leader = 2),
      TopologyFixtures.placement(leader = 3)
    ),
    topics = 2
  )

  test("skippedLiveLogDirsUseStaleSnapshotForDirectoriesAndPartitionSizes") {
    rig().use { built =>
      for {
        _ <- built.admin.set(
          _.copy(logDirs = Right(PartialResult(Map.empty, Map(broker1 -> SkipReason.Failed(unreachable)))))
        )
        result <- built.brokers.logDirsAndSizes(prod.id, broker1)
      } yield result match {
        case Right((found, sizes)) =>
          assertEquals(found.dirs, dirs.sorted)
          assert(found.freshness.isInstanceOf[SnapshotFreshness.Stale])
          assertEquals(sizes.freshness, found.freshness)
        case Left(error) => fail(s"expected stale snapshot, got $error")
      }
    }
  }

  test("unreportedBrokerIsUnavailableRatherThanFreshEmpty") {
    rig(logDirs = Right(PartialResult.empty)).use { built =>
      built.brokers.logDirs(prod.id, broker1).map(result => assert(result.isLeft))
    }
  }

  test("brokerListComesFromTheSnapshotAndMakesNoAdminCall") {
    rig().use { built =>
      for {
        _ <- built.admin.reset
        result <- built.brokers.brokers(prod.id)
        calls <- built.admin.calls
      } yield {
        assertEquals(result.map(_.brokers.size), Right(3))
        assertEquals(calls, Nil, "a list screen must not fan out to the brokers it lists")
      }
    }
  }

  test("brokerListMarksTheController") {
    rig().use { built =>
      built.brokers.brokers(prod.id).map { result =>
        assertEquals(result.map(_.brokers.count(_.isController)), Right(1))
      }
    }
  }

  test("brokerListWithNoControllerHasNoneAndDoesNotFail") {
    val headless = ClusterDescription
      .from(
        kafkaClusterId = None,
        controller = None,
        controllerMode = ControllerMode.KRaft,
        brokers = TopologyFixtures.defaultDescription.brokers,
        authorizedOperations = None
      )
      .toOption
      .get

    ClusterRig
      .resource(List(prod), description = headless)
      .evalTap(ClusterRig.settled)
      .use { built =>
        built.brokers.brokers(prod.id).map { result =>
          assertEquals(result.map(_.brokers.count(_.isController)), Right(0))
        }
      }
  }

  test("brokerListCarriesThePartitionAndLeaderCountsOfACompleteSweep") {
    rig(sweep = Right(fourPartitions)).use { built =>
      built.brokers.brokers(prod.id).map { result =>
        // Every broker holds a replica of all four partitions, and the leader counts add up to four —
        // which is the invariant a reader checks the column against without being told to.
        assertEquals(result.map(_.brokers.flatMap(_.partitions)), Right(List(4, 4, 4)))
        assertEquals(result.map(_.brokers.flatMap(_.leaders)), Right(List(2, 1, 1)))
        assertEquals(result.map(_.brokers.flatMap(_.leaders).sum), Right(4))
        // The leader skew comes off the same census: four leaderships over three brokers is 1.33 each, so
        // broker 1's two are half again over its share and the other two are a quarter under theirs.
        assertEquals(result.map(_.brokers.flatMap(_.leaderSkewPercent)), Right(List(50.0d, -25.0d, -25.0d)))
      }
    }
  }

  test("oneUnreadableTopicWithholdsEveryBrokerRowsPartitionAndLeaderCount") {
    // Not "the affected brokers" and not a smaller number: a sweep that missed a topic cannot say which
    // brokers that topic was on, so every row's figure is withheld. The disk figures beside them are from
    // a different call and survive, which is the point of their being separately optional.
    val partial = fourPartitions.copy(unreadable = Set(kui.kernel.TopicName.unsafe("payments")))

    rig(sweep = Right(partial)).use { built =>
      built.brokers.brokers(prod.id).map { result =>
        assertEquals(result.map(_.brokers.flatMap(_.partitions)), Right(Nil))
        assertEquals(result.map(_.brokers.flatMap(_.leaders)), Right(Nil))
        // It refuses with the count it is derived from, and not with the replica skew beside it, which
        // comes from the log directories and is still a number.
        assertEquals(result.map(_.brokers.flatMap(_.leaderSkewPercent)), Right(Nil))
        assertEquals(result.map(_.brokers.flatMap(_.replicas)), Right(List(2, 2, 2)))
        assertEquals(result.map(_.brokers.flatMap(_.skewPercent)), Right(List(0.0d, 0.0d, 0.0d)))
      }
    }
  }

  test("aSweepThatFailedOutrightWithholdsTheSameFigures") {
    rig(sweep = Left(unreachable)).use { built =>
      built.brokers.brokers(prod.id).map { result =>
        assertEquals(result.map(_.brokers.flatMap(_.leaders)), Right(Nil))
      }
    }
  }

  test("brokerListOfAStaleClusterCarriesStaleFreshness") {
    val scenario = rig().use { built =>
      for {
        _ <- built.admin.set(_.copy(description = Left(unreachable)))
        _ <- IO.sleep(31.seconds)
        result <- built.brokers.brokers(prod.id)
      } yield assert(
        result.toOption.map(_.freshness).exists {
          case SnapshotFreshness.Stale(_, _, _) => true
          case _ => false
        },
        s"expected Stale, got ${result.map(_.freshness)}"
      )
    }

    TestControl.executeEmbed(scenario)
  }

  test("logDirsIsALiveCall") {
    rig().use { built =>
      for {
        _ <- built.admin.reset
        _ <- built.brokers.logDirs(prod.id, broker1)
        calls <- built.admin.callsFor(prod.id)
      } yield assertEquals(calls.count(_ == "describeLogDirs"), 1)
    }
  }

  test("logDirsFallsBackToTheSnapshotWhenTheLiveCallFails") {
    rig().use { built =>
      for {
        _ <- built.admin.set(_.copy(logDirs = Left(unreachable)))
        result <- built.brokers.logDirs(prod.id, broker1)
        calls <- built.admin.callsFor(prod.id)
      } yield result match {
        case Right(view) =>
          assertEquals(view.dirs.size, 1, "the snapshot's directories answer instead")
          assert(
            view.freshness match {
              case SnapshotFreshness.Stale(_, _, _) => true
              case _ => false
            },
            s"a fallback read is stale whatever the snapshot says, got ${view.freshness}"
          )
        case Left(error) => fail(s"the page must still render: $error (admin calls: $calls)")
      }
    }
  }

  test("theFirstSnapshotHoldsLogDirsEvenWhenTheCapabilityProbeIsSlow") {
    // The flake in `logDirsFallsBackToTheSnapshotWhenTheLiveCallFails`, made deterministic.
    //
    // A cluster's two snapshot cells are started together and each loads on its own fiber. The topology
    // load reads the capability cell without blocking, and a capability cell that has not answered yet
    // reports every feature as *unknown* — which makes the topology refresh skip `describeLogDirs`
    // entirely. Whether the first topology snapshot contains any log directories was therefore a race
    // between two fibers, and on a starved machine the topology won it about once in thirty runs. The
    // fallback test then found no directories to fall back to and failed.
    //
    // A delay on the admin port makes the losing order the *only* order, so this test failed every run
    // before `ClusterRig.settled` was taught to wait for the probe and re-read the topology after it.
    ClusterRig
      .resource(List(prod), delay = 20.millis, setup = _.set(_.copy(logDirs = Right(everyBroker))))
      .evalTap(ClusterRig.settled)
      .use { built =>
        for {
          _ <- built.admin.set(_.copy(logDirs = Left(unreachable)))
          result <- built.brokers.logDirs(prod.id, broker1)
          calls <- built.admin.callsFor(prod.id)
        } yield assertEquals(
          result.map(_.dirs.size),
          Right(1),
          s"the settled rig must already hold the snapshot's directories (admin calls: $calls)"
        )
      }
  }

  test("logDirsFailsWhenTheLiveCallFailsAndTheSnapshotHasNothing") {
    rig(logDirs = Right(PartialResult.empty[BrokerId, List[LogDir]])).use { built =>
      for {
        _ <- built.admin.set(_.copy(logDirs = Left(unreachable)))
        result <- built.brokers.logDirs(prod.id, broker1)
      } yield assertEquals(result.left.toOption.map(_.code), Some(unreachable.code))
    }
  }

  test("unknownBrokerIsNotFoundWithNoAdminCall") {
    rig().use { built =>
      for {
        _ <- built.admin.reset
        result <- built.brokers.logDirs(prod.id, missing)
        calls <- built.admin.calls
      } yield {
        // An `ApplicationError` and not an `InfrastructureError`: a mistyped path segment must not
        // dim a capability for everybody else.
        assert(result.left.toOption.exists(_.isInstanceOf[ApplicationError]), s"got $result")
        assertEquals(calls, Nil, "a bad broker id must cost no network call")
      }
    }
  }

  test("unknownClusterIsClusterNotFound") {
    rig().use { built =>
      built.brokers.brokers(ClusterId.unsafe("nope")).map { result =>
        assertEquals(result.left.toOption.map(_.code), Some(ErrorCode.ClusterNotFound))
      }
    }
  }

  test("brokerLookupOnAnUnreachableClusterReportsTheClusterFailureNotBrokerNotFound") {
    // The correct answer to "does broker 3 exist" on a cluster KUI has never reached is "I cannot
    // tell you", and that is an infrastructure failure rather than a 404.
    val scenario = ClusterRig
      .resource(List(prod), setup = _.set(_.copy(description = Left(unreachable))))
      .use { built =>
        for {
          _ <- ClusterRig.settled(built)
          _ <- IO.sleep(31.seconds)
          result <- built.brokers.logDirs(prod.id, broker1)
        } yield assert(
          result.left.toOption.exists(_.isInstanceOf[InfrastructureError]),
          s"expected an InfrastructureError, got $result"
        )
      }

    TestControl.executeEmbed(scenario)
  }

  test("partitionSizesAreSortedLargestFirstAndIncludeFutureReplicas") {
    rig().use { built =>
      built.brokers.partitionSizes(prod.id, broker1).map {
        case Right(sizes) =>
          assertEquals(sizes.partitions.map(_.sizeBytes), List(900L, 500L, 100L))
          // The disk really is holding the arriving copy, and the operator watching it fill needs
          // to see it.
          assertEquals(sizes.partitions.count(_.isFuture), 1)
          assertEquals(sizes.totalBytes, 1500L)
        case Left(error) => fail(s"expected sizes: $error")
      }
    }
  }

  test("logDirsAndSizesIssuesOneAdminCall") {
    rig().use { built =>
      for {
        _ <- built.admin.reset
        _ <- built.brokers.logDirsAndSizes(prod.id, broker1)
        calls <- built.admin.callsFor(prod.id)
      } yield assertEquals(calls.count(_ == "describeLogDirs"), 1)
    }
  }

  test("configsAreLiveSortedAndKeepSensitiveRowsWithNoValue") {
    rig(configs = Map(broker1 -> Right(List(sensitiveEntry, plainEntry)))).use { built =>
      built.brokers.configs(prod.id, broker1, includeDocs = false).map {
        case Right(view) =>
          assertEquals(view.entries.map(_.name), view.entries.map(_.name).sorted)
          // Hiding the row entirely would let an operator conclude a setting is unset when it is
          // set to something they are not allowed to read.
          assertEquals(view.sensitive.map(_.name), List(sensitiveEntry.name))
          assertEquals(view.sensitive.flatMap(_.value), Nil)
          assertEquals(view.dynamic.map(_.name), List(plainEntry.name))
        case Left(error) => fail(s"expected a configuration view: $error")
      }
    }
  }

  test("configsReturnUnsupportedAndNotAnEmptyList") {
    // The named managed-service defect: an empty configuration table and "this cluster does not
    // expose broker configuration" look identical to a user and mean opposite things.
    rig(configs = Map(broker1 -> Left(ApplicationError.Unsupported("broker configuration")))).use { built =>
      built.brokers.configs(prod.id, broker1, includeDocs = false).map { result =>
        assertEquals(result.left.toOption.map(_.code), Some(ErrorCode.Unsupported))
      }
    }
  }

  test("documentationIsNotRequestedWithoutTheCapability") {
    // Asking a 2.5 broker for documentation raises an unsupported-version error and loses the whole
    // call, so the capability decides and the caller's flag can only narrow it.
    rig(
      features = TopologyFixtures.features(ClusterFeature.All - ClusterFeature.ConfigDocumentation),
      configs = Map(broker1 -> Right(Nil))
    ).use { built =>
      for {
        result <- built.brokers.configs(prod.id, broker1, includeDocs = true)
        flag <- built.admin.lastDocsFlag
      } yield {
        assertEquals(flag, Some(false))
        assertEquals(result.map(_.hasDocumentation), Right(false))
      }
    }
  }
}
