package kui.build

import java.nio.file.Files

import munit.FunSuite

/** That `build.mill` still wires the decisions this module tests.
  *
  * [[ForkedDocumentRunSuite]] proves that [[ForkedDocumentRun.decide]] refuses a stale document and names a
  * dead fork. It cannot prove that anything ASKS it. `build.mill` is not a module: it is outside
  * `__.checkFormat`, outside `__.fix` and outside every test module (TD-036), so the two lines that carry
  * `decide`'s verdict into the build are reachable by no ordinary suite — and one of them is a `Task.fail`
  * that one word turns into a log line. Measured on this tree, with the decision itself untouched:
  * `case Left(finding) => Task.fail(finding)` → `Task.log.info(finding)` leaves `build-tests.test` at 137/137
  * SUCCESS and `./mill services.topic.api.openApiCheck` at **629/629 SUCCESS over a committed `openapi.json`
  * with a path deleted from it**, printing the generator's own complaint as information.
  *
  * So this suite reads the file as text. That is a weaker instrument than a case over a value and it is said
  * plainly: it cannot tell whether the build WORKS, only whether the two sentences that make it fail are
  * still written. What it buys is that removing them is no longer one silent word — it is a diff in a file
  * whose every line is quoted here, which is the same trade `-Wunused` makes and the reason wave 10's hunter
  * counted it as a gate.
  *
  * EVERY ASSERTION BELOW HAS AN ANCHOR THAT WOULD FAIL FIRST. A source-reading guard whose pattern has
  * stopped matching answers "no violations" over nothing at all, which is how a green gate measures nothing.
  * Each `assert` that forbids something is therefore preceded by one that requires the shape it is written
  * against to still be found.
  */
final class BuildWiringSuite extends FunSuite {

  /** The files `build.mill` told Mill this module's tests read, as `path@hash` pairs.
    *
    * `build-tests.test.buildWiringInputs` declares them and `forkEnv` stamps them into the environment the
    * test JVM starts with, which is what puts them in the input set of the cached test task. The variable is
    * therefore two things at once: the mechanism, and the only evidence a suite can have that the mechanism
    * is still wired. Absent means somebody deleted the declaration, and every read below then fails rather
    * than passing over a tree Mill would not have noticed changing.
    */
  private lazy val declaredInputs: Set[String] =
    sys.env
      .getOrElse(
        "KUI_BUILD_WIRING_INPUTS",
        fail(
          "KUI_BUILD_WIRING_INPUTS is not set, so build.mill no longer declares the files this suite reads " +
            "as inputs of build-tests.test. Measured at the wave-12 open with it undeclared: " +
            "`./mill build-tests.test.testCached` printed 137/137 SUCCESS in two seconds, without running " +
            "a suite, over a ci.yml whose docs.errorCodes step had stopped passing --check. Restore " +
            "`buildWiringInputs` and the `forkEnv` override on build-tests.test."
        )
      )
      .split(' ')
      .filter(_.nonEmpty)
      .map(_.takeWhile(_ != '@'))
      .toSet

  /** Reads a repository file, and refuses to read one Mill was never told about.
    *
    * THE RULE THIS PACKET OWNS, and it is about the NEXT file rather than the three here. A case in this
    * suite that opens a fourth repository file is a case whose subject is outside the cached task's input
    * set: mutate that file and `testCached` answers out of the previous tree. Routing every read through one
    * place makes adding the file to `build.mill` the only way to add the read, so the hole cannot be reopened
    * one `Files.readString` at a time.
    */
  private def readDeclared(relative: String): String = {
    assert(
      declaredInputs.contains(relative),
      clue = s"this suite reads $relative and build-tests.test.buildWiringInputs does not declare it, so " +
        s"Mill's cached test task has no input that moves when $relative does. Declared: " +
        declaredInputs.toList.sorted.mkString(", ")
    )
    Files.readString(design.DesignSources.repositoryRoot.resolve(relative))
  }

  /** This suite's own source text.
    *
    * Deliberately NOT routed through [[readDeclared]], and the reason is the rule rather than an exception to
    * it: this file is a source of `build-tests.test`, so Mill already invalidates the cached test task when
    * it moves. Declaring a module's own source as an extra input would be a second, weaker copy of a
    * mechanism the build has. It is read with `scala.io.Source` and not with the java.nio call, so that the
    * case below can count the java.nio call sites in this file and find exactly the one inside
    * [[readDeclared]] — a reader that used the same API could not count itself out.
    */
  private def ownSource(): String = {
    val file = design.DesignSources.repositoryRoot.resolve(BuildWiringSuite.ownPath).toFile
    val handle = scala.io.Source.fromFile(file, "UTF-8")
    try handle.mkString
    finally handle.close()
  }

  /** `build.mill` as it ships. The repository is located the way the design suites locate it — by the
    * presence of this very file — so there is one definition of "the root" in this module.
    */
  private lazy val buildFile: String = readDeclared("build.mill")

  /** `build.mill` with its comment lines removed.
    *
    * The rule below is written receiver-independently -- `runMain(` with or without a dot in front of it --
    * and `build.mill`'s own scaladoc for `runDocumentMain` quotes ``runMain(...)()`` while explaining why it
    * is not used. Reading the prose would make the assertion fail on the sentence that argues for it, so the
    * prose is dropped and only code is read. Scaladoc continuation lines start with `*`, so they go too.
    */
  private lazy val buildCode: String =
    buildFile.linesIterator
      .filterNot { line =>
        val trimmed = line.trim
        trimmed.startsWith("//") || trimmed.startsWith("*") || trimmed.startsWith("/*")
      }
      .mkString("\n")

  test("everyForkedDocumentGeneratorGoesThroughRunDocumentMain") {
    // THE ANCHOR. Twenty-three call sites as this is written: ten `openApiCheck` pairs, `docs.errorCodes`
    // and `frontend.apiConstants`, plus the definition. If this number collapses the assertion below is
    // being made against a file that no longer names the task, and that is the failure to report.
    val routed = "\\.?runDocumentMain\\(".r.findAllIn(buildFile).size
    assert(routed >= 20, clue = s"build.mill names runDocumentMain $routed times, which is too few to be it")

    // AND THE RULE. `runMain(...)()` reports a dead child as the bare `Subprocess failed` with no exit
    // code and no stderr, under a task key detached from the one that asked for it. Two waves spent an
    // hour each telling that apart from a genuinely stale document, W10-05 wrote `runDocumentMain` to end
    // it, and two call sites were left on the old call anyway -- one of them `frontend.apiConstants`,
    // which `.github/workflows/ci.yml` runs on every push. This is what stops a third.
    //
    // THE BYPASS THIS PATTERN USED TO HAVE, filed by this packet's verifier as W11-03's first finding.
    // The rule was `\.runMain\(`, which requires a RECEIVER DOT -- and every `openApiCheck` body calls the
    // task bare, `runDocumentMain(...)`, because it is a method of the enclosing module. So
    // `runDocumentMain(...)` -> `runMain(...)` inside a body was invisible to this case: measured on this
    // tree, `build-tests.test` stayed at 137/137 SUCCESS and, with `/capabilities` deleted from
    // `services/cluster/api/openapi.json`, `./mill services.cluster.api.openApiCheck` printed
    // `wrote .../openapi.json` and `642/642, SUCCESS` -- the deleted path silently back in the committed
    // file. The lookbehind forbids a letter and allows a dot, so both the bare call and the qualified one
    // are read.
    val legacy = "(?<![A-Za-z])runMain\\(".r.findAllIn(buildCode).toList
    assertEquals(
      legacy,
      Nil,
      clue = "build.mill calls runMain for a document generator. Use runDocumentMain: it captures the " +
        "fork's two streams and reports silence on both as its own finding, which runMain renders " +
        "identically to a stale document."
    )
  }

  test("everyOpenApiCheckStillChecksRatherThanWrites") {
    // THE CHEAPEST ATTACK ON THIS PACKET'S OWN WORK, found in its last hour. `openApiCheck` and
    // `openApi` are the same generator with one argument between them, and `runDocumentMain` cannot
    // tell them apart: a task that was never asked to CHECK exits 0 by rewriting the file. Measured
    // on this tree, with `"--check"` deleted from `services.topic.api.openApiCheck` and one path
    // deleted from its committed document at the same time:
    //
    //   629] services.topic.api.openApiCheck wrote .../services/topic/api/openapi.json
    //   629/629, SUCCESS      -- and the deleted path was back in the file
    //
    // Every other guard in this module is satisfied by that mutation, including the one above: the
    // decision IS asked and IS acted on, over a run whose answer was never in doubt. So the
    // argument is asserted, per target, and this is the only thing that reads it.
    val bodies =
      "def openApiCheck\\(\\): Command\\[Unit\\] = Task\\.Command \\{\\s*runDocumentMain\\([^)]*\\)".r
        .findAllIn(buildFile)
        .toList

    // The anchor, and it is an EQUALITY rather than a floor since wave 11. `>= 9` over ten declared
    // targets tolerated exactly one target dropping out of `bodies` -- and a target whose body stops
    // matching this shape is precisely the target that is no longer being checked for `"--check"`, so the
    // floor excused the one case it existed to catch. The count of `def openApiCheck()` declarations is
    // derived from the same file by a different expression, so the two cannot be brought into agreement by
    // one edit: a body rewritten out of this shape now fails here instead of vanishing from the list.
    val declared = "def openApiCheck\\(\\): Command\\[Unit\\]".r.findAllIn(buildFile).size
    assert(
      declared >= 9,
      clue = s"build.mill declares $declared openApiCheck targets, which is too few to be the ten " +
        "services that publish a document; the derivation has stopped matching."
    )
    assertEquals(
      bodies.size,
      declared,
      clue = s"build.mill declares $declared openApiCheck targets and ${bodies.size} of them are bodies " +
        "this case can read. A target outside the shape is a target whose \"--check\" argument is " +
        "asserted by nothing, which is how a silently rewriting gate gets back in."
    )

    val writing = bodies.filterNot(_.contains("\"--check\""))
    assertEquals(
      writing,
      Nil,
      clue = "an openApiCheck target does not pass \"--check\", so it REWRITES the committed document " +
        "instead of comparing against it and can never disagree with anything again. A stale document " +
        "is then repaired in silence and the gate reports SUCCESS."
    )
  }

  test("runDocumentMainStillFailsTheBuildOnTheDecisionItReads") {
    // The two lines that carry `ForkedDocumentRun`'s verdict into Mill, quoted. Both are anchors and
    // assertions at once: the first proves the build asks, the second proves it acts on the answer.
    assert(
      buildFile.contains("ForkedDocumentRun.decide(mainClass, ran.exitCode, ran.out.text(), ran.err.text())"),
      clue = "runDocumentMain no longer asks ForkedDocumentRun.decide, so every case in " +
        "ForkedDocumentRunSuite is now about code the build does not run."
    )
    assert(
      buildFile.contains("case Left(finding) => Task.fail(finding)"),
      clue = "runDocumentMain reads the decision and does not fail on it. A `Left` that becomes " +
        "Task.log.info leaves every openApiCheck green over a stale committed document -- measured, " +
        "629/629 SUCCESS -- because the generator's complaint is then printed as information."
    )
  }

  test("theInterfaceTypeGateStillReadsTheBrowserSuite") {
    // A GATE ABOUT A GATE, and the third of this wave's three holes of the same shape. Filed as
    // W11-02/F-2. `frontend/e2e/**` -- 5,169 lines, the largest body of TypeScript outside
    // `packages/` -- was outside every type gate for eight waves, because `tsc --build` at the root
    // checks the ten package projects and `e2e/tsconfig.json` is `composite: false` and cannot be a
    // reference. W11-02 repaired it by making `typecheck` TWO `tsc` invocations, and the repair lives
    // entirely inside a string in `package.json` that nothing anywhere asserts: measured, deleting
    // the second invocation leaves `pnpm -C frontend typecheck` at exit 0 printing only
    // `$ tsc --build --pretty false`, over a tree whose `shell.spec.ts` holds
    // `const x: number = "not a number";` -- the identical tree exits 2 under the shipped script, and
    // `.github/workflows/ci.yml` runs `pnpm typecheck` and inherits whatever the script says.
    //
    // Read here rather than in a vitest case because vitest is configured by the same workspace this
    // is about, and because this module already owns the two gates-about-gates above. It is a text
    // read for the reason the whole suite is: it cannot tell whether the gate WORKS, only whether the
    // sentence that makes it run is still written.
    val manifest = readDeclared("frontend/package.json")

    val script = "\"typecheck\": \"([^\"]*)\"".r
      .findFirstMatchIn(manifest)
      .map(_.group(1))
      .getOrElse(fail("frontend/package.json declares no `typecheck` script at all"))

    // The anchor: the root solution build, which is the half that was never in doubt.
    assert(
      script.contains("tsc --build"),
      clue = s"frontend's typecheck script no longer runs the root solution build: `$script`"
    )
    assert(
      script.contains("e2e/tsconfig.json"),
      clue = s"frontend's typecheck script is `$script`, which does not name e2e/tsconfig.json. The " +
        "browser suite is then outside every type gate again -- `tsc --build` cannot reach it, " +
        "because that project is composite: false and cannot be a reference -- and CI's " +
        "`pnpm typecheck` step exits 0 over it."
    )
  }

  test("everyDocumentGateTheWorkflowRunsAsksItToCheckRatherThanWrite") {
    // THE SAME HOLE ON THE YAML SIDE, filed by this packet's verifier as W11-03's third finding.
    // `openApiCheck` is a target and bakes its own `"--check"` in, which the case above asserts per target.
    // The other two document gates do not: `docs.errorCodes` and `frontend.apiConstants` take the argument
    // from whoever invokes them, and the only invoker is `.github/workflows/ci.yml`. Measured on this
    // tree: `run: ./mill docs.errorCodes --check` -> `run: ./mill docs.errorCodes` left
    // `build-tests.test` and `./scripts/run-tests.sh` both green, because nothing in this repository read
    // `.github/workflows/**` at all -- and in the runner that step regenerates `docs/api/error-codes.md`
    // and exits 0, so the committed table can never disagree with the Scala again.
    //
    // Only `run:` lines are read. The job's own comment two screens up names `./mill docs.errorCodes`
    // without the argument while explaining what a person types to REGENERATE the file, and that sentence
    // is correct; a case that read the prose would refuse it.
    val workflow = readDeclared(".github/workflows/ci.yml")

    val invocations = workflow.linesIterator
      .map(_.trim)
      .filter(line => line.startsWith("run:") && line.contains("./mill "))
      .filter(line => line.contains("docs.errorCodes") || line.contains("frontend.apiConstants"))
      .toList

    // The anchor. Both generators are run by this workflow; if this finds fewer, the steps were renamed or
    // deleted and the rule below is being enforced over nothing.
    assertEquals(
      invocations.size,
      2,
      clue = s"ci.yml runs ${invocations.size} of the two hand-invoked document gates: $invocations. " +
        "A gate that stopped being run is a stronger finding than one run without --check."
    )

    val writing = invocations.filterNot(_.contains("--check"))
    assertEquals(
      writing,
      Nil,
      clue = "a ci.yml step runs a document generator without --check, so the step REGENERATES the " +
        "committed file and exits 0 instead of comparing against it. docs/api/error-codes.md and " +
        "frontend/packages/api/src/constants.generated.ts can then drift from the Scala for ever with " +
        "the workflow green."
    )
  }

  test("theCachedTestTaskIsInvalidatedByTheFilesThisSuiteReads") {
    // THE GATE ON THE OTHER FOUR CASES, and the one thing in this suite that is about Mill rather than
    // about a sentence. Every case above reads a file that is not a source of this module, so until wave 12
    // the cached test task's input set did not mention any of them. Measured at the wave-12 open, with
    // `--check` deleted from ci.yml's `docs.errorCodes` step and nothing else changed:
    //
    //   ./mill --no-daemon build-tests.test              -> 1 FAILED, no `clean`  (a command: it re-runs)
    //   ./mill --no-daemon build-tests.test.testCached   -> 137/137 SUCCESS in 2s, no suite output at all
    //
    // The second answer is the previous tree's, and `scripts/run-tests.sh` is one selector away from it.
    // `buildWiringInputs` in build.mill declares the three files and `forkEnv` stamps their hashes into the
    // environment, which is what makes the cached task move when they do -- a `Task.Sources` nothing reads
    // the VALUE of would invalidate nothing.
    val stamps = sys.env.getOrElse("KUI_BUILD_WIRING_INPUTS", "")
    assert(
      stamps.nonEmpty,
      clue = "KUI_BUILD_WIRING_INPUTS is unset: build-tests.test no longer declares what this suite reads, " +
        "and `testCached` is back to answering out of the previous tree."
    )

    // Each entry is `path@hash`, and the hash half is the whole mechanism: two runs over two different
    // trees must not produce the same string. A declaration that stamped the path alone would read as
    // wired here and cache exactly as it did before.
    val unstamped = stamps.split(' ').filter(entry => !entry.contains('@') || entry.endsWith("@")).toList
    assertEquals(
      unstamped,
      Nil,
      clue = s"KUI_BUILD_WIRING_INPUTS carries an entry with no hash after it: $unstamped. The stamp is " +
        "what changes between two trees; a bare path is a constant and invalidates nothing."
    )

    assertEquals(
      declaredInputs,
      Set(
        "build.mill",
        ".github/workflows/ci.yml",
        "frontend/package.json",
        "deployment/frontend/Dockerfile"
      ),
      clue = s"the files declared as inputs of build-tests.test are ${declaredInputs.toList.sorted}. This " +
        "case is the roster: a file added to build.mill's `buildWiringInputs` belongs here, and a file " +
        "this suite reads without being declared fails in `readDeclared` instead."
    )
  }

  test("everyRepositoryFileThisSuiteReadsIsRoutedThroughReadDeclared") {
    // THE GATE ON `readDeclared` ITSELF, filed by this wave's verification pass over W12-02 as V2 and
    // reproduced by this pass before it was closed. `readDeclared`'s own scaladoc says the hole "cannot be
    // reopened one Files.readString at a time" — and until this case that sentence was a convention with no
    // enforcement. Measured on 2026-09-12: a sixth case added to this suite reading
    // `frontend/pnpm-workspace.yaml` straight off the repository root was 139/139 SUCCESS, and then
    // `build-tests.test.testCached` over a MUTATED pnpm-workspace.yaml (`packages/*` -> `pkgs/*`) was
    // 139/139 SUCCESS again in two seconds with no suite output at all — the previous tree's answer, which
    // is exactly the defect TD-052 was filed for.
    //
    // The count is of the java.nio call SHAPE, with its open parenthesis, so that this comment and the
    // clues below can name the method without changing the number. `ownSource` reads this file through
    // `scala.io.Source` precisely so that it does not count itself.
    val source = ownSource()

    // The anchors, both of them: a pattern that has stopped matching answers "no violations" over nothing.
    val declaredAt = source.indexOf("private def readDeclared(relative: String)")
    assert(
      declaredAt >= 0,
      clue = "this suite no longer declares `readDeclared`, so the rule below is enforced over nothing. " +
        s"Read ${BuildWiringSuite.ownPath}, ${source.length} characters."
    )
    val boundary = source.indexOf("private lazy val buildFile")
    assert(
      boundary > declaredAt,
      clue = "`buildFile` no longer follows `readDeclared` in this file, so the containment test below " +
        "cannot say which method the read sits in. Move the case, not the assertion."
    )

    val directReads = BuildWiringSuite.directReadShape.r.findAllMatchIn(source).map(_.start).toList
    assertEquals(
      directReads.size,
      1,
      clue = s"this suite makes ${directReads.size} direct java.nio file reads and exactly one is allowed: " +
        "the one inside `readDeclared`. A case that opens a repository file any other way has a subject " +
        "outside the cached test task's input set — mutate that file and `testCached` answers out of the " +
        "previous tree, green, in two seconds, without running a suite. Route the read through " +
        "`readDeclared` and declare the file in build.mill's `buildWiringInputs`."
    )
    assert(
      directReads.head > declaredAt && directReads.head < boundary,
      clue = s"this suite's one direct java.nio read sits at character ${directReads.head}, outside " +
        s"`readDeclared` (which spans $declaredAt to $boundary). The count alone cannot tell a read " +
        "inside the checked helper from one that replaced it."
    )

    val delegated = BuildWiringSuite.delegatedReadShape.r.findAllMatchIn(source).toList
    assertEquals(
      delegated.size,
      1,
      clue = s"this suite makes ${delegated.size} `scala.io.Source` reads and exactly one is allowed: " +
        "`ownSource`, which reads this file. A second one is the same hole through the other API — the " +
        "count above would stay at one while an undeclared repository file was being read."
    )
  }

  test("everyJobAndStepTheWorkflowDeclaresIsNamedInThisRoster") {
    // FILED AS V3 BY THE VERIFICATION PASS OVER W12-02, and reproduced by this pass: the guard W12-02 added
    // to ci.yml to close "a filter that matches nothing exits 0" is itself read by nothing. Measured on
    // 2026-09-12 — deleting that six-line `grep -q '"name": "@kui/api"'` guard left
    // `./mill --no-daemon build-tests.test` at 139/139 SUCCESS; deleting the ENTIRE 126-line `compose` job
    // left it at 139/139; deleting the whole "The interface image copies every workspace manifest" step
    // was invisible for the same reason.
    //
    // The case above reads two `run:` lines out of this workflow. That is a rule about what a named step
    // does; nothing was a rule about WHICH STEPS EXIST. So the roster is pinned here, and a job or a step
    // that goes away is a red case rather than a silent deletion. A step that is ADDED is red too, which is
    // the point: this list is where somebody says out loud that CI grew a check.
    val workflow = readDeclared(".github/workflows/ci.yml")

    val jobsAt = workflow.indexOf("\njobs:\n")
    assert(
      jobsAt >= 0,
      clue = "ci.yml declares no `jobs:` mapping at all; the roster below would be read out of the `on:` " +
        "and `env:` blocks instead."
    )
    val jobs = raw"(?m)^  ([a-z][a-z0-9_-]*):[ \t]*$$".r
      .findAllMatchIn(workflow.substring(jobsAt + "\njobs:\n".length))
      .map(_.group(1))
      .toList

    assertEquals(
      jobs,
      List(
        "compile",
        "style",
        "architecture",
        "security",
        "generated",
        "test",
        "frontend",
        "browser",
        "compose"
      ),
      clue = s"ci.yml declares the jobs $jobs. A deleted job takes every gate inside it with it and no " +
        "suite in this repository could see that happen; a new job belongs in this list, deliberately."
    )

    val steps = workflow.linesIterator
      .map(_.trim)
      .filter(_.startsWith("- name:"))
      .map(_.stripPrefix("- name:").trim)
      .toList

    assertEquals(
      steps.sorted,
      BuildWiringSuite.workflowSteps.sorted,
      clue =
        s"ci.yml runs ${steps.size} named steps and this roster pins ${BuildWiringSuite.workflowSteps.size}. " +
          "Compared as a sorted LIST and not a set, because `Install pnpm` appears three times while " +
          "`Install` and `Install the pinned browser` each appear twice; a set would let duplicates be " +
          "deleted silently."
    )
  }

  test("theInterfaceImageCopiesEachManifestIntoItsOwnPackageDirectory") {
    // FILED AS V1 BY THE VERIFICATION PASS OVER W12-02, and it is the defect W12-02 repaired, one level up.
    // ci.yml's "The interface image copies every workspace manifest" step reads three facts out of this
    // Dockerfile and compares them as THREE INDEPENDENT SETS: the sources against `ls frontend/packages`,
    // the destinations against the same, and a fixture for each derivation. No derivation anywhere pairs
    // the package on the LEFT of a line with the directory on the RIGHT of the SAME line. Measured on
    // 2026-09-12 by swapping the destinations of two real packages —
    //   COPY frontend/packages/api/package.json ./packages/kernel/
    //   COPY frontend/packages/kernel/package.json ./packages/api/
    // — both halves still naming packages that exist: the step printed `frontend/packages: 11 packages,
    // 11 manifests copied, every name matched` and exited 0, over an image in which packages/api/package.json
    // declares "name": "@kui/kernel" and vice versa. Any permutation of the eleven destinations passes.
    // The step's `typo` fixture only catches a destination that is not a package name at all.
    //
    // So this case derives PAIRS and compares the two halves of each one. It is a different assertion from
    // the step's, not a copy of it: the step answers "every package is copied", this one answers "every
    // package is copied to itself", and neither implies the other.
    val dockerfile = readDeclared("deployment/frontend/Dockerfile")

    // The derivation, driven over a fixture line before it is trusted over the real file — the shape this
    // whole module is built on. The fixture is the exact mutation above, which the shipped step passes.
    val fixture = "COPY frontend/packages/api/package.json ./packages/kernel/"
    val probe = BuildWiringSuite.manifestPairs(fixture)
    assertEquals(
      probe,
      List("api" -> "kernel"),
      clue = s"the COPY-line pair derivation read $probe out of a line copying packages/api's manifest " +
        "into ./packages/kernel/. A derivation that cannot see that line cannot see it in the real file " +
        "either, and the assertion below would be enforced over nothing."
    )

    val pairs = BuildWiringSuite.manifestPairs(dockerfile)
    assertEquals(
      pairs.size,
      11,
      clue = s"deployment/frontend/Dockerfile carries ${pairs.size} anchored `COPY " +
        "frontend/packages/<name>/package.json ./packages/<name>/` lines and eleven packages exist. This " +
        "is the anchor: a roster that has stopped being read derives nothing and every rule below it is " +
        "true of an empty list. A twelfth package moves this number deliberately."
    )

    val misdirected = pairs.filter { case (from, into) => from != into }
    assertEquals(
      misdirected,
      Nil,
      clue = s"deployment/frontend/Dockerfile copies $misdirected — a package's manifest into another " +
        "package's directory. Both names are real packages, so ci.yml's manifest step compares two equal " +
        "sets and exits 0; the image it builds has `packages/<a>/package.json` declaring `@kui/<b>`, and " +
        "pnpm resolves a workspace in which two packages have swapped identities."
    )
  }
}

private object BuildWiringSuite {

  /** This file, relative to the repository root. */
  val ownPath: String = "build-tests/test/src/kui/build/BuildWiringSuite.scala"

  /** The two file-reading call shapes this suite counts in its own source.
    *
    * Written WITH the open parenthesis so that a comment or a failure clue naming the method does not change
    * the count, and held here rather than inline so that the regular expression is one thing with one name.
    */
  val directReadShape: String = raw"Files\.readString\("
  val delegatedReadShape: String = raw"Source\.fromFile\("

  /** Every named step `.github/workflows/ci.yml` runs, in the order it declares them.
    *
    * `Install pnpm` appears three times, two jobs each install the workspace and the pinned browser, and the
    * two coverage artifacts (Scala, frontend) upload under one step name, so this is a list and is compared
    * as one.
    */
  val workflowSteps: List[String] = List(
    "Compile every module with -Werror",
    "Check formatting (scalafmt)",
    "Check lint rules (scalafix)",
    "Check the module layering rules (ADR-041)",
    "Check immutable action references",
    "Install pnpm",
    "Reject high or critical dependency vulnerabilities",
    "Check the committed OpenAPI documents",
    "Check the committed error-code table",
    "Check the committed browser constants",
    "Run every test suite",
    "Upload test reports",
    "Coverage report and ratchet",
    "Upload coverage report",
    "The interface image copies every workspace manifest",
    "Install pnpm",
    "Install",
    "Deployment proxy and local-only demo ports",
    "Typecheck",
    "Test",
    "Import boundaries",
    "Build",
    "The features are still loaded on demand",
    "Build Storybook",
    "Install the pinned browser",
    "Accessibility sweep over every story, in both themes",
    "The generated types are up to date",
    "Upload coverage report",
    "Install pnpm",
    "Install",
    "Install the pinned browser",
    "Start the product",
    "Drive it",
    "Upload the failure artifacts",
    "The container logs, when it was the stack rather than the screen",
    "Build the container images the distributed stack runs",
    "Run the Compose fault-isolation smoke test"
  )

  /** The `(package, destination directory)` pairs a Dockerfile's anchored manifest COPY lines make.
    *
    * Anchored on `package.json` at the source end for the reason ci.yml's own derivation is — a COPY of
    * anything else under a package directory is not that package's manifest — and on the line start, so a
    * COPY quoted inside a `RUN` or indented into a heredoc is not one of these lines.
    */
  def manifestPairs(text: String): List[(String, String)] =
    raw"(?m)^COPY frontend/packages/([^/]+)/package\.json[ \t]+\./packages/([^/]+)/[ \t]*$$".r
      .findAllMatchIn(text)
      .map(found => found.group(1) -> found.group(2))
      .toList
}
