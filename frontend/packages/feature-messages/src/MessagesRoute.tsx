/**
 * The message feature's route entry: the browser for one topic's records.
 *
 *   /clusters/:clusterId/topics/:topicName/messages
 *
 * ## What it joins up
 *
 * Every piece of this existed and nothing connected them. `MessagesTab` is the screen,
 * `createBrowseSession` is the state machine, `browse.ts` is the URL grammar, `transport.ts` is the
 * network — and the package exported no `default`, so the shell rendered the kernel's "this feature
 * arrived without a screen" panel for every `/messages` address. This is the wiring.
 *
 * ## The browse is the address bar
 *
 * A browse *is* a link: the seek position, the partitions, the filter and the live flag all live in
 * the query string, so an operator can send a colleague exactly what they are looking at. That is
 * why `MessagesTab` never writes the query itself — it asks, through `onQueryChange`, and this
 * component is the one thing that writes the address. Two writers to one URL is how a screen ends
 * up fighting the Back button.
 *
 * ## Stopping actually stops
 *
 * The session is disposed when the route unmounts, which closes the stream, which aborts the
 * request, which cancels the gateway's stream, which releases the Kafka consumer. Without that last
 * link every abandoned browse leaves a consumer assigned on the message service until its budget
 * expires — see `transport.ts` for why `openEventSource` could not do this.
 */
import { Show, createEffect, createMemo, createSignal, onCleanup } from "solid-js";
import type { JSX } from "@solidjs/web";
import { useLocation, useNavigate, useParams } from "@solidjs/router";
import { createMutation, notify, useQuery, useKui, type KafkaRecord } from "@kui/kernel";
import { Actions } from "@kui/api";
import { MessagesTab } from "./MessagesTab.jsx";
import { ProduceDrawer } from "./ProduceDrawer.jsx";
import { SmartFilterDialog } from "./SmartFilterDialog.jsx";
import { ResendDialog } from "./ResendDialog.jsx";
import { registerFilter, testFilter, type RegisteredFilter } from "./filters.js";
import { resend, type ResendDraft } from "./resend.js";
import { toDto } from "./wire.js";
import { TrackPage } from "./TrackPage.jsx";
import { emptyQuery, track, type TrackQuery } from "./track.js";
import { produce, type RecordDraft } from "./produce.js";
import { createBrowseSession } from "./session.js";
import { createBrowseTransport } from "./transport.js";
import { fromParams, queryString, type BrowseQuery } from "./browse.js";
import {
  celFor,
  isEmpty,
  predicateParams,
  predicatesFrom,
  type Predicates,
} from "./predicates.js";
import {
  readPresets,
  withPreset,
  withoutPreset,
  writePresets,
  type FilterPreset,
} from "./presets.js";
import { fetchTopicFacts, topicFactsKey } from "./topic.js";
import { TopicMessageContext } from "./TopicMessageContext.jsx";

export default function Messages(): JSX.Element {
  const params = useParams<{
    readonly clusterId?: string;
    readonly topicName?: string;
  }>();
  const location = useLocation();

  /*
   * `/messages/track` is the one address this feature serves that names no topic: a track reads
   * *across* topics, which is the whole point of it. It is matched on the path rather than by a
   * route parameter because it sits outside the `/topics/:topicName` subtree the browser lives in.
   */
  const tracking = () => location.pathname.replace(/\/+$/, "").endsWith("/messages/track");

  return (
    <Show when={params.clusterId} fallback={<NoSubject what="cluster" />} keyed>
      {(clusterId) => (
        <Show when={!tracking()} fallback={<TrackScreen clusterId={clusterId} />}>
          <Show when={params.topicName} fallback={<NoSubject what="topic" />} keyed>
            {(topicName) => <BrowserScreen clusterId={clusterId} topicName={topicName} />}
          </Show>
        </Show>
      )}
    </Show>
  );
}

/**
 * A message browser needs both a cluster and a topic, and the navigation cannot produce a link to
 * this route without them — the drawer has no topic to name, which is why the feature has no
 * navigation entry at all. So this is a hand-typed or stale address, and it says so.
 */
function NoSubject(props: { readonly what: string }): JSX.Element {
  const kui = useKui();
  return (
    <section aria-label="Messages">
      <p role="status">
        A message browser needs a {props.what} in its address, and this one has none.{" "}
        <a href={kui.paths.clusters()}>Start from the cluster list</a>.
      </p>
    </section>
  );
}

function BrowserScreen(props: {
  readonly clusterId: string;
  readonly topicName: string;
}): JSX.Element {
  const kui = useKui();
  const location = useLocation();
  const navigate = useNavigate();

  /**
   * The browse the address describes.
   *
   * Read from the URL rather than held in a signal, so that the Back button, a pasted link and a
   * reload all produce exactly the same browse. `fromParams` is total: a query string somebody has
   * edited by hand yields a valid browse rather than an exception.
   */
  const query = createMemo<BrowseQuery>(() => {
    const fromUrl = fromParams(new URLSearchParams(location.search));
    if (fromUrl.limit !== undefined) return fromUrl;
    const configured = kui.messageBrowser?.pageSize() ?? 100;
    const pageSize =
      Number.isSafeInteger(configured) && configured >= 1 && configured <= 500 ? configured : 100;
    return { ...fromUrl, limit: pageSize };
  });

  const streamUrl = () =>
    `/api/v1/clusters/${encodeURIComponent(props.clusterId)}` +
    `/topics/${encodeURIComponent(props.topicName)}/messages/stream`;

  // One session for the life of this screen. Changing the *query* restarts the stream inside it;
  // changing the topic unmounts the route, which disposes it.
  const session = createBrowseSession({
    streamUrl: kui.api.url(streamUrl()),
    transport: createBrowseTransport(),
  });

  let disposed = false;
  /* Invalidates an asynchronous filter registration when the controls change before it answers.
   * Without this, Read snapshots one expression for the POST and then starts a browse from the
   * newer URL, pairing an id minted for the old source with a different range or serde selection. */
  let startGeneration = 0;
  onCleanup(() => {
    // Closes the stream, which aborts the request, which releases the consumer. The single most
    // important line in this file.
    disposed = true;
    startGeneration += 1;
    session.stop();
  });

  /**
   * What the operator typed that the browse endpoint has no parameter for.
   *
   * Read from the address, exactly like {@link query}, and for the same reason: a browse is a link,
   * and a colleague opening one has to see the *predicates* in the controls rather than the
   * expression they compiled to. They are stripped before a request is made — see {@link start}.
   */
  const predicates = createMemo<Predicates>(() =>
    predicatesFrom(new URLSearchParams(location.search)),
  );

  /**
   * The topic's real partition count.
   *
   * This route held a signal hard-coded to zero and handed that zero to three children — `topic.ts`
   * spells out the shape it was. `ResendDialog` spends it in a sentence, so every copy dialog this
   * product has ever drawn said the source topic has 0 partitions — and disabled the control that
   * adds a range as a side effect, because a draft with one range already has "one per partition"
   * when there are none.
   *
   * `useQuery` rather than a hand-rolled fetch, and for the reasons the hook actually gives: one
   * request per key however many callers ask, the last good answer kept and marked stale rather
   * than blanked when a refresh fails, and a cache two browsers of one topic in two tabs share.
   *
   * It does **not** share an answer with the topic page. That was written here and it is not true:
   * this key is `messages:topic:<cluster>:<topic>` and the topic page's is its own, and the two ask
   * different endpoints — `topic.ts` says so twice in its own prose, which is where the claim
   * should have been checked. One field of one section is what this screen needs, and asking
   * `/overview` for it would make a partition menu depend on the schema registry's opinion.
   *
   * `undefined` while it is loading, refused or failed — never a number.
   */
  const topic = useQuery({
    key: () => topicFactsKey(props.clusterId, props.topicName),
    load: () => fetchTopicFacts(kui.api, props.clusterId, props.topicName),
  });
  const partitionCount = (): number | undefined => {
    const state = topic.state();
    return state.kind === "ready" || state.kind === "stale" ? state.value.partitionCount : undefined;
  };
  const topicHealth = () => {
    const state = topic.state();
    return state.kind === "ready" || state.kind === "stale" ? state.value.health : "unknown";
  };

  /**
   * The address this route last asked for, until the location agrees with it.
   *
   * `query()` and `predicates()` read the *location*, and the router updates that on its own
   * schedule. A control that changes both halves at once — the time-window chips set a start and
   * drop the window's end — would therefore compose its second write from the state before its
   * first, and land an address holding one half of the change.
   *
   * A plain variable rather than a signal, and read only by the two functions that compose an
   * address: nothing renders from it. The effect below drops it the moment the location catches up,
   * so a Back button, a pasted link and a reload all go through the location as they always did.
   */
  let written: { readonly query: BrowseQuery; readonly predicates: Predicates } | undefined;
  createEffect(
    () => location.search,
    () => {
      written = undefined;
    },
  );

  const currentQuery = (): BrowseQuery => written?.query ?? query();
  const currentPredicates = (): Predicates => written?.predicates ?? predicates();

  const [producing, setProducing] = createSignal(false);
  const [editingFilter, setEditingFilter] = createSignal(false);
  const [resending, setResending] = createSignal(false);

  /** The saved arrangements this browser holds for this cluster. */
  const [presets, setPresets] = createSignal<readonly FilterPreset[]>(
    readPresets(props.clusterId),
    { ownedWrite: true },
  );

  /** Why the last Read started no browse at all. Cleared by the next one. */
  const [refusal, setRefusal] = createSignal<string | undefined>(undefined, { ownedWrite: true });

  const write = createMutation((draft: RecordDraft) =>
    produce(kui.api, props.clusterId, props.topicName, draft),
  );

  /* Registering and previewing are two mutations rather than one, because their states are shown in
   * two different places in the dialog and a shared one would make a failed preview blank out the
   * apply button's error, or the other way round. */
  const compile = createMutation((source: string) =>
    registerFilter(kui.api, props.clusterId, source),
  );
  const preview = createMutation((source: string, sample: KafkaRecord) =>
    testFilter(kui.api, props.clusterId, source, toDto(sample)),
  );
  const copy = createMutation((draft: ResendDraft) =>
    resend(kui.api, props.clusterId, props.topicName, draft),
  );

  /* Registering what Read compiles, and deliberately *not* the same mutation as `compile`. That one
   * drives the editor's apply button; sharing it would put a Read's refusal inside a dialog nobody
   * has open, and would blank the editor's own error the next time somebody pressed Read. */
  const prepare = createMutation((source: string) =>
    registerFilter(kui.api, props.clusterId, source),
  );

  /**
   * The one writer of the address.
   *
   * `replace` rather than a push: adjusting a filter is refining one view, not visiting a new page,
   * and pushing every keystroke would make the Back button walk backwards through a sentence
   * somebody typed.
   *
   * ## Why this goes through the router and not through `window.history`
   *
   * It was `window.history.replaceState(null, "", url)`. The URL in the bar changed and **nothing
   * else did**: the router's history adapter learns about a navigation from `popstate`, which the
   * browser does not fire for a `replaceState` the page made itself, so `useLocation()` never saw
   * the write. `query()` is a memo over `location.search`, so it went on answering with the browse
   * the page was first opened with — every control on this bar wrote an address and then read the
   * old one back, and the next Read read the range from before the change.
   *
   * ## And why the argument is a bare query string
   *
   * `navigate` resolves a `to` that begins with `/` against the deployment's base, and
   * `location.pathname` **already carries** that base — so passing the pathname back produces
   * `/ui/ui/clusters/…`, and the route stops matching on the next reload. That is not a guess: it
   * is what a browser did, against the quickstart, with the first version of this function.
   *
   * A `to` starting with `?` takes the URL-relative path instead (`new URL(to, …current…)`), which
   * keeps the path exactly as it is and replaces only the query. That is precisely what this writer
   * means — the address of a browse is its query — so it is also the honest spelling.
   */
  function writeQuery(next: BrowseQuery, nextPredicates: Predicates = currentPredicates()): void {
    startGeneration += 1;
    written = { query: next, predicates: nextPredicates };
    const search = [
      queryString(next),
      ...predicateParams(nextPredicates).map(
        ([name, value]) => `${encodeURIComponent(name)}=${encodeURIComponent(value)}`,
      ),
    ]
      .filter((part) => part !== "")
      .join("&");
    navigate(`?${search}`, { replace: true });
  }

  /**
   * Start a browse for what the controls now say.
   *
   * The predicates are the reason this is not `session.start(query())`. They have no query parameter
   * — the browse endpoint takes a start position and one plain substring — so they are compiled to
   * one CEL expression and **registered** first, and the browse quotes the id the service minted.
   * The id and the source travel together, as they must: a replica that never saw this registration
   * compiles the source beside it rather than refusing the filter.
   *
   * A registration the cluster refuses stops the browse rather than starting one that silently
   * ignores every predicate on the bar. `filterSource` without a `filterId` is *dropped* by the
   * message service — deliberately, and documented there — so a browse started anyway would return
   * the unfiltered topic while the controls said otherwise, which is the most expensive way this
   * screen can be wrong.
   */
  function start(): void {
    setRefusal(undefined);
    const generation = ++startGeneration;
    const asked = currentQuery();
    const askedPredicates = currentPredicates();
    const source = celFor(askedPredicates, asked.filterSource);
    if (source === undefined) {
      session.start({ ...asked, filterId: undefined, filterSource: undefined });
      return;
    }
    void prepare.run(source).then((state) => {
      if (disposed || generation !== startGeneration) return;
      if (state.kind === "done") {
        session.start({
          ...asked,
          filterId: state.value.id,
          filterSource: state.value.source,
        });
        return;
      }
      /* `running` is the re-entry guard answering a second press while the first is still out, and
         `idle` is unreachable from `run`. Neither is a refusal, and drawing one for them would put
         a red sentence under a browse that is about to start. */
      if (state.kind === "running" || state.kind === "idle") return;
      setRefusal(
        "KUI did not start the browse: this cluster would not compile the filter these " +
          `controls describe. ${state.message}`,
      );
    });
  }

  /**
   * Put a compiled filter on the browse, or take it off.
   *
   * The id and the source move **together, always** — both set or both cleared. That pairing is the
   * whole reason the browse takes two parameters instead of one: a replica which has never seen this
   * id compiles the source beside it rather than refusing a filter that was registered a second ago
   * on a sibling. An address carrying only the id would work until it was opened on the wrong
   * replica, which is the worst possible time to find out.
   */
  function applyFilter(filter: RegisteredFilter | undefined): void {
    const next = currentQuery();
    writeQuery(
      filter === undefined
        ? { ...next, filterId: undefined, filterSource: undefined }
        : { ...next, filterId: filter.id, filterSource: filter.source },
    );
  }

  /*
   * Both questions name the topic this page is about.
   *
   * A grant carries a *pattern*, and `kernel/src/state/session.ts` is explicit that the subjectless
   * form asks the weaker question — *"the right answer for a list heading and the wrong one for a
   * row's delete button"*. This screen is one topic, so the weaker question is never the right one
   * here: until wave 8 an account granted `TOPIC:MESSAGES_PRODUCE` on `analytics\..*` was handed a
   * live `Produce message` over `orders.payments`, and the refusal arrived from the gateway after
   * the record had been typed.
   */
  const produceBlocked = (topic: string = props.topicName): string | undefined =>
    kui.writeBlocked !== undefined
      ? kui.writeBlocked(props.clusterId, Actions.TopicMessagesProduce, topic)
      : kui.permits(Actions.TopicMessagesProduce, topic) ? undefined
        : "You do not have permission to publish into this topic.";
  const mayProduce = () => produceBlocked() === undefined;
  /* A resend reads this topic and writes another. The gateway checks both, and the second is a
   * permission on a topic that has not been named yet — so this only gates on the half that can be
   * checked here, and the server refuses the other half with the destination in the message. */
  const mayResend = () =>
    kui.permits(Actions.TopicMessagesRead, props.topicName);

  /**
   * Name a preset for what is on the bar right now.
   *
   * `prompt` and not a dialog, and the reason is worth stating rather than defending later: a preset
   * is a private label this browser keeps, it takes one word, and a modal for it would be the fourth
   * overlay on a screen that already has three. Cancelling saves nothing, which is what an empty
   * answer means.
   */
  function savePreset(): void {
    const name = window.prompt("Name this filter", "")?.trim() ?? "";
    if (name === "") return;
    const expression = currentQuery().filterSource;
    const next = withPreset(presets(), {
      name,
      predicates: currentPredicates(),
      ...(expression === undefined || expression === "" ? {} : { expression }),
    });
    setPresets(next);
    writePresets(props.clusterId, next);
    notify("Filter saved", { message: `${name} is on this browser only.`, tone: "info" });
  }

  function applyPreset(preset: FilterPreset): void {
    session.stop();
    /* Both halves at once, in one address write. Applying the predicates and then the expression
       would be two navigations, and the second would be computed from a location the first had not
       finished writing — which lands as a browse holding one half of the preset. */
    writeQuery(
      preset.expression === undefined
        ? { ...currentQuery(), filterId: undefined, filterSource: undefined }
        : { ...currentQuery(), filterId: undefined, filterSource: preset.expression },
      preset.predicates,
    );
  }

  return (
    <>
      <TopicMessageContext
        clusterId={props.clusterId}
        topicName={props.topicName}
        partitionCount={partitionCount()}
        health={topicHealth()}
      >
        <MessagesTab
          topic={props.topicName}
          /* The topic's own figure, fetched beside the stream. `undefined` while it is loading or if
           the answer refused: the children below each draw a sentence for that, and none of them
           draws a zero — a topic cannot have no partitions, so a zero here would be a claim that
           is both impossible and reassuring. */
          partitionCount={partitionCount()}
          query={query()}
          defaultView={kui.messageBrowser?.mode() ?? "pages"}
          onQueryChange={writeQuery}
          predicates={predicates()}
          onPredicatesChange={(next) => writeQuery(currentQuery(), next)}
          onRead={start}
          readBusy={prepare.busy()}
          {...(refusal() === undefined ? {} : { refusal: refusal() })}
          presets={presets()}
          onApplyPreset={applyPreset}
          onRemovePreset={(preset) => {
            const next = withoutPreset(presets(), preset.name);
            setPresets(next);
            writePresets(props.clusterId, next);
          }}
          /* Offered only when there is something to save. A "save as preset" that saves the empty
             arrangement is a chip that does nothing, named after nothing. */
          {...(isEmpty(predicates()) && (query().filterSource ?? "") === ""
            ? {}
            : { onSavePreset: savePreset })}
          session={session}
          mayProduce={mayProduce()}
          produceDisabledReason={produceBlocked()}
          onProduce={() => {
            // The last attempt's receipt or error belongs to the drawer that showed it. Reopening to
            // find "written to partition 3" from ten minutes ago reads as this record having been sent.
            write.reset();
            setProducing(true);
          }}
          mayResend={mayResend()}
          resendDisabledReason={
            mayResend()
              ? undefined
              : "You do not have permission to read this topic and publish into another one."
          }
          onResend={() => {
            // Same rule as produce: a tally from a previous copy reappearing over a fresh form would
            // read as this copy's receipt, and the figures are the whole content of that panel.
            copy.reset();
            setResending(true);
          }}
          smartFilter={{
            ...(query().filterSource === undefined ? {} : { source: query().filterSource }),
            onOpen: () => {
              compile.reset();
              preview.reset();
              setEditingFilter(true);
            },
            ...(query().filterId === undefined
              ? {}
              : {
                  onClear: () => {
                    session.stop();
                    applyFilter(undefined);
                  },
                })
          }}
        />
      </TopicMessageContext>

      <SmartFilterDialog
        open={editingFilter()}
        onClose={() => setEditingFilter(false)}
        topic={props.topicName}
        {...(query().filterSource === undefined ? {} : { source: query().filterSource })}
        /* The records on screen are what a preview may be tried against. A filter is written about
           a shape of document, and a synthetic record would answer a question about a document this
           topic does not contain. */
        samples={session.rows()}
        testState={preview.state()}
        onTest={(source, sample) => void preview.run(source, sample)}
        applyState={compile.state()}
        onApply={(source) => {
          void compile.run(source).then((state) => {
            if (disposed) return;
            /* Only a filter the server compiled reaches the browse — and the id travels with the
               source it was minted from, because a replica that has never seen this id compiles the
               source rather than refusing a filter registered a second ago on its neighbour. */
            if (state.kind !== "done") return;
            session.stop();
            applyFilter(state.value);
            setEditingFilter(false);
          });
        }}
        {...(query().filterId === undefined
          ? {}
          : {
              onClear: () => {
                session.stop();
                applyFilter(undefined);
                setEditingFilter(false);
              },
            })}
      />

      <ResendDialog
        open={resending()}
        onClose={() => setResending(false)}
        {...(partitionCount() === undefined ? {} : { partitionCount: partitionCount() })}
        topic={props.topicName}
        state={copy.state()}
        destinationBlocked={produceBlocked}
        /* Stays open on success, like the produce drawer and for a stronger reason: the answer is
           two figures, and a copy that read and wrote nothing is a 200 whose whole meaning is in
           them. Closing on success would show the operator nothing at all. */
        onSend={(draft) => {
          if (!mayResend() || produceBlocked(draft.toTopic) !== undefined) return;
          void copy.run(draft).then((state) => {
            if (state.kind !== "done") return;
            /* The dialog already shows the figures; the toast is what survives it being closed.
               A copy is not reversible and the operator has to be able to say afterwards that it
               happened, which a panel they dismissed cannot do.

               `written`, not `requested`: the request said how many records to try for and the
               answer says how many arrived, and on a range that retention has eaten those are
               different numbers. Reporting the first would be reporting the intention. */
            notify(state.value.written === 0 ? "Nothing was copied" : "Records copied", {
              tone: state.value.written === 0 ? "warning" : "success",
              message:
                `${state.value.written.toLocaleString()} of ${state.value.read.toLocaleString()} ` +
                `records read from ${props.topicName} reached ${state.value.toTopic}.`,
            });
          });
        }}
      />

      <ProduceDrawer
        open={producing()}
        onClose={() => setProducing(false)}
        topic={props.topicName}
        {...(partitionCount() === undefined ? {} : { partitionCount: partitionCount() })}
        state={write.state()}
        disabledReason={produceBlocked()}
        onSend={(draft) => {
          if (produceBlocked() !== undefined) return;
          /* The drawer deliberately stays open on success: it shows the partition and offset the
           broker assigned. "Sent" is not something an operator can go and check; a position is. */
          void write.run(draft).then((state) => {
            if (state.kind !== "done" || state.value.length === 0) return;
            /* A produce cannot be undone either, and the drawer is dismissible. The toast quotes a
               position rather than saying "sent", for the reason the drawer does: a position is
               something the operator can go and look at. */
            const first = state.value[0];
            notify(
              state.value.length === 1
                ? "Record published"
                : `${String(state.value.length)} records published`,
              {
                message:
                  state.value.length === 1 && first !== undefined
                    ? `${props.topicName} partition ${String(first.partition)}, ` +
                      `offset ${String(first.offset)}.`
                    : `Written to ${props.topicName}.`,
              },
            );
          });
        }}
      />
    </>
  );
}

/**
 * Why Search is closed, in the words that tell the reader what to ask for.
 *
 * Two sentences rather than one, because they send the reader to different places. "on this
 * cluster" is a role nobody granted; a named topic is a grant that exists and does not stretch
 * this far, and an operator who is told only the first will ask an administrator for a permission
 * they already hold. The topic is named because a track form carries several, and the reader
 * cannot otherwise tell which of them closed the button.
 */
function disabledReasonFor(anywhere: boolean, refused: string | undefined): string | undefined {
  if (!anywhere) return "You do not have permission to read messages on this cluster.";
  if (refused === undefined) return undefined;
  return `You do not have permission to read messages on ${refused}.`;
}

/** Tracking one value across several topics. */
function TrackScreen(props: { readonly clusterId: string }): JSX.Element {
  const kui = useKui();
  const [query, setQuery] = createSignal<TrackQuery>(emptyQuery());
  const run = createMutation((q: TrackQuery) => track(kui.api, props.clusterId, q));

  /**
   * The first topic on the form this principal may not read, or `undefined`.
   *
   * A track is a full read of **every** topic it names, so one refused topic refuses the whole
   * search — and it is refused before the request rather than after a multi-topic scan has already
   * started. The subjectless question cannot see that: a grant on `analytics\..*` answers yes to it
   * while answering no to `orders.payments`, which is the arrangement this page is most likely to
   * meet, because the form is where somebody types a topic they do not normally read.
   */
  const refusedTopic = (): string | undefined =>
    query().topics.find((topic) => !kui.permits(Actions.TopicMessagesRead, topic));

  /* The weaker question, asked for its own sake: it separates "this account may not read anything
     here" from "this grant does not stretch to that topic", and those send the reader to different
     places. With no topic named it is also the only question there is — `TrackPage` refuses an
     empty form on its own, with "at least one topic". */
  const holdsRead = () => kui.permits(Actions.TopicMessagesRead);

  return (
    <TrackPage
      query={query()}
      onQueryChange={(next) => {
        setQuery(next);
        /* The last answer described the last query. Leaving it on screen under a changed form is
           how somebody concludes a value is absent from a window they never searched. */
        run.reset();
      }}
      onSearch={() => void run.run(query())}
      state={run.state()}
      disabledReason={disabledReasonFor(holdsRead(), refusedTopic())}
    />
  );
}
