/**
 * The schema feature's route entry.
 *
 *   /clusters/:clusterId/schemas                          the subjects, no pane selected
 *   /clusters/:clusterId/schemas/:subject                 the same list, with that subject's pane
 *   /clusters/:clusterId/schemas/:subject?version=3       and at that version
 *
 * ## One screen, two panes, and the address is the selection
 *
 * Both routes render the same {@link SchemaWorkspace}; the second one also has a right-hand pane.
 * That is what keeps the list on screen while a subject is read, and it is why a link somebody
 * pastes opens the pane it names rather than the list with instructions attached.
 *
 * ## Why `useQuery` and not another `useFetch`
 *
 * This file used to carry the fifth hand-rolled copy of fetch-hold-refetch. Every copy starts each
 * attempt at `loading`, so a refetch that fails blanks a pane that was showing real figures a moment
 * ago — and every copy asks for the global compatibility level again, per component, because there
 * is nothing shared to ask through. `useQuery` keeps the last good value and marks it stale with the
 * reason, and two readers of one key make one request. Both panes on this screen read the same
 * cluster's registry, so that is not a hypothetical saving.
 *
 * A key is a promise: everything that changes the request is in the string. The loaders below read
 * the route's own signals rather than parsing the key back apart, which is sound precisely because
 * of that promise — a loader is bound at the moment its key is computed, so the signals it reads are
 * the ones the key was built from.
 */
import { Show, createEffect, createMemo, createSignal, onCleanup } from "solid-js";
import type { JSX } from "@solidjs/web";
import { useLocation, useParams } from "@solidjs/router";
import { Actions } from "@kui/api";
import {
  createMutation,
  notify,
  useKui,
  useQuery,
  valueOf,
  writeBlockedReason,
  type Fetched,
} from "@kui/kernel";
import { RegisterSchemaDialog } from "./RegisterSchemaDialog.jsx";
import { SchemaWorkspace } from "./SchemaWorkspace.jsx";
import { SubjectList } from "./SubjectList.jsx";
import { SubjectPage } from "./SubjectPage.jsx";
import {
  checkCompatibility,
  fetchGlobalCompatibility,
  fetchSchema,
  fetchSubjectCompatibility,
  fetchSubjects,
  fetchVersions,
  registerBlockedReason,
  registerSchema,
  setCompatibility,
  schemaKeys,
  type Compatibility,
  type CompatibilityLevel,
  type ProposedSchema,
  type RegisteredSchema,
  type SchemaVersion,
  type SubjectListResult,
} from "./data.js";

/** How many subjects a page holds. The registry pages and searches; the browser decides when. */
const PageSize = 50;

export default function Schemas(): JSX.Element {
  const params = useParams<{ readonly clusterId?: string; readonly subject?: string }>();
  return (
    <Show keyed when={params.clusterId} fallback={<NoCluster />}>
      {(clusterId) => <Registry clusterId={clusterId} subject={params.subject} />}
    </Show>
  );
}

function NoCluster(): JSX.Element {
  const kui = useKui();
  return (
    <section aria-label="Schema registry">
      <p role="status">
        No cluster is selected, so there is no registry to read.{" "}
        <a href={kui.paths.clusters()}>Choose a cluster</a> and try again.
      </p>
    </section>
  );
}

/**
 * A failure in the screen's own words, and how loudly to say it.
 *
 * The registry is the one upstream in this product that is routinely *absent* rather than broken —
 * plenty of clusters have none — so "not configured" is a different sentence from "not answering",
 * and neither is an empty subject list.
 *
 * ## `stale` is not a failure, and it never had a code
 *
 * A stale query is a real answer with a badge on it: the figures below the banner are the ones the
 * registry gave, they are simply older than the last attempt. This drew them under `tone="danger"`
 * — the tone this product reserves for something that is wrong — beside an invented error code,
 * `KUI-STALE`, which exists in no generated constant, in no `docs/api/error-codes.md` and in no
 * service. `Banner`'s `code` prop is documented as *"the stable code, for whoever the operator
 * escalates to"*, and escalating a code nobody has ever heard of wastes the afternoon of whoever
 * receives it.
 *
 * So stale is a warning with no code. That is this repository's written rule — *data that is real
 * and out of date is still the best answer anybody has* — and it is the same rule `optional()`
 * below exists to keep.
 */
function failureOf(
  state: Fetched<unknown>,
): { message: string; code?: string; tone?: "danger" | "warning" } | undefined {
  switch (state.kind) {
    case "failed":
      return { message: state.message, code: state.code, tone: "danger" };
    case "stale":
      return { message: state.reason, tone: "warning" };
    case "forbidden":
      return {
        message: "You do not have permission to read this cluster's schema registry.",
        tone: "danger",
      };
    case "not-configured":
      return {
        message: "This cluster has no schema registry configured, so there are no subjects to list.",
        tone: "danger",
      };
    default:
      return undefined;
  }
}

/**
 * The value a query holds, or `undefined`.
 *
 * `valueOf` needs a fallback of the same type and these three call sites want "nothing yet", which
 * is a different type. Spelled once here rather than as a conditional at each site, because the
 * conditional is where "ready" gets written and "stale" gets forgotten — and a stale value is a real
 * value with a badge on it, not an absence.
 */
function optional<T>(state: Fetched<T>): T | undefined {
  return state.kind === "ready" || state.kind === "stale" ? state.value : undefined;
}

/** A failed write, in the shape the screens' banner takes. */
function mutationFailure(
  state: ReturnType<ReturnType<typeof createMutation<[CompatibilityLevel], unknown>>["state"]>,
): { message: string; code?: string; tone?: "danger" | "warning" } | undefined {
  if (state.kind === "forbidden") {
    return { message: state.message, code: "KUI-FORBIDDEN", tone: "danger" };
  }
  if (state.kind === "failed") return { message: state.message, code: state.code, tone: "danger" };
  return undefined;
}

/**
 * The confirmation for a level that has just changed.
 *
 * `NONE` gets a different toast because it is a different event: every other level narrows what the
 * registry will accept, and `NONE` turns the checking off. A green "done" for that is the product
 * agreeing with a decision it should be reporting.
 */
function notifyLevelSet(level: CompatibilityLevel, scope: string): void {
  if (level === "NONE") {
    notify(`Compatibility for ${scope} set to NONE`, {
      tone: "warning",
      message:
        "Nothing is checked from now on: the registry will accept a schema that breaks every " +
        "existing reader.",
    });
    return;
  }
  notify(`Compatibility for ${scope} set to ${level}`, { tone: "success" });
}

/**
 * The confirmation for a schema the registry accepted.
 *
 * The version and the id are stated separately and labelled, because they are different numbers and
 * only one of them is written into a record's header — the same distinction `SubjectPage` draws in
 * its fact list. Either can be absent from the answer, and an absence is said rather than filled
 * with the other one: "registered as 4" over a registry that reported an id and no version would
 * send somebody looking for version 4 of a subject that has two.
 */
function notifyRegistered(registered: RegisteredSchema): void {
  const figures: string[] = [];
  if (registered.version !== undefined) figures.push(`version ${registered.version}`);
  if (registered.id !== undefined) figures.push(`schema id ${registered.id}`);
  notify(`Registered a schema under ${registered.subject}`, {
    tone: "success",
    message:
      figures.length === 0
        ? "The registry accepted it and did not say which version or id it became."
        : `The registry accepted it as ${figures.join(", ")}.`,
  });
}

function Registry(props: {
  readonly clusterId: string;
  readonly subject: string | undefined;
}): JSX.Element {
  const kui = useKui();
  const [search, setSearch] = createSignal("");
  const [typed, setTyped] = createSignal("");
  const [page, setPage] = createSignal(1);
  const [direction, setDirection] = createSignal<"asc" | "desc">("asc");

  // The registry searches and pages; the browser only decides when to ask. Same rule as the topic
  // list, and for the same reason — a registry with four thousand subjects is not unusual.
  let timer: ReturnType<typeof setTimeout> | undefined;
  onCleanup(() => clearTimeout(timer));

  const subjects = useQuery<SubjectListResult>({
    key: () =>
      schemaKeys.subjects(props.clusterId, { q: search(), direction: direction(), page: page(), pageSize: PageSize }),
    load: () =>
      fetchSubjects(kui.api, props.clusterId, {
        q: search(),
        direction: direction(),
        page: page(),
        pageSize: PageSize,
      }),
  });

  const globalLevel = useQuery<Compatibility>({
    key: () => schemaKeys.global(props.clusterId),
    load: () => fetchGlobalCompatibility(kui.api, props.clusterId),
  });

  const setGlobal = createMutation((level: CompatibilityLevel) =>
    setCompatibility(kui.api, props.clusterId, level),
  );

  const [registering, setRegistering] = createSignal(false);
  const register = createMutation((subject: string, proposed: ProposedSchema) =>
    registerSchema(kui.api, props.clusterId, subject, proposed),
  );

  const registerRefusal = (subject?: string): string | undefined => kui.writeBlocked !== undefined
    ? kui.writeBlocked(props.clusterId, Actions.SchemaCreate, subject)
    : registerBlockedReason(kui.permits(Actions.SchemaCreate, subject));
  const mayRegister = (): boolean => registerRefusal() === undefined;

  createEffect(
    () => subjects.state(),
    (current) => {
      if (current.kind !== "loading") kui.report("feature", current.kind === "failed");
    },
  );

  const result = (): SubjectListResult =>
    valueOf(subjects.state(), {
      subjects: [],
      page: { page: 1, pageSize: PageSize, totalItems: undefined },
    });

  const global = (): Compatibility | undefined => optional(globalLevel.state());

  const globalRefusal = (): string | undefined => kui.writeBlocked !== undefined
    ? kui.writeBlocked(props.clusterId, Actions.SchemaModifyGlobalCompatibility)
    : writeBlockedReason({ permitted: kui.permits(Actions.SchemaModifyGlobalCompatibility), readOnly: false,
        action: "change the registry's compatibility level" });
  const mayEditGlobal = (): boolean => globalRefusal() === undefined;

  const listHref = (): string =>
    `${kui.paths.clusters()}/${encodeURIComponent(props.clusterId)}/schemas`;
  const hrefFor = (subject: string): string => `${listHref()}/${encodeURIComponent(subject)}`;

  const list = (
    <SubjectList
      subjects={result().subjects}
      loading={subjects.state().kind === "loading"}
      global={global()}
      selected={props.subject}
      search={typed()}
      onSearch={(text) => {
        setTyped(text);
        clearTimeout(timer);
        timer = setTimeout(() => {
          setSearch(text.trim());
          setPage(1);
        }, 300);
      }}
      direction={direction()}
      onDirection={(next) => {
        setDirection(next);
        // Back to page one. Page 3 of an ascending list is a different set of subjects from page 3
        // of a descending one, and keeping the number would silently change what is on screen.
        setPage(1);
      }}
      page={result().page.page}
      pageSize={result().page.pageSize}
      totalItems={result().page.totalItems}
      onPage={setPage}
      hrefFor={hrefFor}
      onSetGlobal={
        mayEditGlobal()
          ? (level) => {
              if (!mayEditGlobal()) return;
              void setGlobal.run(level).then((outcome) => {
                if (outcome.kind !== "done") return;
                notifyLevelSet(level, "every inheriting subject");
              });
            }
          : undefined
      }
      setGlobalDisabledReason={globalRefusal()}
      state={setGlobal.state()}
      failure={failureOf(subjects.state()) ?? mutationFailure(setGlobal.state())}
    />
  );

  return (
    <>
      <SchemaWorkspace
        list={list}
        /*
         * The registry's total, and never `result().subjects.length`.
         *
         * The page holds fifty rows out of however many the registry has, so the row count is the
         * page's size dressed up as an inventory — right only on a registry with one page, which is
         * every registry anybody develops against. It is character for character the defect the
         * consumer groups screen was rewritten to remove.
         */
        subjectCount={result().page.totalItems}
        globalLevel={global()?.level}
        loading={subjects.state().kind === "loading"}
        onRegister={mayRegister() ? () => setRegistering(true) : undefined}
        registerDisabledReason={registerRefusal()}
        detail={
          props.subject === undefined ? undefined : <Show keyed when={props.subject}>
            {(subject) => (
              <SubjectPane
                clusterId={props.clusterId}
                subject={subject}
                listHref={listHref()}
              />
            )}
          </Show>
        }
      />

      {/* Mounted only while it is open, so that each opening is a fresh form.

          `Dialog` already unmounts its own *surface* when closed, but the fields' signals live in
          `RegisterSchemaDialog` itself and would outlive it — which means the dialog reopens
          holding the schema that was just registered, one press away from registering it twice.
          A `Show` here is the same rule applied one level up, and it is why the component needs no
          reset logic of its own. */}
      <Show when={registering()}>
        <RegisterSchemaDialog
          open
          onClose={() => {
            setRegistering(false);
            // The mutation outlives the dialog, so its state is cleared here rather than there:
            // without this the *next* opening would draw the previous attempt's refusal above a
            // box nobody has typed in yet.
            register.reset();
          }}
          state={register.state()}
          knownSubjects={result().subjects.map((row) => row.subject)}
          writeBlocked={registerRefusal}
          onRegister={(subject, proposed) => {
            if (registerRefusal(subject) !== undefined) return;
            void register.run(subject, proposed).then((outcome) => {
              if (outcome.kind !== "done") return;
              setRegistering(false);
              register.reset();
              // The data layer invalidates every affected query before resolving. Keep the
              // current search/page and announce success even if the new subject is on another page.
              notifyRegistered(outcome.value);
            });
          }}
        />
      </Show>
    </>
  );
}

function SubjectPane(props: {
  readonly clusterId: string;
  readonly subject: string;
  readonly listHref: string;
}): JSX.Element {
  const kui = useKui();
  const location = useLocation();

  const versions = useQuery<readonly number[]>({
    key: () => schemaKeys.versions(props.clusterId, props.subject),
    load: () => fetchVersions(kui.api, props.clusterId, props.subject),
  });

  /**
   * Which version is on screen: the one in the address, or the newest.
   *
   * `latest` rather than a number when the address names none, because the registry understands the
   * word and it stays correct while somebody is looking at the page and a new version is registered.
   */
  const version = createMemo(() => new URLSearchParams(location.search).get("version") ?? "latest");

  const schema = useQuery<SchemaVersion>({
    key: () => schemaKeys.schema(props.clusterId, props.subject, version()),
    load: () => fetchSchema(kui.api, props.clusterId, props.subject, version()),
  });

  const compatibility = useQuery<Compatibility>({
    key: () => schemaKeys.compatibility(props.clusterId, props.subject),
    load: () => fetchSubjectCompatibility(kui.api, props.clusterId, props.subject),
  });

  const setLevel = createMutation((level: CompatibilityLevel) =>
    setCompatibility(kui.api, props.clusterId, level, props.subject),
  );

  /**
   * The compatibility check.
   *
   * A `createMutation` although it changes nothing: what it needs is the running / done / failed /
   * forbidden state machine and the guard that stops a double press sending two requests. The
   * endpoint carries no mutation marker on the server, is not gated behind an edit permission, and is
   * answered on a read-only cluster like any other read — so unlike `setLevel` above it is offered to
   * every principal who can see this page.
   */
  const check = createMutation((proposed: ProposedSchema) =>
    checkCompatibility(kui.api, props.clusterId, props.subject, proposed),
  );

  createEffect(
    () => schema.state(),
    (current) => {
      if (current.kind !== "loading") kui.report("feature", current.kind === "failed");
    },
  );

  const editRefusal = (): string | undefined => kui.writeBlocked !== undefined
    ? kui.writeBlocked(props.clusterId, Actions.SchemaEdit, props.subject)
    : writeBlockedReason({ permitted: kui.permits(Actions.SchemaEdit, props.subject), readOnly: false,
        action: "change this subject's compatibility level" });
  const mayEdit = (): boolean => editRefusal() === undefined;

  return (
    <SubjectPage
      subject={props.subject}
      versions={valueOf(versions.state(), [])}
      current={optional(schema.state())}
      compatibility={optional(compatibility.state())}
      loading={schema.state().kind === "loading"}
      listHref={props.listHref}
      hrefForVersion={(one) =>
        `${kui.paths.clusters()}/${encodeURIComponent(props.clusterId)}/schemas/${encodeURIComponent(props.subject)}?version=${one}`
      }
      onSetCompatibility={
        mayEdit()
          ? (level) => {
              if (!mayEdit()) return;
              void setLevel.run(level).then((outcome) => {
                if (outcome.kind !== "done") return;
                notifyLevelSet(level, props.subject);
              });
            }
          : undefined
      }
      setCompatibilityDisabledReason={editRefusal()}
      state={setLevel.state()}
      failure={failureOf(schema.state()) ?? mutationFailure(setLevel.state())}
      onCheckCompatibility={(proposed) => void check.run(proposed)}
      checkState={check.state()}
    />
  );
}
