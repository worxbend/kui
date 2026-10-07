/**
 * Writing a smart filter, and trying it before a browse is started with it.
 *
 * ## The two buttons are not the same button
 *
 * **Try it** runs the expression against one record that is already on screen. **Use this filter**
 * compiles it and hands it to the browse. They are separate because they answer different questions
 * and because the second one is not undoable in the useful sense: it stops whatever browse is
 * running and starts a new one over the topic.
 *
 * Compiling — which is what "use" does first — only tells you the expression is *legal*. The
 * expression `record.offset` compiles perfectly and returns an id; it is not a predicate, and the
 * only place that is ever said out loud is a preview, which answers "the filter returned Long rather
 * than true or false". Without the preview an operator learns this by watching a browse over a
 * production topic return nothing and having no way to tell whether that is the filter or the data.
 * That is the experience this dialog exists to prevent, and it is why the preview is offered beside
 * the editor rather than hidden behind a disclosure.
 *
 * ## Which record it tries against, and why the operator picks
 *
 * The records on screen, by offset. Not a synthetic one: a filter is written about a shape of
 * document, and a made-up record would answer questions about a document that is not in the topic.
 * The picker matters because the interesting record is usually a specific one — the operator has
 * seen the row they want to match and wants to know whether their expression catches *it*.
 *
 * When there are no records on screen there is nothing honest to test against, so the button is
 * **disabled with the reason in it** rather than removed. A control that vanishes teaches an
 * operator the product cannot do the thing; a disabled one with a sentence teaches them what to do
 * first, which here is "read some records".
 *
 * ## Three verdicts get three different panels
 *
 * Matched, did not match, and threw. The third wears the failure tone and the second does not,
 * because "your filter is wrong" and "this record is not one of the ones you want" are opposite
 * pieces of news and a shared neutral panel would make them look alike. A non-match is a *working*
 * filter reporting a fact.
 *
 * ## The editor keeps what was typed on every failure
 *
 * A compile error, a failed preview and a rejected registration all leave the expression exactly
 * where it is, with the server's own message beside it — including the line and column, which is
 * the part an operator can act on. Clearing the box on failure would throw away the work at the one
 * moment it is needed.
 */
import { For, Show, createEffect, createMemo, createSignal, untrack } from "solid-js";
import type { JSX } from "@solidjs/web";
import { Banner, Button, Dialog, Icon, Select, TextField, type Mutation } from "@kui/kernel";
import type { IconName, KafkaRecord } from "@kui/kernel";

import {
  FILTER_EXAMPLES,
  FILTER_VARIABLES,
  filterProblem,
  type FilterVerdict,
  type RegisteredFilter,
} from "./filters.js";
import {
  compileJsonPathFilter,
  type JsonPathComparisonOperator,
  type JsonPathFilterTarget,
} from "./jsonPathFilter.js";

type EditorMode = "field" | "advanced";
type FieldValueType = "string" | "number" | "boolean" | "null";

export interface SmartFilterDialogProps {
  readonly open: boolean;
  readonly onClose: () => void;
  readonly topic: string;

  /** The expression the dialog opens with — the filter already running, when there is one. */
  readonly source?: string | undefined;

  /**
   * The records the preview may be tried against, newest first. Usually the ones on screen.
   *
   * Empty is a legitimate state and not an error: it is what a browse that has not been read yet
   * looks like. The preview control disables itself and says so.
   */
  readonly samples: readonly KafkaRecord[];

  /** Runs the preview against the chosen record. The screen owns the request. */
  readonly onTest: (source: string, sample: KafkaRecord) => void;
  readonly testState: Mutation<FilterVerdict>;

  /** Compiles it and applies it to the browse. */
  readonly onApply: (source: string) => void;
  readonly applyState: Mutation<RegisteredFilter>;

  /** Takes the filter off the browse. Absent when there is no filter running. */
  readonly onClear?: (() => void) | undefined;
}

export function SmartFilterDialog(props: SmartFilterDialogProps): JSX.Element {
  /* The expression is the dialog's own state, and it is seeded from the prop on *open* rather than
   * on mount.
   *
   * Both halves matter. Reading `props.source` on every render would rewrite the box under the
   * operator's cursor the instant the browse changed — the defect the seek inputs in
   * `MessageFilterBar` avoid, for the same reason. But seeding it only at mount is worse in the
   * other direction: this dialog is mounted with the screen, long before it is opened, so the box
   * would be seeded with the filter that was running when the *page* loaded and would never catch
   * up. An operator who applied a filter and reopened the editor would find it empty.
   *
   * `untrack` around the read for the same reason `MessageFilterBar` uses it: this is a deliberate
   * one-off read of a reactive value, which Solid 2's strict mode otherwise reports. */
  const [source, setSource] = createSignal(untrack(() => props.source ?? ""));
  const [sampleAt, setSampleAt] = createSignal(0);
  const [mode, setMode] = createSignal<EditorMode>(
    untrack(() => ((props.source ?? "") === "" ? "field" : "advanced")),
  );
  const [fieldTarget, setFieldTarget] = createSignal<JsonPathFilterTarget>("value");
  const [fieldPath, setFieldPath] = createSignal("$.status");
  const [fieldOperator, setFieldOperator] = createSignal<JsonPathComparisonOperator>("==");
  const [fieldValueType, setFieldValueType] = createSignal<FieldValueType>("string");
  const [fieldValue, setFieldValue] = createSignal("");

  createEffect(
    () => props.open,
    (open) => {
      if (!open) return;
      /* Reopening starts from what is actually running, and from the first sample again — the row
       * that was chosen last time may not be on screen any more, and an index into a list that has
       * moved on points at a different record. */
      setSource(untrack(() => props.source ?? ""));
      setMode(untrack(() => (props.source ?? "") === "" ? "field" : "advanced"));
      setSampleAt(0);
    },
  );

  const compiledField = createMemo(() =>
    compileJsonPathFilter({
      target: fieldTarget(),
      path: fieldPath(),
      operator: fieldOperator(),
      value: fieldFilterValue(fieldValueType(), fieldValue()),
    }),
  );
  const effectiveSource = (): string => {
    if (mode() === "advanced") return source();
    const compiled = compiledField();
    return compiled.ok ? compiled.source : "";
  };
  const problem = createMemo(() => {
    if (mode() === "advanced") return filterProblem(source());
    const compiled = compiledField();
    return compiled.ok ? undefined : compiled.error;
  });
  const busy = () => props.testState.kind === "running" || props.applyState.kind === "running";
  const suggestions = createMemo(() => jsonPathSuggestions(props.samples, fieldTarget()));

  const sample = (): KafkaRecord | undefined => props.samples[sampleAt()];

  const [tested, setTested] = createSignal<{
    source: string; sample: KafkaRecord | undefined; topic: string;
  } | undefined>(untrack(() => props.testState.kind === "done"
    ? { source: effectiveSource(), sample: sample(), topic: props.topic } : undefined));
  createEffect(
    () => [effectiveSource(), sample(), props.topic, props.open] as const,
    ([source, chosen, topic, open]) => {
      const previous = untrack(tested);
      if (!open || previous?.source !== source || previous.sample !== chosen || previous.topic !== topic) {
        setTested(undefined);
      }
    },
  );
  const currentTest = (): boolean => {
    const submitted = tested();
    return props.open && submitted !== undefined && submitted.source === effectiveSource() &&
      submitted.sample === sample() && submitted.topic === props.topic;
  };

  /**
   * Why the preview cannot run, in the words the operator needs.
   *
   * `Button`'s type requires this whenever `disabled` is set, which is the rule that stops a control
   * being greyed out with no explanation anywhere on the screen.
   */
  const testDisabledReason = (): string | undefined => {
    if (props.samples.length === 0) {
      return "There are no records on screen to try this against. Read some records first, then come back.";
    }
    const stated = problem();
    if (stated !== undefined) return stated;
    if (busy()) return "Waiting for the last request to finish.";
    return undefined;
  };

  const applyDisabledReason = (): string | undefined => {
    const stated = problem();
    if (stated !== undefined) return stated;
    if (busy()) return "Waiting for the last request to finish.";
    return undefined;
  };

  const verdict = (): FilterVerdict | undefined =>
    currentTest() && props.testState.kind === "done" ? props.testState.value : undefined;

  const failure = (): { readonly message: string; readonly code?: string } | undefined => {
    for (const state of [props.applyState, ...(currentTest() ? [props.testState] : [])]) {
      if (state.kind === "failed") return { message: state.message, code: state.code };
      if (state.kind === "forbidden") return { message: state.message };
    }
    return undefined;
  };

  return (
    <Dialog
      open={props.open}
      onClose={props.onClose}
      title={`Filter ${props.topic} by expression`}
      description="Runs on the server, over every record in the range — not over the records already on screen."
      size="lg"
      /* The box holds work the operator typed, so a stray click on the veil must not discard it. */
      closeOnScrimClick={false}
      testId="smart-filter-dialog"
      actions={
        <>
          <Show when={props.onClear !== undefined}>
            <Button variant="secondary" onClick={() => props.onClear?.()}>
              Remove filter
            </Button>
          </Show>
          <Button variant="secondary" onClick={props.onClose}>
            Cancel
          </Button>
          <Button
            variant="secondary"
            icon="filter"
            busy={props.testState.kind === "running"}
            {...disabledProps(testDisabledReason())}
            onClick={() => {
              const chosen = sample();
              if (chosen !== undefined) {
                setTested({ source: effectiveSource(), sample: chosen, topic: props.topic });
                props.onTest(effectiveSource(), chosen);
              }
            }}
          >
            Try it on one record
          </Button>
          <Button
            variant="primary"
            icon="check"
            busy={props.applyState.kind === "running"}
            {...disabledProps(applyDisabledReason())}
            onClick={() => props.onApply(effectiveSource())}
          >
            Use this filter
          </Button>
        </>
      }
    >
      <div class="kui-smart-filter">
        <div class="kui-smart-filter__modes" role="group" aria-label="Filter editor mode">
          <button
            type="button"
            class="kui-smart-filter__mode kui-focusable"
            aria-pressed={mode() === "field" ? "true" : "false"}
            onClick={() => setMode("field")}
          >
            Field filter
          </button>
          <button
            type="button"
            class="kui-smart-filter__mode kui-focusable"
            aria-pressed={mode() === "advanced" ? "true" : "false"}
            onClick={() => setMode("advanced")}
          >
            Advanced CEL
          </button>
        </div>

        <Show
          when={mode() === "field"}
          fallback={
            <label class="kui-smart-filter__field">
              <span class="kui-smart-filter__label">Expression</span>
              {/* A textarea rather than the kernel's `TextField`: a CEL predicate wraps, and a filter
                  that has scrolled off the right edge of a one-line box is a filter nobody can check. */}
              <textarea
                class="kui-smart-filter__editor kui-focusable"
                rows={3}
                spellcheck={false}
                autocapitalize="off"
                autocorrect="off"
                autocomplete="off"
                placeholder={'record.value.status == "CAPTURED"'}
                value={source()}
                onInput={(event) => setSource(event.currentTarget.value)}
              />
            </label>
          }
        >
          <section class="kui-smart-filter__builder" aria-label="Field filter">
            <div class="kui-smart-filter__builder-grid">
              <Select<JsonPathFilterTarget>
                label="Payload"
                options={[
                  { value: "value", label: "Value" },
                  { value: "key", label: "Key" },
                ]}
                value={fieldTarget()}
                onChange={setFieldTarget}
              />
              <TextField
                label="Field path"
                name="json-path-filter-path"
                value={fieldPath()}
                mono
                placeholder="$.customer.address.city"
                help="JSONPath-style field access. Arrays use [0]; unusual names use ['field-name']."
                onInput={setFieldPath}
              />
              <Select<JsonPathComparisonOperator>
                label="Comparison"
                options={[
                  { value: "==", label: "equals" },
                  { value: "!=", label: "does not equal" },
                  { value: "contains", label: "contains" },
                  { value: ">", label: "greater than" },
                  { value: ">=", label: "at least" },
                  { value: "<", label: "less than" },
                  { value: "<=", label: "at most" },
                ]}
                value={fieldOperator()}
                onChange={setFieldOperator}
              />
              <Select<FieldValueType>
                label="Value type"
                options={[
                  { value: "string", label: "Text" },
                  { value: "number", label: "Number" },
                  { value: "boolean", label: "True / false" },
                  { value: "null", label: "Null" },
                ]}
                value={fieldValueType()}
                onChange={setFieldValueType}
              />
              <Show when={fieldValueType() !== "null"}>
                <Show
                  when={fieldValueType() === "boolean"}
                  fallback={
                    <TextField
                      label="Compare with"
                      name="json-path-filter-value"
                      value={fieldValue()}
                      mono
                      type={fieldValueType() === "number" ? "number" : "text"}
                      onInput={setFieldValue}
                    />
                  }
                >
                  <Select<"true" | "false">
                    label="Compare with"
                    options={[
                      { value: "true", label: "True" },
                      { value: "false", label: "False" },
                    ]}
                    value={fieldValue() === "false" ? "false" : "true"}
                    onChange={setFieldValue}
                  />
                </Show>
              </Show>
            </div>
            <Show when={suggestions().length > 0}>
              <div class="kui-smart-filter__suggestions" aria-label="Fields found in records on screen">
                <span class="kui-smart-filter__suggestions-label">Fields on screen</span>
                <For each={suggestions()}>
                  {(path) => (
                    <button
                      type="button"
                      class="kui-smart-filter__suggestion kui-focusable"
                      onClick={() => setFieldPath(path)}
                    >
                      <code>{path}</code>
                    </button>
                  )}
                </For>
              </div>
            </Show>
            <p class="kui-smart-filter__compiled">
              Runs as <code>{effectiveSource()}</code>
            </p>
          </section>
        </Show>

        <Show when={problem()}>
          {(stated) => <p class="kui-smart-filter__problem">{stated()}</p>}
        </Show>

        <section class="kui-smart-filter__preview" aria-label="Try the filter on one record">
          <div class="kui-smart-filter__preview-head">
            <Select<string>
              label="Try it against"
              size="sm"
              options={props.samples.map((record, index) => ({
                value: String(index),
                label: `Partition ${String(record.partition)}, offset ${record.offset}`,
              }))}
              value={String(sampleAt())}
              onChange={(value) => setSampleAt(Number(value))}
              disabled={props.samples.length === 0}
              disabledReason="Nothing has been read yet, so there is no record to try this against."
              emptyMessage="No records have been read yet."
            />
            <Show when={props.samples.length === 0}>
              <p class="kui-smart-filter__hint">
                No records are on screen yet, so there is nothing to try this against. Read some
                first — the preview runs against a record the topic really holds, never a made-up
                one.
              </p>
            </Show>
          </div>

          <Show when={verdict()}>{(answer) => <Verdict verdict={answer()} />}</Show>
        </section>

        <Show when={failure()}>
          {(problem) => (
            <Banner
              tone="danger"
              message={`The server refused that expression: ${problem().message}`}
              {...(problem().code === undefined ? {} : { code: problem().code })}
            />
          )}
        </Show>

        <FilterHelp
          onUse={(example) => {
            setSource(example);
            setMode("advanced");
          }}
        />
      </div>
    </Dialog>
  );
}

function fieldFilterValue(type: FieldValueType, raw: string): string | number | boolean | null {
  switch (type) {
    case "string":
      return raw;
    case "number":
      return raw.trim() === "" ? Number.NaN : Number(raw);
    case "boolean":
      return raw !== "false";
    case "null":
      return null;
  }
}

/** Field paths observed in decoded records. Schema-backed values arrive here as JSON too. */
function jsonPathSuggestions(
  samples: readonly KafkaRecord[],
  target: JsonPathFilterTarget,
): readonly string[] {
  const found = new Set<string>();
  const limit = 16;

  const visit = (value: unknown, path: string, depth: number): void => {
    if (found.size >= limit || depth > 6) return;
    if (Array.isArray(value)) {
      if (value.length === 0) found.add(path);
      else visit(value[0], `${path}[0]`, depth + 1);
      return;
    }
    if (value !== null && typeof value === "object") {
      const entries = Object.entries(value as Record<string, unknown>);
      if (entries.length === 0) found.add(path);
      for (const [key, nested] of entries) {
        const next = /^[A-Za-z_][A-Za-z0-9_]*$/.test(key)
          ? `${path}.${key}`
          : `${path}[${JSON.stringify(key)}]`;
        visit(nested, next, depth + 1);
        if (found.size >= limit) return;
      }
      return;
    }
    found.add(path);
  };

  for (const sample of samples) {
    const text = target === "value" ? (sample.value.kind === "json" ? sample.value.text : undefined) : sample.key;
    if (text === undefined || text === null) continue;
    try {
      visit(JSON.parse(text) as unknown, "$", 0);
    } catch {
      // A plain string key/value has no discoverable field path; the text predicates remain available.
    }
    if (found.size >= limit) break;
  }
  return [...found];
}

/**
 * What the preview found, in three shapes.
 *
 * Not the kernel's `Banner`: its tones are danger, warning and info, and a match is none of those —
 * it is good news, and drawing it in the same furniture as a problem is how an operator learns to
 * skim past all three. This is a small panel of its own with a tone per verdict.
 *
 * The distinction that has to survive is between the last two. "The expression threw on this record"
 * and "this record does not match" both arrive as `matched: false`, and they are opposite pieces of
 * news: one is a broken filter, the other is a working filter reporting a fact. They get different
 * colours, different glyphs and different words, and the failure says out loud what the empty list
 * it would produce looks like.
 */
function Verdict(props: { readonly verdict: FilterVerdict }): JSX.Element {
  const tone = (): "match" | "no-match" | "failed" =>
    props.verdict.kind === "matched" ? "match" : props.verdict.kind === "no-match" ? "no-match" : "failed";

  return (
    <div class={["kui-verdict", `kui-verdict--${tone()}`]} role="status">
      <Icon name={GLYPH[tone()]} size="16px" class="kui-verdict__glyph" />
      <div class="kui-verdict__body">
        <p class="kui-verdict__headline">{HEADLINE[tone()]}</p>
        <Show
          when={props.verdict.kind === "failed" ? props.verdict : undefined}
          fallback={<p class="kui-verdict__detail">{DETAIL[tone()]}</p>}
        >
          {(failed) => (
            <p class="kui-verdict__detail">
              {/* The server's own words first, because they name the field or the type that went
                  wrong and are the only part the operator can act on. */}
              <code class="kui-verdict__reason">{failed().reason}</code>
              {DETAIL.failed}
            </p>
          )}
        </Show>
      </div>
    </div>
  );
}

const GLYPH: Record<"match" | "no-match" | "failed", IconName> = {
  match: "check",
  "no-match": "filter",
  failed: "error",
};

const HEADLINE: Record<"match" | "no-match" | "failed", string> = {
  match: "This record matches",
  "no-match": "This record does not match",
  failed: "The expression threw on this record",
};

const DETAIL: Record<"match" | "no-match" | "failed", string> = {
  match: "A browse with this filter would deliver this record.",
  "no-match":
    "The expression ran and answered no. That is a working filter reporting a fact — a browse " +
    "with it would skip this record and keep the ones it does match.",
  failed:
    " — which is neither a match nor a non-match. A filter that throws on every record delivers " +
    "an empty list, and an empty list looks exactly like a topic with nothing in it.",
};

/** The vocabulary, beside the box rather than in a manual somebody has to go and find. */
function FilterHelp(props: { readonly onUse: (source: string) => void }): JSX.Element {
  return (
    <details class="kui-smart-filter__help">
      <summary class="kui-smart-filter__help-summary kui-focusable">
        What a filter can talk about
      </summary>
      <div class="kui-smart-filter__help-body">
        <p class="kui-smart-filter__help-note">
          Expressions are CEL. Every one is about a single <code>record</code>, and it has to answer
          true or false — an expression that returns anything else is refused when it runs, not when
          it compiles.
        </p>
        <dl class="kui-smart-filter__variables">
          <For each={FILTER_VARIABLES}>
            {(variable) => (
              <>
                <dt>
                  <code>{variable.name}</code> <span class="kui-smart-filter__type">{variable.type}</span>
                </dt>
                <dd>{variable.describe}</dd>
              </>
            )}
          </For>
        </dl>
        <p class="kui-smart-filter__help-note">
          <code>record.key</code> and <code>record.value</code> are <strong>absent</strong>, not
          null, when the payload is not JSON — so a filter reading a field of a plain-text value
          throws rather than quietly matching nothing. Avro, Protobuf and JSON Schema payloads are
          decoded to JSON before a filter runs, so field paths, array indexing and array predicates
          (below) work on those topics too — with one Avro wrinkle: a nullable field decodes as{" "}
          <code>{"{"}"&lt;type&gt;": value{"}"}</code>, so its path includes the branch type (see the
          example below).
        </p>
        <ul class="kui-smart-filter__examples">
          <For each={FILTER_EXAMPLES}>
            {(example) => (
              <li>
                <button
                  type="button"
                  class="kui-smart-filter__example kui-focusable"
                  onClick={() => props.onUse(example.source)}
                >
                  <code>{example.source}</code>
                </button>
                <span class="kui-smart-filter__example-note">{example.describe}</span>
              </li>
            )}
          </For>
        </ul>
      </div>
    </details>
  );
}

/**
 * A control that cannot be used is disabled *with the reason*, never hidden.
 *
 * `Button` pairs `disabled: true` with a required `disabledReason` in its type, so the two have to
 * be spread together — under `exactOptionalPropertyTypes` passing either one as `undefined` is a
 * different type from omitting it. The same helper, for the same reason, as
 * `feature-topics/src/TopicPage.tsx`.
 */
function disabledProps(
  reason: string | undefined,
): { readonly disabled: true; readonly disabledReason: string } | Record<string, never> {
  return reason === undefined ? {} : { disabled: true, disabledReason: reason };
}
