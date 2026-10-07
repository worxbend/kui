/**
 * "Check a schema": would the registry accept this, and if not, why not.
 *
 * ## The panel says it writes nothing, on the panel
 *
 * A control next to a schema registry that reads "check" and might register something is a control
 * an operator will not press, and the ones who do press it will have been guessing. So the sentence
 * "nothing is registered" is on the panel itself rather than in a tooltip or in documentation — the
 * endpoint behind it carries no mutation marker on the server, is answered on a read-only cluster
 * like any other read, and running it twice does what running it once does.
 *
 * ## The registry's words, not this screen's
 *
 * A refusal is reproduced verbatim, one entry per line, in a monospaced block. A Confluent registry
 * answers with the field path, the reader and writer types, the version it compared against and the
 * whole of that older schema; those are the facts that say what to change. Rewriting them into
 * "not backward compatible" would leave the operator knowing only what the red pill already told
 * them.
 *
 * A refusal with **no** messages is a real answer this gateway gives — Apicurio's
 * Confluent-compatible API words its explanation under a key KUI's registry client does not read —
 * and it is said out loud rather than drawn as an empty area. "Refused, and the registry gave no
 * reason" and "refused, for these five reasons" must never look alike.
 *
 * ## Why a level of NONE disables the button
 *
 * `NONE` means the registry checks nothing, so it answers "compatible" for every schema, including
 * one that breaks every reader of the topic. The verdict document is identical either way. Running
 * the check there would hand somebody a green pill as evidence for a change that is about to break
 * production, so the control is disabled and the reason is the sentence explaining it — disabled
 * rather than hidden, because a missing control teaches an operator the product cannot do the thing.
 */
import { For, Show, createEffect, createMemo, createSignal, untrack } from "solid-js";
import type { JSX } from "@solidjs/web";
import { Banner, Button, Select, StatusPill, type Mutation } from "@kui/kernel";
import {
  checkBlockedReason,
  checkIsMeaningful,
  proposedSchemaProblem,
  type CompatibilityLevel,
  type CompatibilityVerdict,
  type ProposedSchema,
} from "./data.js";

/** The schema languages a Confluent-compatible registry knows. The registry decides, not KUI. */
const SCHEMA_TYPES = ["AVRO", "JSON", "PROTOBUF"] as const;

export interface CompatibilityCheckProps {
  readonly subject: string;
  /**
   * The level in force for this subject, as the registry reports it. `null` is a level the registry
   * named and this browser does not recognise, and `undefined` is "not read yet" — neither is
   * `NONE`, and the panel treats all three differently.
   */
  readonly level: CompatibilityLevel | null | undefined;
  /**
   * The schema type the panel starts on. The registered version's type, so that somebody editing an
   * Avro schema is not asked to choose Avro first.
   */
  readonly initialSchemaType?: string | undefined;
  /**
   * What the box starts with. The registered definition, so the ordinary use — paste the current
   * schema, change one field, ask — needs no copying between windows.
   */
  readonly initialDefinition?: string | undefined;
  readonly onCheck: (proposed: ProposedSchema) => void;
  readonly state: Mutation<CompatibilityVerdict>;
}

export function CompatibilityCheck(props: CompatibilityCheckProps): JSX.Element {
  // Initialize late-arriving definitions while pristine, never over an operator's edits.
  // A different subject is a different proposal and deliberately starts a fresh editor.
  const [schemaType, setSchemaType] = createSignal("AVRO");
  const [definition, setDefinition] = createSignal("");
  const fingerprint = () => JSON.stringify([props.subject, props.level, schemaType(), definition(), props.initialSchemaType, props.initialDefinition]);
  // Preloaded story results describe the initial inputs; live results are bound at submission.
  const [submitted, setSubmitted] = createSignal<string | undefined>(untrack(() =>
    JSON.stringify([props.subject, props.level, props.initialSchemaType ?? "AVRO", props.initialDefinition ?? "", props.initialSchemaType, props.initialDefinition]),
  ));
  const resultCurrent = () => submitted() === fingerprint();
  let dirty = false;
  let subject: string | undefined;
  createEffect(
    () => [props.subject, props.initialSchemaType, props.initialDefinition] as const,
    ([nextSubject, initialType, initialDefinition]) => {
      if (subject !== nextSubject) {
        if (subject !== undefined) setSubmitted(undefined);
        subject = nextSubject;
        dirty = false;
      }
      if (!dirty) {
        setSchemaType(initialType ?? "AVRO");
        setDefinition(initialDefinition ?? "");
      }
    },
  );

  const problem = createMemo(() => proposedSchemaProblem(schemaType(), definition()));
  const busy = () => props.state.kind === "running";
  const meaningful = () => props.level === undefined || checkIsMeaningful(props.level);

  /** Why the check cannot run. Never empty: `Button`'s type refuses that. Decided in `data.ts`. */
  const blockedReason = (): string | undefined =>
    checkBlockedReason({
      level: props.level,
      schemaType: schemaType(),
      definition: definition(),
      busy: busy(),
    });

  const verdict = (): CompatibilityVerdict | undefined =>
    resultCurrent() && props.state.kind === "done" ? props.state.value : undefined;

  return (
    <section class="kui-schema-check" aria-label={`Check a schema against ${props.subject}`}>
      <header class="kui-schema-check__head">
        <h2 class="kui-schema-check__title">Check a schema</h2>
        {/* On the panel, not in a tooltip. See the note at the top of this file. */}
        <p class="kui-schema-check__promise">
          Asks the registry whether it would accept this schema for this subject. Nothing is
          registered and nothing is changed — this is a question, and the answer is the registry's.
        </p>
      </header>

      <Show when={!meaningful()}>
        {/* Before the box, because it is the reason the button below it will not press, and reading
            it afterwards means having typed a schema for nothing. */}
        <Banner
          tone="warning"
          message={`This subject's compatibility level is NONE. The registry checks nothing, so it would accept any schema and answer “compatible” whatever is checked against it. Set a level on the subject, or on the registry, before the answer means anything.`}
        />
      </Show>

      <div class="kui-schema-check__controls">
        <Select
          label="Schema type"
          size="sm"
          value={schemaType()}
          options={SCHEMA_TYPES.map((one) => ({ value: one, label: one }))}
          onChange={(value) => { dirty = true; setSubmitted(undefined); setSchemaType(value); }}
        />
        <span class="kui-schema-check__against">
          Checked against the latest registered version of{" "}
          <code>{props.subject}</code>
        </span>
      </div>

      <label class="kui-schema-check__field">
        <span class="kui-schema-check__label">Proposed schema</span>
        {/* A textarea rather than the kernel's `TextField`: a schema is dozens of lines, and one
            that has scrolled off the right edge of a single-line box is one nobody can check.
            `spellcheck` and the autocapitalise family off because this is source text — a browser
            that capitalises a field name proposes a different schema than the one that was typed. */}
        <textarea
          class="kui-schema-check__editor kui-focusable"
          rows={12}
          spellcheck={false}
          autocapitalize="off"
          autocorrect="off"
          autocomplete="off"
          value={definition()}
          onInput={(event) => { dirty = true; setSubmitted(undefined); setDefinition(event.currentTarget.value); }}
        />
      </label>

      <Show when={problem()}>
        {(stated) => (
          <p class="kui-schema-check__problem" role="alert">
            {stated()}
          </p>
        )}
      </Show>

      <div class="kui-schema-check__actions">
        {/* Two branches rather than `disabled={...}`: `Button` makes `disabledReason` mandatory
            exactly when `disabled` is true, and that rule is what stops a greyed-out control with no
            explanation anywhere on the screen. */}
        <Show
          when={blockedReason() === undefined}
          fallback={
            <Button
              variant="secondary"
              icon="check"
              busy={busy()}
              disabled
              disabledReason={blockedReason() ?? "This schema cannot be checked."}
            >
              Check compatibility
            </Button>
          }
        >
          <Button
            variant="secondary"
            icon="check"
            onClick={() => {
              setSubmitted(fingerprint());
              props.onCheck({ schemaType: schemaType(), definition: definition() });
            }}
          >
            Check compatibility
          </Button>
        </Show>
      </div>

      <Show when={resultCurrent() && (props.state.kind === "forbidden" || props.state.kind === "failed")}>
        <Banner
          tone="danger"
          message={
            props.state.kind === "forbidden" || props.state.kind === "failed"
              ? props.state.message
              : ""
          }
          {...(props.state.kind === "failed" ? { code: props.state.code } : {})}
        />
      </Show>

      <Show when={verdict()}>
        {(answer) => (
          <div
            class="kui-schema-check__verdict"
            aria-live="polite"
            data-testid="compatibility-verdict"
          >
            <StatusPill tone={answer().compatible ? "success" : "danger"} dot>
              {answer().compatible ? "Would be accepted" : "Would be refused"}
            </StatusPill>

            <Show
              when={answer().messages.length > 0}
              fallback={
                <Show
                  when={!answer().compatible}
                  fallback={
                    <p class="kui-schema-check__note">
                      The registry raised nothing against it. It compared this schema with the latest
                      registered version under the level in force for the subject.
                    </p>
                  }
                >
                  {/* An absence, said as one. A refusal with no reason and a refusal with five must
                      never render as the same shape of thing. */}
                  <p class="kui-schema-check__note kui-schema-check__note--absent">
                    The registry refused it and gave no reason. Some registries word their
                    explanation in a field KUI does not read; the refusal itself stands.
                  </p>
                </Show>
              }
            >
              <>
                <p class="kui-schema-check__note">
                  The registry's own words, unedited:
                </p>
                <ul class="kui-schema-check__messages">
                  <For each={answer().messages}>
                    {(message) => <li>{message}</li>}
                  </For>
                </ul>
              </>
            </Show>
          </div>
        )}
      </Show>
    </section>
  );
}
