/**
 * Registering a schema.
 *
 * ## What this form is actually deciding
 *
 * A registration is not a create-if-absent. Under a subject that already exists it is a *new
 * version*, and whether the registry accepts it is decided by that subject's compatibility level —
 * so the same three fields, submitted twice, are two different acts. The dialog says which one is
 * about to happen, from the subject list the screen already holds, rather than leaving somebody to
 * work it out from whether the name is familiar.
 *
 * ## The subject name is not validated against a pattern
 *
 * Unlike a topic name, which Kafka constrains to `[a-zA-Z0-9._-]{1,249}` and which
 * `CreateTopicDialog` therefore checks in the browser, a Confluent-compatible registry places no
 * documented restriction on a subject beyond it being a non-empty string — the naming strategies
 * that produce `<topic>-value` are a *client* convention, not a registry rule. Inventing a pattern
 * here would refuse subject names a registry accepts, which is a worse failure than a round trip.
 *
 * What is checked is the schema text, and only where checking it is certain: an Avro or JSON schema
 * that is not JSON at all is caught by `proposedSchemaProblem` with the parser's own position,
 * because the gateway's answer for the same text — "Could not execute compatibility rule on invalid
 * Avro schema" — names nothing and arrives a second later. Protobuf is left alone: a `.proto`
 * definition is its own syntax.
 *
 * ## A refusal keeps everything typed
 *
 * The registry refusing a schema is the *ordinary* outcome this dialog exists for, not an accident:
 * an incompatible field is what the compatibility level is there to catch. So a failure leaves the
 * dialog open with the schema in the box and the registry's own words above the actions — the
 * operator's next act is to edit one field and press again, and a dialog that closed would make
 * them paste it all back.
 */
import { Show, createMemo, createSignal, createUniqueId } from "solid-js";
import type { JSX } from "@solidjs/web";
import { Banner, Button, Dialog, Select, TextField, type Mutation } from "@kui/kernel";
import { proposedSchemaProblem, type ProposedSchema, type RegisteredSchema } from "./data.js";

/** The schema languages a Confluent-compatible registry knows. The registry decides, not KUI. */
const SCHEMA_TYPES = ["AVRO", "JSON", "PROTOBUF"] as const;

export interface RegisterSchemaDialogProps {
  readonly open: boolean;
  readonly onClose: () => void;
  /** Runs the write. The dialog stays open until `state` says it is done. */
  readonly onRegister: (subject: string, proposed: ProposedSchema) => void;
  readonly state: Mutation<RegisteredSchema>;
  /**
   * The subjects this registry already holds, as far as the page has read them.
   *
   * Used only to say whether this will be a new subject or a new version of one — never to refuse
   * a name. The page holds one page of a registry that may have four thousand subjects, so a name
   * that is absent from this list is not a name the registry does not have.
   */
  readonly knownSubjects: readonly string[];
  readonly writeBlocked?: ((subject: string) => string | undefined) | undefined;
}

export function RegisterSchemaDialog(props: RegisterSchemaDialogProps): JSX.Element {
  const [subject, setSubject] = createSignal("");
  const [schemaType, setSchemaType] = createSignal<string>("AVRO");
  const [definition, setDefinition] = createSignal("");

  const problem = createMemo(() => proposedSchemaProblem(schemaType(), definition()));
  const busy = () => props.state.kind === "running";
  const editorId = createUniqueId();

  const existing = () => props.knownSubjects.includes(subject().trim());

  const canRegister = () =>
    props.writeBlocked?.(subject().trim()) === undefined &&
    subject().trim() !== "" && definition().trim() !== "" && problem() === undefined && !busy();

  /** Why the button will not press. Never empty: `Button`'s type refuses that. */
  const blockedReason = (): string => {
    const refusal = props.writeBlocked?.(subject().trim());
    if (refusal !== undefined) return refusal;
    if (busy()) return "The registry is being asked to accept this schema.";
    if (subject().trim() === "") return "Name the subject this schema belongs to first.";
    if (definition().trim() === "") return "Paste the schema you want to register first.";
    return problem() ?? "This schema cannot be registered.";
  };

  return (
    <Dialog
      open={props.open}
      onClose={props.onClose}
      title="Register a schema"
      description={
        "The registry decides whether to accept it, under the compatibility level in force for " +
        "the subject."
      }
      size="lg"
      /* The operator has pasted a schema. A stray click on the veil must not discard it; Cancel and
         Escape are the two deliberate ways out. */
      closeOnScrimClick={false}
      testId="register-schema-dialog"
      actions={
        <>
          <Show
            when={busy()}
            fallback={
              <Button variant="ghost" onClick={props.onClose}>
                Cancel
              </Button>
            }
          >
            <Button variant="ghost" disabled disabledReason="The registry is answering.">
              Cancel
            </Button>
          </Show>
          <Show
            when={canRegister()}
            fallback={
              <Button
                variant="primary"
                icon="plus"
                busy={busy()}
                disabled
                disabledReason={blockedReason()}
              >
                Register
              </Button>
            }
          >
            <Button
              variant="primary"
              icon="plus"
              onClick={() => {
                if (!canRegister()) return;
                props.onRegister(subject().trim(), {
                  schemaType: schemaType(),
                  definition: definition(),
                });
              }}
            >
              Register
            </Button>
          </Show>
        </>
      }
    >
      <div class="kui-schema-register">
        <TextField
          label="Subject"
          value={subject()}
          onInput={setSubject}
          placeholder="orders.payments.v2-value"
          required
          mono
          /* The one thing this list can honestly say. A subject already on screen means this is a
             new *version* under a compatibility level that may refuse it; a subject that is not on
             screen means either a new subject or a page of the registry the browser has not read,
             and the sentence says so rather than promising the first. */
          help={
            subject().trim() === ""
              ? "The registry's own name for this schema. Clients usually derive it from the topic."
              : existing()
                ? "This subject is already in the registry, so this registers a new version of " +
                  "it — the subject's compatibility level decides whether it is accepted."
                : "No subject with this name is on screen. If the registry has none either, this " +
                  "creates the subject."
          }
        />

        <Select
          label="Schema type"
          value={schemaType()}
          options={SCHEMA_TYPES.map((one) => ({ value: one, label: one }))}
          onChange={setSchemaType}
        />

        <label class="kui-schema-register__field">
          <span class="kui-schema-register__label">Schema</span>
          {/* A textarea rather than the kernel's `TextField`, for the reason `CompatibilityCheck`
              gives: a schema is dozens of lines and one that has scrolled off the right edge of a
              single-line box is one nobody can read before registering it. `spellcheck` and the
              autocapitalise family off because this is source text — a browser that capitalises a
              field name registers a different schema than the one that was pasted. */}
          <textarea
            id={editorId}
            class="kui-schema-check__editor kui-focusable"
            rows={14}
            spellcheck={false}
            autocapitalize="off"
            autocorrect="off"
            autocomplete="off"
            value={definition()}
            onInput={(event) => setDefinition(event.currentTarget.value)}
          />
        </label>

        <Show when={problem()}>
          {(stated) => (
            <p class="kui-schema-check__problem" role="alert">
              {stated()}
            </p>
          )}
        </Show>

        <Show when={props.state.kind === "failed" || props.state.kind === "forbidden"}>
          {/* The registry's own words, whichever refusal this is: an incompatible schema, a cluster
              KUI is configured read-only for, or a principal without SCHEMA:CREATE. All three
              arrive here as sentences the server wrote, and none of them is paraphrased. */}
          <Banner
            tone="danger"
            message={
              props.state.kind === "failed" || props.state.kind === "forbidden"
                ? props.state.message
                : ""
            }
            {...(props.state.kind === "failed" ? { code: props.state.code } : {})}
          />
        </Show>
      </div>
    </Dialog>
  );
}
