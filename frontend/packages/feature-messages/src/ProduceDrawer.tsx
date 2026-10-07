/**
 * Composing a record and writing it.
 *
 * ## The tombstone is a control, not an empty box
 *
 * On a compacted topic a record whose value is null tells Kafka to delete that key for good. A
 * record whose value is the empty string is an ordinary record that happens to hold no characters.
 * One text box cannot express both, and inferring the tombstone from an empty box would mean an
 * operator who cleared the field to start again deleted a key by pressing Backspace. So the
 * tombstone is a switch, it says what it does in words, and turning it on disables the value box
 * rather than hiding it — hiding the box would leave the operator wondering where their text went.
 *
 * ## Why the key is not required, and why that is stated
 *
 * A record with no key is partitioned round-robin; a record with a key always lands on the same
 * partition. Operators reaching for this drawer are usually reproducing something, and which of the
 * two they get changes whether the reproduction is faithful. The field says so rather than leaving
 * it to be discovered.
 *
 * ## The receipt
 *
 * A successful write answers with the partition, offset and timestamp the broker assigned, and the
 * drawer shows them instead of closing. "Sent" is not a fact an operator can check; "partition 3,
 * offset 148 991" is one they can go and look at.
 */
import { For, Show, createMemo, createSignal } from "solid-js";
import type { JSX } from "@solidjs/web";
import { Banner, Button, Drawer, Switch, TextField, type Mutation } from "@kui/kernel";
import { JsonValueEditor } from "./JsonValueEditor.jsx";
import { analyzeJsonValue, formatJsonValue } from "./jsonValue.js";
import {
  EMPTY_RECORD_DRAFT,
  draftProblem,
  type HeaderDraft,
  type ProducedRecord,
  type RecordDraft,
} from "./produce.js";

export interface ProduceDrawerProps {
  readonly open: boolean;
  readonly onClose: () => void;
  readonly topic: string;
  /**
   * How many partitions the topic has, for the field's help text.
   *
   * `undefined` means KUI has not been told — never `0`, which would be a claim, and which is what
   * the route above passed for the whole life of this drawer. The help text says which of the two
   * it is, because "leave it empty" is good advice and "0 to -1" is not.
   */
  readonly partitionCount?: number | undefined;
  readonly onSend: (draft: RecordDraft) => void;
  readonly state: Mutation<readonly ProducedRecord[]>;
  readonly disabledReason?: string | undefined;
}

export function ProduceDrawer(props: ProduceDrawerProps): JSX.Element {
  const [draft, setDraft] = createSignal<RecordDraft>(EMPTY_RECORD_DRAFT);
  const [partitionText, setPartitionText] = createSignal("");
  const [countText, setCountText] = createSignal("1");
  const [minifyBeforeSending, setMinifyBeforeSending] = createSignal(false);

  const patch = (change: Partial<RecordDraft>): void => {
    setDraft({ ...draft(), ...change });
  };

  const tombstone = () => draft().value === null;
  const busy = () => props.state.kind === "running";
  const valueAnalysis = createMemo(() => {
    const value = draft().value;
    return value === null ? ({ kind: "empty" } as const) : analyzeJsonValue(value);
  });
  const problem = createMemo(() => {
    if (props.disabledReason !== undefined) return props.disabledReason;
    const structural = draftProblem(draft());
    if (structural !== undefined) return structural;
    const value = valueAnalysis();
    return value.kind === "invalid-json" ? value.message : undefined;
  });
  const canSend = () => problem() === undefined && !busy();

  const send = (): void => {
    if (!canSend()) return;
    const current = draft();
    if (
      minifyBeforeSending() &&
      current.value !== null &&
      valueAnalysis().kind === "valid-json"
    ) {
      props.onSend({ ...current, value: formatJsonValue(current.value, "compact") });
      return;
    }
    props.onSend(current);
  };

  const receipt = (): readonly ProducedRecord[] | undefined =>
    props.state.kind === "done" ? props.state.value : undefined;

  const failure = () =>
    props.state.kind === "failed" || props.state.kind === "forbidden" ? props.state : undefined;

  const setHeader = (index: number, change: Partial<HeaderDraft>): void => {
    patch({
      headers: draft().headers.map((header, at) =>
        at === index ? { ...header, ...change } : header,
      ),
    });
  };

  return (
    <Drawer
      open={props.open}
      onClose={props.onClose}
      title={`Produce into ${props.topic}`}
      description="The record is written to this topic on this cluster. It cannot be taken back."
      /* Everything in here was typed by hand. A stray click on the veil must not discard it. */
      testId="produce-drawer"
      error={
        failure() === undefined
          ? undefined
          : {
              message: failure()!.message,
              code: failure()!.kind === "failed" ? (failure() as { code: string }).code : undefined,
            }
      }
      footer={
        <>
          <Show
            when={busy()}
            fallback={
              <Button variant="ghost" onClick={props.onClose}>
                Close
              </Button>
            }
          >
            <Button variant="ghost" disabled disabledReason="The record is being written.">
              Close
            </Button>
          </Show>
          <Show
            when={canSend()}
            fallback={
              <Button
                variant="primary"
                icon="send"
                busy={busy()}
                disabled
                disabledReason={busy() ? "The record is being written." : (problem() ?? "")}
              >
                {sendLabel(draft())}
              </Button>
            }
          >
            <Button variant="primary" icon="send" onClick={send}>
              {sendLabel(draft())}
            </Button>
          </Show>
        </>
      }
    >
      <div class="kui-produce">
        <TextField
          label="Key"
          value={draft().key ?? ""}
          onInput={(value) => patch({ key: value === "" ? null : value })}
          mono
          help="A record with a key always lands on the same partition; a record without one is spread round-robin. Leave it empty for no key."
        />

        <div class="kui-produce__tombstone">
          <Switch
            label="Tombstone (delete this key)"
            checked={tombstone()}
            onChange={(on) => {
              /* The previous text is not kept and restored: an operator who turns the tombstone off
                 should see an empty box and decide what to write, not find an old draft they had
                 stopped thinking about sitting there, ready to send. */
              patch({ value: on ? null : "" });
            }}
          />
          {/* `Switch` carries no help text of its own, and this switch needs one more than any other
              control on the screen: it is the difference between writing a record and deleting a
              key for good. */}
          <p class="kui-produce__note">
            On a compacted topic this deletes the key permanently. It is not the same as an empty
            value.
          </p>
        </div>

        <JsonValueEditor
          value={draft().value ?? ""}
          onInput={(value) => patch({ value })}
          disabled={tombstone()}
          help={
            tombstone()
              ? "A tombstone carries no value at all."
              : "An empty box writes a record with an empty value, which is an ordinary record."
          }
          minifyBeforeSending={minifyBeforeSending()}
          onMinifyChange={setMinifyBeforeSending}
        />

        <TextField
          label="Partition"
          type="number"
          value={partitionText()}
          onInput={(text) => {
            setPartitionText(text);
            const value = Number(text);
            patch({
              partition: text.trim() === "" || !Number.isInteger(value) ? null : value,
            });
          }}
          placeholder="chosen by Kafka"
          help={
            props.partitionCount !== undefined && props.partitionCount > 0
              ? `0 to ${String(props.partitionCount - 1)}. Leave it empty to let the key decide.`
              : "KUI has not been told how many partitions this topic has. " +
                "Leave it empty to let the key decide."
          }
        />

        <fieldset class="kui-produce__headers">
          <legend>Headers</legend>
          <For each={draft().headers}>
            {(header, index) => (
              <div class="kui-produce__header">
                <TextField
                  label="Name"
                  value={header.name}
                  onInput={(name) => setHeader(index(), { name })}
                  mono
                />
                <TextField
                  label="Value"
                  value={header.value ?? ""}
                  onInput={(value) => setHeader(index(), { value })}
                  mono
                  disabled={header.value === null}
                />
                <Switch
                  label="No value"
                  checked={header.value === null}
                  onChange={(on) => {
                    setHeader(index(), { value: on ? null : "" });
                  }}
                />
                <Button
                  variant="ghost"
                  icon="minus"
                  onClick={() =>
                    patch({ headers: draft().headers.filter((_, at) => at !== index()) })
                  }
                >
                  Remove
                </Button>
              </div>
            )}
          </For>
          <Button
            variant="ghost"
            icon="plus"
            onClick={() => patch({ headers: [...draft().headers, { name: "", value: "" }] })}
          >
            Add a header
          </Button>
        </fieldset>

        <TextField
          label="How many copies"
          type="number"
          value={countText()}
          onInput={(text) => {
            setCountText(text);
            const value = Number(text);
            patch({ count: Number.isInteger(value) && value > 0 ? value : 0 });
          }}
          help="One, normally. More is for filling a topic while testing a consumer."
        />

        <Show when={receipt()}>
          {(records) => (
            <Banner
              tone="info"
              /* Where it landed, not that it was sent. "Sent" is not something an operator can go
                 and check; a partition and an offset are. */
              message={describeReceipt(records())}
            />
          )}
        </Show>
      </div>
    </Drawer>
  );
}

/** The button says what it will actually do, including how many records and whether they delete. */
export function sendLabel(draft: RecordDraft): string {
  const what = draft.value === null ? "tombstone" : "record";
  return draft.count === 1 ? `Produce ${what}` : `Produce ${draft.count} ${what}s`;
}

/** Where the records landed. Names each one while there are few enough to read. */
export function describeReceipt(records: readonly ProducedRecord[]): string {
  if (records.length === 0) {
    // The broker acknowledged without saying where. Better to say that than to claim a position.
    return "The cluster accepted the write but did not say where the record landed.";
  }
  if (records.length === 1) {
    const only = records[0] as ProducedRecord;
    return `Written to partition ${only.partition} at offset ${only.offset.toLocaleString()}.`;
  }
  const partitions = [...new Set(records.map((record) => record.partition))].sort((a, b) => a - b);
  return `Wrote ${records.length.toLocaleString()} records across ${partitions.length === 1 ? `partition ${partitions[0]}` : `partitions ${partitions.join(", ")}`}.`;
}
