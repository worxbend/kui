import type { JSX } from "@solidjs/web";
import { For, Show, createMemo, createUniqueId } from "solid-js";
import { Button, Checkbox } from "@kui/kernel";

import { analyzeJsonValue, formatJsonValue, tokenizeJsonValue } from "./jsonValue.js";

const MAX_HIGHLIGHT_CHARACTERS = 200_000;

export interface JsonValueEditorProps {
  readonly value: string;
  readonly onInput: (value: string) => void;
  readonly disabled?: boolean | undefined;
  readonly help: string;
  readonly minifyBeforeSending: boolean;
  readonly onMinifyChange: (checked: boolean) => void;
  /**
   * The operator's word that a value which starts like JSON is not JSON.
   *
   * Offered only while the grammar check fails. String topics carry log lines such as
   * `[INFO] started`, and without this the check would leave them impossible to send.
   */
  readonly sendAsText: boolean;
  readonly onSendAsTextChange: (checked: boolean) => void;
}

/** A native textarea with a safe, non-interactive JSON syntax layer behind its text. */
export function JsonValueEditor(props: JsonValueEditorProps): JSX.Element {
  const generated = createUniqueId();
  const inputId = `kui-json-value-${generated}`;
  const helpId = `kui-json-value-help-${generated}`;
  const statusId = `kui-json-value-status-${generated}`;
  const analysis = createMemo(() => analyzeJsonValue(props.value));
  const highlightBounded = () => props.value.length <= MAX_HIGHLIGHT_CHARACTERS;
  const tokens = createMemo(() =>
    highlightBounded()
      ? tokenizeJsonValue(props.value)
      : [{ kind: "text" as const, text: props.value }],
  );
  const validJson = () => analysis().kind === "valid-json";
  let highlight: HTMLPreElement | undefined;

  const status = (): string => {
    const current = analysis();
    if (current.kind === "valid-json") {
      return highlightBounded() ? "Valid JSON" : "Valid JSON · highlighting paused for size";
    }
    if (current.kind === "invalid-json") return current.message;
    return current.kind === "empty" ? "Plain text or JSON" : "Plain text";
  };

  return (
    <div
      class={[
        "kui-json-editor",
        {
          "kui-json-editor--invalid": analysis().kind === "invalid-json",
          "kui-json-editor--disabled": props.disabled === true,
        },
      ]}
    >
      <div class="kui-json-editor__heading">
        <label for={inputId} class="kui-json-editor__label">Value</label>
        <span
          id={statusId}
          class={[
            "kui-json-editor__status",
            {
              "kui-json-editor__status--valid": validJson(),
              "kui-json-editor__status--invalid": analysis().kind === "invalid-json",
            },
          ]}
          role={analysis().kind === "invalid-json" ? "alert" : "status"}
        >
          {status()}
        </span>
      </div>

      <div class="kui-json-editor__surface">
        <pre ref={highlight} class="kui-json-editor__highlight" aria-hidden="true"><code><For each={tokens()}>{(token) => (
          <span class={`kui-json-editor__token kui-json-editor__token--${token.kind}`}>
            {token.text}
          </span>
        )}</For>{props.value.endsWith("\n") ? "\n" : ""}</code></pre>
        <textarea
          id={inputId}
          aria-label="Value"
          class="kui-json-editor__input"
          value={props.value}
          rows={8}
          disabled={props.disabled === true}
          aria-invalid={analysis().kind === "invalid-json" ? "true" : undefined}
          aria-describedby={`${helpId} ${statusId}`}
          autocomplete="off"
          autocapitalize="off"
          spellcheck={false}
          onInput={(event) => props.onInput(event.currentTarget.value)}
          onScroll={(event) => {
            if (highlight === undefined) return;
            highlight.scrollTop = event.currentTarget.scrollTop;
            highlight.scrollLeft = event.currentTarget.scrollLeft;
          }}
        />
      </div>

      <div class="kui-json-editor__actions">
        <Button
          variant="ghost"
          size="sm"
          {...(!validJson() || props.disabled === true
            ? {
                disabled: true as const,
                disabledReason:
                  props.disabled === true
                    ? "A tombstone carries no value."
                    : "Enter valid JSON before formatting it.",
              }
            : {})}
          onClick={() => props.onInput(formatJsonValue(props.value, "pretty"))}
        >
          Format JSON
        </Button>
        <Checkbox
          label="Minify before sending"
          checked={props.minifyBeforeSending}
          disabled={!validJson() || props.disabled === true}
          onChange={props.onMinifyChange}
        />
        <Show when={analysis().kind === "invalid-json"}>
          <Checkbox
            label="Send as plain text"
            checked={props.sendAsText}
            disabled={props.disabled === true}
            onChange={props.onSendAsTextChange}
          />
        </Show>
      </div>

      <span id={helpId} class="kui-json-editor__help">
        {props.help} Valid JSON can stay formatted here and be compacted only in the request.
      </span>
    </div>
  );
}
