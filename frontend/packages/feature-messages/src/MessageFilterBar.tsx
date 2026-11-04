/**
 * The row above the record list: where to start, which partitions, what text, and whether to follow.
 *
 * ## This bar is built once and never rebuilt
 *
 * A drawer must not rebuild while somebody is typing in it — a defect this project shipped, where
 * the filter box lost its caret every time results arrived. In Solid 2 the rule is concrete: the
 * bar is **not** inside a `<Show>` or `<Loading>` whose condition flips when records land, and the
 * record list is **not** a sibling that shares a keyed parent with it. Fine-grained reactivity
 * gives you that for free *if* the form stays outside the boundary that re-renders; putting it
 * inside is how the defect returns. `MessagesTab` keeps that arrangement and `messages.test.tsx`
 * asserts it by holding on to the input element and checking it is the same node after a record
 * arrives.
 *
 * ## Nothing here starts a browse
 *
 * Every control reports upwards and the screen decides. That is not indirection for its own sake:
 * changing *where* to read has to stop whatever is running (a browse in flight is reading a
 * different range from the one the controls now describe, and letting it keep delivering would mix
 * two ranges in one list with nothing on screen to say why), and that decision belongs to the thing
 * that owns the session.
 *
 * ## The bar wraps; it never scrolls sideways
 *
 * A control that has slid off the right edge of a bar is a control nobody finds.
 *
 * ## Three rows, and what divides them
 *
 * `SCREENS-V4.md` §3.12 draws three, and the division is by *who applies the control*, which is why
 * it is worth keeping rather than being a layout accident:
 *
 *   1. **Where to read.** Every control here is a parameter the browse endpoint accepts — the seek,
 *      the partitions, the plain substring, the tail.
 *   2. **What to keep.** The typed key and value predicates and the upper bounds have no query
 *      parameter at all; they compile to one CEL expression the message service evaluates per
 *      record (`predicates.ts`). The screen registers it before it starts a browse.
 *   3. **Presets.** A name this browser has given to a row-2 arrangement. Absent when there are
 *      none, rather than an empty row with a heading — §3.12 says so explicitly.
 */

import type { JSX } from "@solidjs/web";
import { For, Show, createEffect, createSignal, createUniqueId, onCleanup, untrack } from "solid-js";
import {
  Checkbox,
  FilterChip,
  FilterChipBar,
  Icon,
  Select,
  StatusPill,
  TextField,
} from "@kui/kernel";
import {
  offsetOf,
  partitionSummary,
  seekFor,
  seekKind,
  timestampOf,
  type SeekKind,
  type SeekMode,
} from "./browse.js";
import {
  MATCH_MODES,
  TIME_WINDOWS,
  windowOf,
  windowStart,
  type FieldPredicate,
  type MatchMode,
  type Predicates,
  type TimeWindow,
} from "./predicates.js";
import type { FilterPreset } from "./presets.js";

/** Whether live tailing can be offered at all, and why not when it cannot. */
export type LiveAvailability = { readonly available: true } | { readonly available: false; readonly reason: string };

export interface MessageFilterBarProps {
  readonly seek: SeekMode;
  readonly onSeekChange: (seek: SeekMode) => void;
  /**
   * How many partitions the topic has, or `undefined` when KUI has not been told.
   *
   * Optional, and never defaulted to a number. The picker's menu is a list of partition ids, so a
   * count nobody has measured means there is no list to offer — and the control says that, disabled,
   * with the reason on it. It read `all 0` and offered an empty menu for as long as the route above
   * it hard-coded a zero, which looks exactly like a topic that has no partitions.
   */
  readonly partitionCount?: number | undefined;
  /** Empty means every partition — which is what the server means by the parameter being absent. */
  readonly partitions: readonly number[];
  readonly onPartitionsChange: (partitions: readonly number[]) => void;
  readonly filter: string;
  readonly onFilterChange: (filter: string) => void;
  /** Fired when the filter is committed — Enter, the debounce elapsing, or a flush. */
  readonly onFilterCommit: (filter: string) => void;
  /**
   * Receives the function that commits, at once, whatever a debounced box is still holding.
   *
   * Read calls it before it starts a browse. Typing, Tab and Enter fits inside the debounce, and a
   * Read that went ahead without it started the browse the boxes no longer described — and the
   * commit that landed a moment later stopped that browse and left its unfiltered rows on screen.
   */
  readonly flushRef?: ((flush: () => void) => void) | undefined;
  readonly live: boolean;
  readonly onLiveChange: (live: boolean) => void;
  readonly liveAvailability?: LiveAvailability | undefined;

  /**
   * The smart filter running on this browse, and the way to change it.
   *
   * Optional as a whole. A deployment whose cluster has no filter engine is refused with
   * `KUI-UNSUPPORTED`, and on such a cluster the control is offered **disabled with that reason**
   * rather than left out, because a missing control reads as a product that cannot do the thing at
   * all.
   *
   * This used to say the quickstart is such a cluster. It is not: `MessageWiring.filterEnginesFor`
   * builds one CEL engine per *configured* cluster, so every cluster in a running deployment has
   * one, and a browse against the quickstart registers an expression and gets an id back — which
   * `frontend/e2e/messages.spec.ts` now drives against a real broker.
   */
  readonly smartFilter?: SmartFilterSlot | undefined;

  /**
   * The typed predicates and the upper bounds — everything the browse endpoint has no parameter for.
   *
   * The bar reports changes and applies nothing, the same rule every other control here follows.
   * Compiling these to an expression and registering it is the screen's job, because it needs a
   * client and because a browse in flight has to be stopped first.
   */
  readonly predicates: Predicates;
  readonly onPredicatesChange: (predicates: Predicates) => void;

  /**
   * The clock the quick time windows are measured from.
   *
   * Passed in rather than read from `Date.now()` so that a story renders the same window on every
   * screenshot and a test can assert the instant a chip produces.
   */
  readonly now?: number | undefined;

  /**
   * The named arrangements this browser has saved, and what may be done with them.
   *
   * An empty list draws **no** presets row — not an empty one with a heading. §3.12 is explicit, and
   * it is the right rule: a heading over nothing tells the reader a feature is broken rather than
   * unused.
   */
  readonly presets?: readonly FilterPreset[] | undefined;
  readonly onApplyPreset?: ((preset: FilterPreset) => void) | undefined;
  readonly onRemovePreset?: ((preset: FilterPreset) => void) | undefined;
  /** Offered only when there is something to save; the screen decides what the name is. */
  readonly onSavePreset?: (() => void) | undefined;

  /** Anything the screen wants at the right of the bar, before the LIVE pill. */
  readonly children?: JSX.Element;
}

/**
 * The smart-filter control's state, as the screen owns it.
 *
 * The bar shows what is running and asks for the editor; it never registers, tests or clears
 * anything itself. That is the same rule every other control here follows and it is in the header:
 * changing what a browse reads has to stop what is running, and only the screen can do that.
 */
export interface SmartFilterSlot {
  /** The expression currently applied, if any. Shown so it can be read without opening anything. */
  readonly source?: string | undefined;
  readonly onOpen: () => void;
  /** Removes it from the browse. */
  readonly onClear?: (() => void) | undefined;
  /** Why smart filtering cannot be used here — an absent filter engine, or a missing permission. */
  readonly unavailableReason?: string | undefined;
}

const SEEK_OPTIONS: readonly { readonly value: SeekKind; readonly label: string }[] = [
  { value: "latest", label: "Latest" },
  { value: "beginning", label: "Earliest" },
  { value: "offset", label: "Offset" },
  { value: "timestamp", label: "Timestamp" },
];

/**
 * How long the filter box waits before committing.
 *
 * Long enough that typing `orderId` is one browse rather than seven, short enough that a person who
 * has stopped typing does not wonder whether the control works. Enter commits immediately, which is
 * what somebody who knows what they are looking for will press.
 */
export const FILTER_DEBOUNCE_MS = 350;

export function MessageFilterBar(props: MessageFilterBarProps): JSX.Element {
  const kind = (): SeekKind => seekKind(props.seek);

  /* The offset and timestamp boxes hold text, not a parsed value. A control that reparsed on every
   * keystroke would erase a half-typed `1` the moment it became `1_`, and an operator pasting an
   * offset would watch it be rewritten under the cursor. They are committed on change. */
  /* `untrack`, and Solid 2 insists on it: reading `props.seek` in the component body is a reactive
   * read outside a tracking scope, which the framework's strict mode reports because such a read
   * silently never updates. Here it genuinely is a one-off — these are the *initial* contents of two
   * text boxes, which the user owns from then on. Rewriting them from the prop on every change is
   * exactly the "the field changed under my cursor" behaviour they must not have. */
  const [offsetText, setOffsetText] = createSignal(untrack(() => offsetOf(props.seek) ?? ""));
  const [timestampText, setTimestampText] = createSignal(
    untrack(() => isoLocal(timestampOf(props.seek))),
  );

  let debounce: ReturnType<typeof setTimeout> | undefined;

  /* The commit each box's debounce is still waiting to make, by box. A flush makes them now. */
  const pending = new Map<string, () => void>();
  /* While a flush runs, predicate commits are composed here and emitted once. Each box spreads the
   * predicates it was rendered with, so two boxes committed in one turn would each drop the other's
   * change — the router has not caught up between them. */
  let flushing: Predicates | undefined;

  function changePredicates(change: Partial<Predicates>): void {
    if (flushing !== undefined) {
      flushing = { ...flushing, ...change };
      return;
    }
    props.onPredicatesChange({ ...props.predicates, ...change });
  }

  function flushPending(): void {
    if (pending.size === 0) return;
    const commits = [...pending.entries()];
    const before = props.predicates;
    flushing = before;
    for (const [box, commit] of commits) if (box !== "filter") commit();
    const next = flushing;
    flushing = undefined;
    if (next !== before) props.onPredicatesChange(next);
    pending.get("filter")?.();
  }
  untrack(() => props.flushRef?.(flushPending));

  /* The upper bounds are typed text too, and committed the same debounced way as the plain filter
   * box above: `onPredicatesChange` stops a running browse and rewrites the address (see
   * `FieldPredicateControl` below for the fuller reasoning), so applying it on every digit made
   * typing a five-digit offset five stops and five address-bar rewrites. */
  const [untilOffsetText, setUntilOffsetText] = createSignal(
    untrack(() => props.predicates.untilOffset ?? ""),
  );
  let emittedUntilOffset = untrack(() => props.predicates.untilOffset ?? "");
  let untilOffsetDebounce: ReturnType<typeof setTimeout> | undefined;

  createEffect(
    () => props.predicates.untilOffset ?? "",
    (incoming) => {
      if (incoming !== emittedUntilOffset) {
        emittedUntilOffset = incoming;
        setUntilOffsetText(incoming);
      }
    },
  );

  function commitUntilOffset(digits: string): void {
    if (untilOffsetDebounce !== undefined) {
      clearTimeout(untilOffsetDebounce);
      untilOffsetDebounce = undefined;
    }
    pending.delete("untilOffset");
    emittedUntilOffset = digits;
    changePredicates({ untilOffset: digits === "" ? undefined : digits });
  }

  const [untilTimeText, setUntilTimeText] = createSignal(
    untrack(() => isoLocal(props.predicates.untilTime)),
  );
  let emittedUntilTime = untrack(() => isoLocal(props.predicates.untilTime));
  let untilTimeDebounce: ReturnType<typeof setTimeout> | undefined;

  createEffect(
    () => isoLocal(props.predicates.untilTime),
    (incoming) => {
      if (incoming !== emittedUntilTime) {
        emittedUntilTime = incoming;
        setUntilTimeText(incoming);
      }
    },
  );

  function commitUntilTime(raw: string): void {
    if (untilTimeDebounce !== undefined) {
      clearTimeout(untilTimeDebounce);
      untilTimeDebounce = undefined;
    }
    pending.delete("untilTime");
    emittedUntilTime = raw;
    changePredicates({ untilTime: epochOf(raw) });
  }

  onCleanup(() => {
    if (debounce !== undefined) clearTimeout(debounce);
    if (untilOffsetDebounce !== undefined) clearTimeout(untilOffsetDebounce);
    if (untilTimeDebounce !== undefined) clearTimeout(untilTimeDebounce);
  });

  function changeSeekKind(next: SeekKind): void {
    props.onSeekChange(seekFor(next, offsetText(), epochOf(timestampText())));
  }

  function commitFilter(value: string): void {
    if (debounce !== undefined) clearTimeout(debounce);
    pending.delete("filter");
    props.onFilterCommit(value);
  }

  /* The quick windows are measured from a clock the screen supplies, so that a story renders the
   * same window every time and a test can assert the instant a chip produces. */
  const clock = (): number => props.now ?? Date.now();

  /** The window chip that is lit, if the seek is one a chip could have set. */
  const activeWindow = (): TimeWindow | undefined => {
    const start = timestampOf(props.seek);
    return start === undefined ? undefined : windowOf(start, clock());
  };

  function chooseWindow(window: TimeWindow): void {
    /* A window is a *start*, and pressing the same chip again is not a toggle: there is no "no
     * start", and clearing one would have to mean something — earliest, or latest — that the chip
     * does not say. Re-pressing it re-reads the window from now, which is what somebody pressing
     * `5m` a second time is asking for. */
    const start = windowStart(window, clock());
    setTimestampText(isoLocal(start));
    props.onSeekChange({ kind: "timestamp", epochMillis: start });
    /* The window's *end* is dropped: the chips name a window that runs up to now, and leaving a
     * stale upper bound in place would silently exclude everything the new window is about. */
    props.onPredicatesChange({ ...props.predicates, untilTime: undefined });
  }

  return (
    <div class="kui-browse-bar">
      <div class="kui-browse-bar__row">
      <Select<SeekKind>
        label="Seek"
        /* The visible name of this control is its *prefix* ("Seek:"), which is what the design
         * draws inside the trigger. Without this the control drew "Seek" above itself and "Seek:"
         * inside itself — the same word twice, once as a stray line of text nothing was aligned
         * to. The accessible name is unchanged; only the second drawing of it goes. */
        labelHidden
        prefix="Seek:"
        size="sm"
        options={SEEK_OPTIONS}
        value={kind()}
        onChange={changeSeekKind}
      />

      {/* Choosing Offset or Timestamp reveals an extra input inline, and the bar wraps to a second
          line rather than scrolling. `<Show>` is safe around *these* — they are not the field
          somebody is typing a filter into, and their condition changes only when the operator
          changes the seek mode themselves. */}
      <Show when={kind() === "offset"}>
        <TextField
          label="Start at offset"
          /* The label is for the accessibility tree only. On screen the control is named by the
             seek control immediately to its left, which reads "Seek: Offset" — and a visible label
             stacked above this one box made it the only two-line control in a bar of 26px ones,
             which is what the design draws as a single row. */
          labelHidden
          size="sm"
          mono
          placeholder="0"
          value={offsetText()}
          onInput={(value) => {
            /* Digits only, filtered rather than rejected: an offset is a 64-bit integer carried as
               a string everywhere in this frontend, and a box that accepted `1e6` would send the
               server something it has to refuse. */
            const digits = value.replace(/\D/g, "");
            setOffsetText(digits);
            props.onSeekChange(seekFor("offset", digits, undefined));
          }}
        />
        <span class="kui-browse-bar__to" aria-hidden="true">
          &#8594;
        </span>
        {/* The other end of the range, and the reason it is a predicate rather than a parameter:
            the browse endpoint has a start and no stop. `record.offset <= n` is evaluated by the
            service as it reads, so the records past the bound are counted in the scanned figure
            instead of being quietly dropped by the browser after they arrived. */}
        <TextField
          label="Until offset"
          labelHidden
          size="sm"
          mono
          placeholder="to"
          value={untilOffsetText()}
          onInput={(value) => {
            const digits = value.replace(/\D/g, "");
            setUntilOffsetText(digits);
            if (untilOffsetDebounce !== undefined) clearTimeout(untilOffsetDebounce);
            untilOffsetDebounce = setTimeout(() => commitUntilOffset(digits), FILTER_DEBOUNCE_MS);
            pending.set("untilOffset", () => commitUntilOffset(digits));
          }}
          onKeyDown={(event) => {
            if (event.key === "Enter") commitUntilOffset(untilOffsetText());
          }}
        />
      </Show>

      <Show when={kind() === "timestamp"}>
        <label class="kui-browse-bar__stamp">
          <span class="kui-visually-hidden">Start at time</span>
          <input
            type="datetime-local"
            class="kui-browse-bar__stamp-input kui-focusable"
            value={timestampText()}
            onInput={(event) => {
              const raw = event.currentTarget.value;
              setTimestampText(raw);
              const epoch = epochOf(raw);
              if (epoch !== undefined) props.onSeekChange({ kind: "timestamp", epochMillis: epoch });
            }}
          />
        </label>
        <span class="kui-browse-bar__to" aria-hidden="true">
          &#8594;
        </span>
        <label class="kui-browse-bar__stamp">
          <span class="kui-visually-hidden">Until time</span>
          <input
            type="datetime-local"
            class="kui-browse-bar__stamp-input kui-focusable"
            value={untilTimeText()}
            onInput={(event) => {
              const raw = event.currentTarget.value;
              setUntilTimeText(raw);
              if (untilTimeDebounce !== undefined) clearTimeout(untilTimeDebounce);
              untilTimeDebounce = setTimeout(() => commitUntilTime(raw), FILTER_DEBOUNCE_MS);
              pending.set("untilTime", () => commitUntilTime(raw));
            }}
            onKeyDown={(event) => {
              if (event.key === "Enter") commitUntilTime(event.currentTarget.value);
            }}
          />
        </label>
      </Show>

      <PartitionPicker
        total={props.partitionCount}
        selected={props.partitions}
        onChange={props.onPartitionsChange}
      />

      {/* The quick windows. They set a start and nothing else, which is why one can be lit while
          LIVE is on: §3.12 is explicit that a tail with a 15m window is a forward read that begins
          fifteen minutes ago and then follows, rather than an invalid request. */}
      <FilterChipBar label="Time window">
        <For each={TIME_WINDOWS}>
          {(window) => (
            <FilterChip
              label={window.label}
              icon="clock"
              active={activeWindow() === window.value}
              onToggle={() => chooseWindow(window.value)}
            />
          )}
        </For>
      </FilterChipBar>

      <TextField
        label="Filter by key or value"
        labelHidden
        size="sm"
        icon="filter"
        placeholder="Filter by key or value…"
        value={props.filter}
        onInput={(value) => {
          props.onFilterChange(value);
          if (debounce !== undefined) clearTimeout(debounce);
          debounce = setTimeout(() => commitFilter(value), FILTER_DEBOUNCE_MS);
          pending.set("filter", () => commitFilter(value));
        }}
        onKeyDown={(event) => {
          if (event.key === "Enter") commitFilter(props.filter);
        }}
      />

      <div class="kui-browse-bar__end">
        {props.children}
        <LiveToggle
          live={props.live}
          availability={props.liveAvailability ?? { available: true }}
          onChange={props.onLiveChange}
        />
      </div>
      </div>

      {/* Row 2: what to keep. Nothing here is a query parameter — see the header. */}
      <div class="kui-browse-bar__row kui-browse-bar__row--predicates">
        <FieldPredicateControl
          field="key"
          glyph="key"
          predicate={props.predicates.key}
          onChange={(key) => changePredicates({ key })}
          onPending={(commit) =>
            commit === undefined ? pending.delete("key") : pending.set("key", commit)
          }
        />
        <FieldPredicateControl
          field="value"
          glyph="braces"
          predicate={props.predicates.value}
          onChange={(value) => changePredicates({ value })}
          onPending={(commit) =>
            commit === undefined ? pending.delete("value") : pending.set("value", commit)
          }
        />

        <Show when={props.smartFilter}>
          {(slot) => <SmartFilterControl slot={slot()} />}
        </Show>

        <Show when={props.onSavePreset}>
          {(save) => (
            <button
              type="button"
              class="kui-browse-bar__save kui-focusable"
              onClick={() => save()()}
            >
              <Icon name="plus" size="12px" />
              Save as preset
            </button>
          )}
        </Show>
      </div>

      {/* Row 3, and it is absent rather than empty when nothing is saved. */}
      <Show when={(props.presets ?? []).length > 0}>
        <div class="kui-browse-bar__row kui-browse-bar__presets">
          <span class="kui-browse-bar__presets-label">PRESETS</span>
          <FilterChipBar label="Saved filters">
            <For each={props.presets ?? []}>
              {(preset) => (
                <span class="kui-browse-bar__preset">
                  <FilterChip
                    label={preset.name}
                    icon="filter"
                    /* Never drawn as active. A preset is a *shortcut* that fills the controls in,
                       not a state the bar is in — and a chip that stayed lit after the operator
                       edited one of the boxes it filled would be claiming the browse still matches
                       the preset when it no longer does. */
                    active={false}
                    onToggle={() => props.onApplyPreset?.(preset)}
                  />
                  <Show when={props.onRemovePreset}>
                    <button
                      type="button"
                      class="kui-browse-bar__preset-remove kui-focusable"
                      aria-label={`Forget the preset ${preset.name}`}
                      title={`Forget the preset ${preset.name}`}
                      onClick={() => props.onRemovePreset?.(preset)}
                    >
                      <Icon name="close" size="10px" />
                    </button>
                  </Show>
                </span>
              )}
            </For>
          </FilterChipBar>
        </div>
      </Show>
    </div>
  );
}

/**
 * One typed predicate: which field, how it is matched, and the text.
 *
 * The mode is a real control rather than a syntax the operator has to know. `ord_*` and `ord_%` and
 * `/^ord_/` are three guesses at the same intent, and a bar that accepted one of them silently
 * treats the other two as text to look for — which finds nothing, and looks exactly like a topic
 * that has nothing in it.
 *
 * The text is held here and committed on input, for the reason the offset box gives: a value driven
 * straight from the prop is a value that can be rewritten under the cursor. The effect below
 * re-seeds it only when the change came from somewhere else — applying a preset, or opening a link —
 * which is the one case where the box genuinely should be replaced.
 */
function FieldPredicateControl(props: {
  readonly field: "key" | "value";
  readonly glyph: "key" | "braces";
  readonly predicate?: FieldPredicate | undefined;
  readonly onChange: (predicate: FieldPredicate | undefined) => void;
  /** The commit the debounce is waiting to make, or `undefined` once it is made. */
  readonly onPending: (commit: (() => void) | undefined) => void;
}): JSX.Element {
  const [text, setText] = createSignal(untrack(() => props.predicate?.text ?? ""));
  let emitted = untrack(() => props.predicate?.text ?? "");
  let debounce: ReturnType<typeof setTimeout> | undefined;
  onCleanup(() => {
    if (debounce !== undefined) clearTimeout(debounce);
  });

  createEffect(
    () => props.predicate?.text ?? "",
    (incoming) => {
      if (incoming !== emitted) {
        emitted = incoming;
        setText(incoming);
      }
    },
  );

  const mode = (): MatchMode => props.predicate?.mode ?? "contains";

  /* Committed on a timer and on blur-equivalents (Enter, or the mode changing), the same way the
     substring filter box above is debounced — never straight from `onInput`. `onChange` stops a
     running browse and rewrites the address bar (see `MessagesTab`), so calling it once a keystroke
     killed a LIVE tail and rewrote the URL for every character typed. */
  function commit(nextText: string, nextMode: MatchMode): void {
    if (debounce !== undefined) {
      clearTimeout(debounce);
      debounce = undefined;
    }
    props.onPending(undefined);
    emitted = nextText;
    /* An empty box is not a predicate that matches everything; it is no predicate. Emitting
       `undefined` is what keeps `record.keyAsText.contains("")` out of the compiled expression,
       where it would look — in the URL and in the chip — exactly like a filter that is working. */
    props.onChange(nextText === "" ? undefined : { mode: nextMode, text: nextText });
  }

  return (
    <div class="kui-browse-bar__predicate">
      <Icon name={props.glyph} size="14px" class="kui-browse-bar__predicate-glyph" />
      <Select<MatchMode>
        label={`How to match the ${props.field}`}
        labelHidden
        size="sm"
        options={MATCH_MODES}
        value={mode()}
        onChange={(next) => commit(text(), next)}
      />
      <TextField
        label={`${props.field === "key" ? "Key" : "Value"} predicate`}
        labelHidden
        size="sm"
        mono
        placeholder={props.field === "key" ? "key\u2026" : "value\u2026"}
        value={text()}
        onInput={(next) => {
          setText(next);
          if (debounce !== undefined) clearTimeout(debounce);
          debounce = setTimeout(() => commit(next, mode()), FILTER_DEBOUNCE_MS);
          props.onPending(() => commit(next, mode()));
        }}
        onKeyDown={(event) => {
          if (event.key === "Enter") commit(text(), mode());
        }}
      />
    </div>
  );
}

/**
 * The smart filter, as one control in the bar.
 *
 * Two different things depending on whether one is running, because they are two different pieces of
 * news. With no filter it is a plain button that opens the editor. With one applied it shows the
 * **expression itself**, truncated, with a clear beside it — an operator who comes back to a tab an
 * hour later has to be able to see why the list is short without opening a dialog, and a control that
 * only said "Filter ✓" would leave them to guess.
 *
 * When the cluster has no filter engine the button stays and is disabled with the server's reason.
 * Removing it would teach the operator KUI cannot filter, when in fact this cluster cannot.
 */
function SmartFilterControl(props: { readonly slot: SmartFilterSlot }): JSX.Element {
  const reason = (): string | undefined => props.slot.unavailableReason;
  const applied = (): string | undefined =>
    props.slot.source === undefined || props.slot.source === "" ? undefined : props.slot.source;

  return (
    <div class="kui-browse-bar__smart">
      <button
        type="button"
        class={[
          "kui-smart-chip",
          "kui-focusable",
          ...(applied() === undefined ? [] : ["kui-smart-chip--applied"]),
        ]}
        disabled={reason() !== undefined}
        title={reason() ?? applied() ?? "Filter with an expression evaluated on the server"}
        aria-label={
          applied() === undefined
            ? "Filter with an expression"
            : `Filtering by ${applied() ?? ""}. Edit the expression.`
        }
        onClick={() => props.slot.onOpen()}
      >
        <Icon name="filter" size="14px" />
        <Show when={applied()} fallback={<span>Expression…</span>}>
          {(source) => <code class="kui-smart-chip__source">{source()}</code>}
        </Show>
      </button>

      {/* Clearing is its own control, never a second meaning for clicking the chip. The chip opens
          the editor; a chip that sometimes removed the filter instead would be a control whose
          effect depends on state the operator cannot see. */}
      <Show when={applied() !== undefined && props.slot.onClear !== undefined}>
        <button
          type="button"
          class="kui-smart-chip__clear kui-focusable"
          aria-label="Stop filtering by this expression"
          title="Stop filtering by this expression"
          onClick={() => props.slot.onClear?.()}
        >
          <Icon name="close" size="12px" />
        </button>
      </Show>
    </div>
  );
}

/**
 * The LIVE pill: a toggle, not a light.
 *
 * On, it is the success pill with a pulsing dot and new records arrive at the top. Off, it is
 * neutral and reads PAUSED — which is a different word from LIVE rather than the same word dimmed,
 * because "dimmed green" and "green" are one distinction and it is a colour one.
 *
 * Unavailable it stays in the bar, disabled, and reads `LIVE unavailable` with the reason as its
 * title. Removing it would tell the operator the product cannot tail at all.
 */
function LiveToggle(props: {
  readonly live: boolean;
  readonly availability: LiveAvailability;
  readonly onChange: (live: boolean) => void;
}): JSX.Element {
  return (
    <Show
      when={props.availability.available}
      fallback={
        <StatusPill
          tone="neutral"
          dot
          disabled
          title={props.availability.available ? "" : props.availability.reason}
        >
          LIVE unavailable
        </StatusPill>
      }
    >
      <StatusPill
        tone={props.live ? "success" : "neutral"}
        dot
        pulsing={props.live}
        pressed={props.live}
        onClick={() => props.onChange(!props.live)}
        title={props.live ? "Stop following the end of the topic" : "Follow the end of the topic"}
      >
        {props.live ? "LIVE" : "PAUSED"}
      </StatusPill>
    </Show>
  );
}

/**
 * Which partitions to read.
 *
 * A custom listbox rather than a native `<select multiple>`, for the reason the spec gives for every
 * control in this product: the native one cannot be styled to look like the rest of the page, and a
 * control that looks like nothing else on the screen reads as broken. It is built from a real
 * checkbox per row — the kernel's, which is a visually-hidden `<input type="checkbox">` behind a
 * drawn box — so every native keyboard and form behaviour survives.
 *
 * **Empty is "all", never "none".** The server reads an absent `partition` parameter as every
 * partition, an *explicitly empty* one as a client bug, and this control cannot express the second.
 * "Clear" therefore means "select all", which is what the summary says.
 */
function PartitionPicker(props: {
  readonly total?: number | undefined;
  readonly selected: readonly number[];
  readonly onChange: (partitions: readonly number[]) => void;
}): JSX.Element {
  const [open, setOpen] = createSignal(false, { ownedWrite: true });
  const listId = createUniqueId();
  let root: HTMLDivElement | undefined;

  /**
   * Why the control cannot be used, or `undefined`.
   *
   * Two reasons and two different sentences, and neither of them hides the control: a topic with one
   * partition is a fact about the topic, and a count KUI has not been told is a fact about KUI.
   * Drawing the second as `all 0` with an empty menu — which is what a hard-coded zero produced —
   * says the topic has no partitions, which is not a thing a Kafka topic can be.
   */
  const blocked = (): string | undefined => {
    if (props.total === undefined) {
      return "KUI has not been told how many partitions this topic has, so it cannot list them.";
    }
    return props.total <= 1 ? "This topic has one partition." : undefined;
  };

  const isSelected = (partition: number): boolean =>
    props.selected.length === 0 || props.selected.includes(partition);

  function toggle(partition: number, on: boolean): void {
    const total = props.total ?? 0;
    const current = props.selected.length === 0 ? range(total) : props.selected;
    const next = on ? [...current, partition] : current.filter((p) => p !== partition);
    const distinct = [...new Set(next)].sort((a, b) => a - b);
    /* Everything selected is the same request as nothing selected, and the shorter URL is the one
       a person can read. Emitting `[]` for both is what keeps the two from being different states
       that look identical. */
    props.onChange(distinct.length === total ? [] : distinct);
  }

  const close = (event: PointerEvent): void => {
    if (!open()) return;
    if (root !== undefined && event.target instanceof Node && root.contains(event.target)) return;
    setOpen(false);
  };
  document.addEventListener("pointerdown", close, true);
  onCleanup(() => document.removeEventListener("pointerdown", close, true));

  return (
    <div class="kui-partition-picker" ref={(el: HTMLDivElement) => (root = el)}>
      <button
        type="button"
        class="kui-partition-picker__trigger kui-focusable"
        aria-expanded={open() ? "true" : "false"}
        aria-controls={open() ? listId : undefined}
        aria-haspopup="true"
        disabled={blocked() !== undefined}
        title={blocked()}
        onClick={() => setOpen((was) => !was)}
      >
        <span class="kui-partition-picker__prefix">Partitions:</span>
        <span class="kui-partition-picker__value">
          {partitionSummary(props.selected, props.total)}
        </span>
        <Icon name="chevron-down" size="14px" class="kui-partition-picker__chevron" />
      </button>

      <Show when={open()}>
        <div id={listId} class="kui-partition-picker__menu" role="group" aria-label="Partitions">
          <div class="kui-partition-picker__actions">
            <button
              type="button"
              class="kui-partition-picker__action kui-focusable"
              onClick={() => props.onChange([])}
            >
              Select all
            </button>
          </div>
          <div class="kui-partition-picker__list">
            <For each={range(props.total ?? 0)}>
              {(partition) => (
                <Checkbox
                  label={`Partition ${String(partition)}`}
                  checked={isSelected(partition)}
                  onChange={(on) => toggle(partition, on)}
                />
              )}
            </For>
          </div>
        </div>
      </Show>
    </div>
  );
}

function range(size: number): number[] {
  return Array.from({ length: Math.max(0, size) }, (_, index) => index);
}

/**
 * An epoch millisecond as `<input type="datetime-local">` spells it, in the viewer's own zone.
 *
 * The zone matters and is not a detail: a browse seeking "09:00" means nine o'clock where the
 * person typing it is sitting, and converting through UTC would silently move it by however far
 * they are from Greenwich.
 */
function isoLocal(epochMillis: number | undefined): string {
  if (epochMillis === undefined) return "";
  const at = new Date(epochMillis);
  const pad = (value: number): string => String(value).padStart(2, "0");
  return (
    `${String(at.getFullYear())}-${pad(at.getMonth() + 1)}-${pad(at.getDate())}` +
    `T${pad(at.getHours())}:${pad(at.getMinutes())}`
  );
}

function epochOf(local: string): number | undefined {
  if (local === "") return undefined;
  const at = new Date(local).getTime();
  return Number.isFinite(at) ? at : undefined;
}
