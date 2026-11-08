/**
 * The alerts card (`SCREENS-V4.md` §3.8): a bell, an open pill, and one row per event.
 *
 * ## Two layouts, one component
 *
 * §3.8 draws the same rows twice — one third of the row on the Overview tab, full width on the
 * Alerts screen with the age pushed to the far edge. That is a width and a `--wide` modifier, not a
 * second component: two components drawing one feed is two places for the sentence under an empty
 * feed to drift apart, and this product has just spent a wave repairing exactly that on the metrics
 * cards.
 *
 * ## Every state this card has, and the one that draws nothing
 *
 * `not-configured` renders **no element at all** — not an empty card, not a heading. ADR-032's rule
 * is that a deployment which has not configured a thing is not a deployment with a broken thing, and
 * an empty card headed "Alerts & events" on a deployment running no alerts service sends an operator
 * hunting for an outage that does not exist. Every other state draws the card and says what it
 * knows: the failure with its code and a retry, the refusal with no retry (there is nothing to try
 * again), and the answered-and-empty feed with a sentence naming the source.
 *
 * ## The count in the header is the service's, and never this component's arithmetic
 *
 * `props.state`'s `openCount` is the alerts service's figure over the whole feed; `events` is one
 * page of it, and the filter above the list removes rows from the page. Counting the rows would draw
 * a smaller number in the most reassuring direction, and it would be wrong on every cluster with
 * more open events than fit a page. That is this packet's owned rule; `feedTest`'s
 * "the open count on the card is the API's own figure" is the case that fails when it is broken.
 */
import { For, Show, createMemo } from "solid-js";
import type { JSX } from "@solidjs/web";
import {
  Banner,
  Button,
  Card,
  Icon,
  StatusPill,
  Tag,
  relativeAge,
  type Fetched,
} from "@kui/kernel";

import {
  acknowledgedBy,
  categoryWords,
  filterEvents,
  glyphOf,
  isOpen,
  openPill,
  pageCaption,
  severityWords,
  toneOf,
  EVERY_EVENT,
  NOT_EVALUATED,
  NO_EVENTS,
  NO_MATCHES,
  NO_OPEN_COUNT,
  type AlertEvent,
  type AlertsFeedPage,
  type FeedFilter,
} from "./model.js";

export interface AlertsFeedProps {
  readonly state: Fetched<AlertsFeedPage>;
  /** Which rows of the page are drawn. The header's count is never filtered — see the header. */
  readonly filter?: FeedFilter | undefined;
  /** Full width, with the age at the far edge (§3.8's Alerts-screen layout). */
  readonly wide?: boolean | undefined;
  /** The clock, so a story and a test can hold one still. The product passes nothing. */
  readonly now?: (() => Date) | undefined;
  /** Acknowledging one event. Absent means the control is drawn disabled with its reason. */
  readonly onAcknowledge?: ((event: AlertEvent) => void) | undefined;
  /** Why acknowledgement is unavailable to this principal or on this cluster. */
  readonly acknowledgeRefusal?: string | undefined;
  /**
   * Ids whose acknowledgement is in flight. A set, not one id: two rows can be outstanding at
   * once, and a single slot let the second click clear the first row's busy mark.
   */
  readonly acknowledging?: ReadonlySet<string> | undefined;
  /** A refused acknowledgement, said above the rows and beside nothing else. */
  readonly acknowledgeFailure?:
    | { readonly message: string; readonly code?: string | undefined }
    | undefined;
  readonly onRetry?: (() => void) | undefined;
  readonly testId?: string | undefined;
}

const TITLE = "Alerts & events";

export function AlertsFeed(props: AlertsFeedProps): JSX.Element {
  const feed = createMemo<AlertsFeedPage | undefined>(() => {
    const state = props.state;
    return state.kind === "ready" || state.kind === "stale" ? state.value : undefined;
  });
  const held = createMemo<readonly AlertEvent[]>(() => feed()?.items ?? []);
  const shown = createMemo<readonly AlertEvent[]>(() =>
    filterEvents(held(), props.filter ?? EVERY_EVENT),
  );
  const now = (): Date => props.now?.() ?? new Date();

  /* The count the service answered. Read straight off the document — the whole rule of this card. */
  const openCount = (): number | null | undefined => feed()?.openCount;
  const pill = createMemo(() => {
    const page = feed();
    // No pill over a feed with nothing in it: the sentence in the body is the answer, and a count
    // beside it is a second one. The exception is a cluster the rules have never run on, where
    // "not evaluated yet" is the most important thing on the card.
    if (page === undefined) return undefined;
    if (page.items.length === 0 && page.evaluatedAt !== undefined) return undefined;
    return openPill(page);
  });

  const cardState = () => {
    switch (props.state.kind) {
      case "loading":
        return "loading" as const;
      case "failed":
        return "unavailable" as const;
      case "forbidden":
        return "forbidden" as const;
      default:
        // An answered feed with nothing in it is `empty`; with rows that the filter removed it is
        // `filtered`, which is a different sentence and a different thing to do about it.
        return held().length === 0 ? "empty" as const
          : shown().length === 0 ? "filtered" as const
          : "ready" as const;
    }
  };

  const message = (): string | undefined => {
    switch (props.state.kind) {
      case "failed":
        return props.state.message;
      case "forbidden":
        return "You do not have permission to read this cluster's alerts.";
      default: {
        /* A page the *reader's* filter emptied and a feed the *service* is holding nothing in are
           two different facts with two different things to do about them, and only one of them is
           about the cluster. Saying `NO_EVENTS` here would tell an operator who had just clicked
           "Critical" that KUI is holding no events at all. */
        if (cardState() === "filtered") return NO_MATCHES;
        if (cardState() !== "empty") return undefined;
        /* Two empty feeds, two opposite facts. Rules that have run and opened nothing is a
           measurement; rules that have never run here is the absence of one, and drawing them the
           same way is the confident-false-statement failure this screen exists to avoid. */
        return feed()?.evaluatedAt === undefined ? NOT_EVALUATED : NO_EVENTS;
      }
    }
  };

  const caption = (): string | undefined => {
    if (cardState() !== "ready") return undefined;
    const counted = openCount() === null ? ` ${NO_OPEN_COUNT}` : "";
    return `${pageCaption(shown().length, held().length)}${counted}`;
  };

  return (
    /* Nothing at all for a deployment with no alerts service. See the header. */
    <Show when={props.state.kind !== "not-configured"}>
      <Card
        title={TITLE}
        icon="bell"
        class={props.wide === true ? "kui-alerts kui-alerts--wide" : "kui-alerts"}
        testId={props.testId ?? "alerts-feed"}
        state={cardState()}
        message={message()}
        code={props.state.kind === "failed" ? props.state.code : undefined}
        caption={caption()}
        stateAction={
          /* `failed` and nothing else. A `forbidden` read is not a read that might work next time,
             and a Retry beside it teaches the operator that this product's retries do nothing —
             the rule this file's header states and `alerts.test.tsx`' "offers no retry on a read
             this principal may not make" is the case for. */
          <Show when={props.state.kind === "failed" && props.onRetry !== undefined}>
            <Button variant="secondary" icon="refresh" onClick={() => props.onRetry?.()}>
              Retry
            </Button>
          </Show>
        }
        headerEnd={
          <Show when={pill()}>
            {(open) => (
              /* The pill is the kernel's and takes no test id, so the wrapper carries one: the
                 header's count is the figure this packet's owned case reads. */
              <span data-testid="alerts-open-count">
                <StatusPill tone={open().tone} dot>
                  {open().text}
                </StatusPill>
              </span>
            )}
          </Show>
        }
      >
        {/* Stale is a real answer with a caveat, so it sits above the rows rather than replacing
            them, and carries no code: `KUI-STALE` is not a code any service has ever sent. */}
        <Show when={props.state.kind === "stale" ? props.state.reason : undefined}>
          {(reason) => <Banner tone="warning" message={reason()} testId="alerts-stale" />}
        </Show>
        <Show when={props.acknowledgeFailure}>
          {(failure) => (
            <Banner
              tone="danger"
              message={failure().message}
              code={failure().code}
              testId="alerts-ack-failed"
            />
          )}
        </Show>
        <ul class="kui-alerts__list">
          <For each={shown()}>
            {(event) => (
              <AlertRow
                event={event}
                now={now()}
                onAcknowledge={props.onAcknowledge}
                acknowledgeRefusal={props.acknowledgeRefusal}
                busy={props.acknowledging?.has(event.id) === true}
              />
            )}
          </For>
        </ul>
      </Card>
    </Show>
  );
}

/** Why acknowledgement is unavailable, in the caller's words or in the general ones. */
function refusalReason(given: string | undefined): string {
  return given === undefined || given.trim() === ""
    ? "You do not have permission to acknowledge alerts on this cluster."
    : given;
}

interface AlertRowProps {
  readonly event: AlertEvent;
  readonly now: Date;
  readonly onAcknowledge?: ((event: AlertEvent) => void) | undefined;
  readonly acknowledgeRefusal?: string | undefined;
  readonly busy: boolean;
}

/**
 * One row: the mark, the title, the detail line that names the entity, and the age.
 *
 * The mark carries both of §3.9's fields at once — the tone from the severity, the glyph from the
 * category — and states them in words for anybody not reading the colour. That sentence is not
 * decoration: colour alone is not an accessible distinction, and "the severity is one this build
 * does not recognise" is a thing a screen reader user needs told as much as anybody.
 */
function AlertRow(props: AlertRowProps): JSX.Element {
  /*
   * "Opened" is KUI's own observation and the row says so, because it is not the cluster's: a broker
   * publishes no "this partition went offline at", so after a restart this age is an age since the
   * restart. `AlertEventDto.openedAt`'s scaladoc makes the same point, and a screen that printed it
   * as though it were the cluster's would be quietly wrong every time KUI is restarted.
   */
  const opened = (): string =>
    `KUI noticed ${relativeAge(new Date(props.event.openedAt), props.now)}`;

  /* Only an open event: a resolved one has nothing left to acknowledge, and offering the control
     would put a write in front of somebody that the service answers 409 KUI-INVALID-STATE to. */
  const acknowledgeable = (): boolean => isOpen(props.event);

  return (
    <li class="kui-alerts__row" data-testid="alert-row" data-event={props.event.id}>
      <span class="kui-alerts__mark" data-tone={toneOf(props.event)}>
        <Icon name={glyphOf(props.event)} />
        <span class="kui-visually-hidden">
          {`${severityWords(props.event)}. ${categoryWords(props.event)}.`}
        </span>
      </span>
      <div class="kui-alerts__body">
        <p class="kui-alerts__title">{props.event.title}</p>
        <Show when={props.event.detail}>
          {(detail) => <p class="kui-alerts__detail">{detail()}</p>}
        </Show>
        <div class="kui-alerts__tags">
          {/* `cleared` and `acknowledged` are different facts and are drawn as different tags: the
              first is the cluster saying the condition stopped, the second is a person saying they
              know about it, and a single "closed" tag answers "is it fixed?" with "somebody
              looked at it". */}
          <Show when={props.event.resolution}>
            {(resolution) => (
              <Tag tone={resolution().kind === "acknowledged" ? "neutral" : "success"}>
                {resolution().kind === "acknowledged"
                  ? `Acknowledged ${relativeAge(new Date(resolution().at), props.now)}`
                  : `Cleared ${relativeAge(new Date(resolution().at), props.now)}`}
              </Tag>
            )}
          </Show>
          <Show when={acknowledgedBy(props.event)}>
            {(who) => <Tag tone="neutral">{`By ${who()}`}</Tag>}
          </Show>
        </div>
      </div>
      <div class="kui-alerts__end">
        <span class="kui-alerts__age">{opened()}</span>
        <Show when={acknowledgeable()}>
          <Show
            when={props.onAcknowledge}
            fallback={
              <Button
                size="sm"
                variant="secondary"
                icon="check"
                disabled
                /* A disabled control that does not say why is a dead end: the operator cannot tell
                   "you may not do this" from "this build is broken". `??` alone would let a caller
                   pass an empty string and produce exactly that, so a blank reason falls back too —
                   there is no arrangement of props that draws this button without a sentence. */
                disabledReason={refusalReason(props.acknowledgeRefusal)}
              >
                Acknowledge
              </Button>
            }
          >
            {(acknowledge) => (
              <Button
                size="sm"
                variant="secondary"
                icon="check"
                busy={props.busy}
                onClick={() => acknowledge()(props.event)}
              >
                Acknowledge
              </Button>
            )}
          </Show>
        </Show>
      </div>
    </li>
  );
}
