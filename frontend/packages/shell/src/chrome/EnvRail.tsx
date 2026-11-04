/**
 * The 48px column to the left of the drawer: the product mark, one tile per environment, a stack of
 * shortcut glyphs, and the account.
 *
 * ## What it is, and what it is not
 *
 * It is not a second drawer. It has no fill of its own — it is drawn straight on the page ground,
 * which is why it has no edge (`research/design/SCREENS.md` §1.1, sampled at row 700 of
 * `13-topics-list.png`: the first 48 pixels are `--kui-color-surface`, and the drawer's
 * `--kui-color-surface-raised` only starts at x=48).
 *
 * It exists because switching environment used to be a dropdown in the top bar, and a dropdown
 * hides the one fact an operator most needs to have in peripheral vision: **which cluster am I
 * about to break?** A column of always-visible tiles answers that without being asked, and it makes
 * "production" and "staging" two different places on the screen rather than two lines of the same
 * menu.
 *
 * ## The letter is not an identifier
 *
 * A tile shows the environment's initial. Two environments beginning with the same letter therefore
 * produce two identical tiles — `prod-kyiv-01` and `prod-eu-02` are both `P` — and the design has no
 * answer for that yet (`SCREENS.md` §6, open finding 1).
 *
 * Until it does, the rule this component enforces is: **the rail is never the only place an
 * environment is named.** Every tile has a tooltip carrying the full name, the tooltip is not
 * optional, and the drawer's head below states the current environment in words. If you are tempted
 * to remove the tooltip because the tiles "look cleaner", read this paragraph again — the tiles are
 * ambiguous by construction and the tooltip is what makes them safe.
 *
 * ## Health is on the tile and in words elsewhere
 *
 * The dot on a tile's corner mirrors that environment's health, and it is decorative: colour is
 * never the only signal, so the authoritative statement is the drawer's head and the tooltip, both
 * of which say it in words. The dot's fourth state matters as much as the other three — an
 * environment whose health is not yet known takes `--kui-color-text-subtle`, which is not any of
 * the health colours, so "we have not asked" can never be read as "it is fine".
 *
 * The words themselves come from `types.ts` and are shared with the drawer's head, so a rename
 * changes both screens or neither. Two tables would each read correctly on their own, which is what
 * makes that kind of drift so hard to notice.
 */
import { For, Show } from "solid-js";
import type { JSX } from "@solidjs/web";
import { Avatar, Icon, Tooltip, type IconName } from "@kui/kernel";
import { dismissOnEscapeOrPressElsewhere } from "./dismissal.js";
import { healthWord, type ClusterSummary } from "./types.js";

/**
 * One of the glyph shortcuts at the rail's foot.
 *
 * These are destinations, not actions: each one goes somewhere. A shortcut whose capability is
 * absent is not drawn at all — a rail is a set of shortcuts, and a dead shortcut is worse than a
 * missing one, because the operator spends their attention discovering it does nothing.
 */
export type RailDestination = {
  readonly id: string;
  /** Read out by a screen reader and shown as the tooltip. The glyph alone names nothing. */
  readonly label: string;
  readonly icon: IconName;
  readonly href: string;
  /** Pins this one to the foot, below the flexible space. The design puts settings there. */
  readonly atFoot?: boolean | undefined;
};

export type EnvRailProps = {
  readonly environments: readonly ClusterSummary[];
  readonly currentId?: string | undefined;
  /**
   * The environment-switch *request*: the user pressed a tile, and would like to be on that
   * cluster.
   *
   * A request and not the switch itself, and emphatically not a place to raise a toast. Switching
   * environments changes the selection, the drawer's counts, the address and — this is the part
   * that matters — what every destructive control on the page is about to act on, and only the
   * frame knows when all of that has actually happened. A rail that announced "switched to
   * staging-fra" the instant it was clicked would be announcing its own intention; if the selection
   * were then refused, the toast would already be on screen saying otherwise.
   *
   * So the confirmation is raised by `App.tsx`, on the single `ToastRegion` it already mounts. The
   * rail reports the press and draws the new current tile when the caller tells it to.
   */
  readonly onSelect?: ((id: string) => void) | undefined;
  readonly destinations?: readonly RailDestination[] | undefined;
  /** Which shortcut's page is being shown, if any. */
  readonly currentDestinationId?: string | undefined;
  readonly onAdd?: (() => void) | undefined;
  readonly accountName?: string | undefined;
  /**
   * Absent means the avatar is a picture rather than a button.
   *
   * That is the honest rendering for a deployment with authentication disabled: there is an
   * anonymous principal, there is nothing to open, and a control that opens an empty panel teaches
   * the operator that the rail's controls do nothing.
   */
  readonly onOpenAccount?: (() => void) | undefined;
  /**
   * What the avatar reveals, and whether it is revealed.
   *
   * The panel is passed in rather than built here for the same reason the drawer takes its groups:
   * the rail knows nothing about sessions, so every state of it — signed in, signing out, a
   * sign-out the gateway refused — is reachable from a story with no server. Openness is the
   * caller's too, so that something besides the avatar can close it: the rail asks through
   * {@link onCloseAccount} on Escape and on a press elsewhere.
   */
  readonly accountOpen?: boolean | undefined;
  readonly onCloseAccount?: (() => void) | undefined;
  readonly accountPanel?: JSX.Element | undefined;
  /** Where the product mark links to. */
  readonly homeHref?: string | undefined;
};

/**
 * The tile's letter.
 *
 * The first character of the name, uppercased — but taken with the spread operator rather than with
 * `name[0]`, because a name beginning with an emoji or an astral-plane character would otherwise be
 * cut in half and render as a replacement glyph. Environment names come from configuration files
 * that people write, so this is not hypothetical.
 */
export function tileLetter(name: string): string {
  const trimmed = name.trim();
  if (trimmed.length === 0) return "?";
  return ([...trimmed][0] ?? "?").toUpperCase();
}

export function EnvRail(props: EnvRailProps) {
  const destinations = () => props.destinations ?? [];
  const body = () => destinations().filter((destination) => destination.atFoot !== true);
  const foot = () => destinations().filter((destination) => destination.atFoot === true);

  let accountAnchor: HTMLDivElement | undefined;
  dismissOnEscapeOrPressElsewhere({
    anchor: () => accountAnchor,
    open: () => props.accountOpen === true && props.accountPanel !== undefined,
    close: () => props.onCloseAccount?.(),
    returnFocus: () => accountAnchor?.querySelector<HTMLElement>(":scope > button"),
  });

  return (
    <nav class="kui-rail" aria-label="Environment navigation" data-testid="env-rail">
      <a class="kui-rail__mark kui-focusable" href={props.homeHref ?? "#"} aria-label="KUI — cluster overview">
        <Icon name="topology" size="22px" />
      </a>

      {/* The caption is drawn (SCREENS.md §2.1) but it is decoration: the list below carries its own
          accessible name, and a heading here would put a second, emptier one in the document. */}
      <p class="kui-rail__caption" aria-hidden="true">
        ENVS
      </p>

      <ul class="kui-rail__envs" aria-label="Environments">
        <For each={props.environments}>
          {(environment) => {
            const current = () => environment.id === props.currentId;
            // The full name and the health, in words. This is the accessible name *and* the
            // tooltip, so the sighted user and the screen-reader user get the same sentence — see
            // the note above about the letter not being an identifier.
            const description = () => `${environment.name} — ${healthWord(environment.health)}`;
            return (
              <li>
                <Tooltip content={description()}>
                  <button
                    type="button"
                    class={["kui-rail__env", "kui-focusable", { "kui-rail__env--current": current() }]}
                    aria-label={description()}
                    aria-current={current() ? "true" : undefined}
                    data-testid={`env-tile-${environment.id}`}
                    onClick={() => props.onSelect?.(environment.id)}
                  >
                    <span class="kui-rail__letter" aria-hidden="true">
                      {tileLetter(environment.name)}
                    </span>
                    <span
                      class={`kui-rail__dot kui-rail__dot--${environment.health}`}
                      aria-hidden="true"
                    />
                  </button>
                </Tooltip>
              </li>
            );
          }}
        </For>

        <Show when={props.onAdd !== undefined}>
          <li>
            <Tooltip content="Add a cluster">
              <button
                type="button"
                class="kui-rail__add kui-focusable"
                aria-label="Add a cluster"
                data-testid="env-add"
                onClick={() => props.onAdd?.()}
              >
                <Icon name="plus" size="16px" />
              </button>
            </Tooltip>
          </li>
        </Show>
      </ul>

      <Show when={body().length > 0}>
        <ul class="kui-rail__links" aria-label="Shortcuts">
          <For each={body()}>{(destination) => <RailLink destination={destination} currentId={props.currentDestinationId} />}</For>
        </ul>
      </Show>

      {/* Everything below this is pinned to the bottom. */}
      <div class="kui-rail__spacer" />

      <Show when={foot().length > 0}>
        <ul class="kui-rail__links" aria-label="Settings">
          <For each={foot()}>{(destination) => <RailLink destination={destination} currentId={props.currentDestinationId} />}</For>
        </ul>
      </Show>

      <div class="kui-rail__account" ref={(element) => (accountAnchor = element)}>
        {/* The kernel's Avatar owns the rule that an unknown identity is a neutral person glyph
            rather than guessed initials, and that rule has to live in exactly one place. */}
        <Avatar name={props.accountName} onClick={props.onOpenAccount} />
        {/* Anchored to the avatar rather than portalled, so it stays with it when the window is
            resized and so Tab moves from the avatar straight into it. */}
        <Show when={props.accountOpen === true && props.accountPanel !== undefined}>
          <div class="kui-rail__account-panel">{props.accountPanel}</div>
        </Show>
      </div>
    </nav>
  );
}

function RailLink(props: { readonly destination: RailDestination; readonly currentId: string | undefined }) {
  const current = () => props.destination.id === props.currentId;
  return (
    <li>
      <Tooltip content={props.destination.label}>
        <a
          class={["kui-rail__link", "kui-focusable", { "kui-rail__link--current": current() }]}
          href={props.destination.href}
          aria-label={props.destination.label}
          aria-current={current() ? "page" : undefined}
          data-testid={`rail-link-${props.destination.id}`}
        >
          <Icon name={props.destination.icon} size="20px" />
        </a>
      </Tooltip>
    </li>
  );
}
