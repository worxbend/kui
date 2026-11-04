import { Show, createSignal, onCleanup } from "solid-js";
import { Icon } from "@kui/kernel";
import { AppearancePopover, type AppearancePreferences } from "./AppearancePopover.js";
import { Breadcrumb } from "./Breadcrumb.js";
import { dismissOnEscapeOrPressElsewhere } from "./dismissal.js";
import { NotificationBell, NotificationPanel, type NoticeFeed } from "./Notifications.js";
import { SearchField, type SearchFieldProps } from "./SearchField.js";
import type { Crumb } from "./types.js";

/**
 * The band across the top of the content column: where you are, what you are looking for, and the
 * controls that are not about any one page.
 *
 * ## It is a band, not a bar
 *
 * It has no fill. Scanning column 1000 of `13-topics-list.png` from the top returns the page ground
 * the whole way down (`SCREENS.md` §1.1): what used to be a 58px bar with its own surface is now
 * 58px of page with three things floating in it — the breadcrumb, the search pill, and the control
 * cluster. The height is unchanged, so nothing below it moves.
 *
 * ## What left, and where it went
 *
 * The cluster selector is gone from here. It is the environment rail now, and for a reason worth
 * stating: a dropdown answers "which cluster am I on?" only when it is asked, and that is the one
 * question an operator should never have to ask. The account avatar moved to the rail's foot with
 * it, so that identity and environment — the two things that decide what a destructive action will
 * actually destroy — sit together.
 *
 * ## Three theme states, not two
 *
 * The theme control cycles auto → light → dark and shows which one is in force. Two states would be
 * simpler and would be wrong: an operator on "follow the system" whose laptop turns dark at sunset
 * expects KUI to turn with it, and one who explicitly chose light expects light at midnight. A
 * toggle cannot express the difference between "light" and "light because everything else is", and
 * the difference is the whole point. The accessible name says the mode in words — "Theme: follows
 * system" — because the glyph alone cannot distinguish "currently light" from "auto, and it is
 * daytime". `SCREENS-V4.md` §7.4 asks whether the glyph should instead name the theme it will move
 * *to*; it cannot, because a cycle of three has no single target, and that is the half of the
 * finding settled here. The other half — a two-segment control over a three-valued preference — is
 * settled in `AppearancePopover`.
 *
 * ## The appearance popover is this component's own
 *
 * The notifications panel's openness belongs to the caller, because what opening it *means* is a
 * product decision with a real trade-off (does it mark everything read?) and because its contents
 * are fetched and can fail. Neither is true here: the popover reads and writes three browser
 * preferences, has no request behind it and no consequence beyond itself, so a caller that had to
 * hold a boolean for it would be holding it for nothing. It closes on Escape and on a click
 * outside, and `onOpenAppearance` still fires for a caller that wants to know.
 *
 * ## The bell counts, and says so
 *
 * A dot with no number is a colour-only signal. The count goes in the accessible name
 * ("Notifications, 3 unread"), and when there is nothing unread there is no dot at all rather than a
 * grey one — a permanently present marker is a marker nobody looks at.
 *
 * With an alerts feed behind it the figure is the *open count* and the read marker is the badge's
 * tone; both arrive as props and neither is computed here. The band holds no store and folds no
 * feed, which is what keeps the bell and the alerts card reading one number: the store is the
 * kernel's, the card is in a feature package, and a component that recounted either would be the
 * second answer to a question that has one.
 */
export type ThemeMode = "auto" | "light" | "dark";

export type TopBarProps = {
  /**
   * Where you are, beginning with the cluster: `prod-kyiv-01 › Topics › analytics.clickstream`.
   *
   * This is the *installation* trail. Object pages keep a second, shorter breadcrumb in the content
   * column, and the two are not redundant: this one says where you are in the deployment and stays
   * put, that one says where you are in the section and scrolls away.
   */
  readonly crumbs?: readonly Crumb[] | undefined;
  readonly search: SearchFieldProps;
  /**
   * Which theme mode the glyph names, for a caller that holds it itself.
   *
   * Ignored when `appearance` is supplied: the preference is then the single source, and a second
   * one passed alongside it could only ever disagree. `auto` when neither is given.
   */
  readonly theme?: ThemeMode | undefined;
  /**
   * The three preferences the appearance popover writes — the kernel's own singletons, or a
   * stand-in built by `createRootPreference` in a test.
   *
   * Absent leaves both controls inert rather than drawing controls that do nothing: without it the
   * theme button falls back to `onCycleTheme` and the sliders button to `onOpenAppearance`, which
   * is what a caller driving the preferences itself would pass.
   */
  readonly appearance?: AppearancePreferences | undefined;
  readonly onCycleTheme?: (() => void) | undefined;
  readonly onOpenAppearance?: (() => void) | undefined;
  /** Unread notifications. Zero means no marker at all. */
  readonly unreadCount?: number | undefined;
  /**
   * How many alert events are open, as the alerts service counted them, or `null` when nothing
   * has said. Absent when the deployment has no alerts feed, which leaves {@link unreadCount} in
   * charge — see `NotificationBell`, where all three renderings are argued.
   */
  readonly alertsOpen?: number | null | undefined;
  /** Whether this principal has anything unread, as the service counted it. The badge's tone. */
  readonly alertsUnread?: boolean | undefined;
  /** Whether the notifications panel is showing. Owned by the caller, so that something besides the
   * bell can close it: this bar asks through {@link onCloseNotifications} on Escape, on a press
   * elsewhere and when a link in the panel is followed. */
  readonly notificationsOpen?: boolean | undefined;
  readonly onToggleNotifications?: (() => void) | undefined;
  readonly onCloseNotifications?: (() => void) | undefined;
  /** What the panel shows. Absent means it has not been asked for yet. */
  readonly notifications?: NoticeFeed | undefined;
  readonly onMarkAllRead?: (() => void) | undefined;
  readonly onRetryNotifications?: (() => void) | undefined;
};

const THEME_LABEL: Record<ThemeMode, string> = {
  auto: "Theme: follows system. Change theme",
  light: "Theme: light. Change theme",
  dark: "Theme: dark. Change theme",
};

const THEME_ICON = { auto: "theme-auto", light: "sun", dark: "moon" } as const;

/** auto → light → dark → auto. The order the settings page lists them in, so the two agree. */
const THEME_CYCLE: readonly ThemeMode[] = ["auto", "light", "dark"];

export function TopBar(props: TopBarProps) {
  const unread = () => props.unreadCount ?? 0;

  /* The preference wins when there is one, because it is the thing the stylesheet reads. A `theme`
     prop passed beside it could only be a second, staler copy of the same fact. */
  const mode = (): ThemeMode => props.appearance?.theme.choice() ?? props.theme ?? "auto";

  const cycleTheme = () => {
    if (props.onCycleTheme !== undefined) {
      props.onCycleTheme();
      return;
    }
    const preference = props.appearance?.theme;
    if (preference === undefined) return;
    const next = THEME_CYCLE[(THEME_CYCLE.indexOf(preference.choice()) + 1) % THEME_CYCLE.length];
    if (next !== undefined) preference.select(next);
  };

  const [appearanceOpen, setAppearanceOpen] = createSignal(false);
  let appearanceAnchor: HTMLDivElement | undefined;

  /* `mousedown` and not `click`: a click that begins inside the popover and ends outside it — a
     drag that overshoots a segment — is not a click elsewhere, and closing on it would take the
     control away mid-gesture. Registered once for the life of the bar rather than added and removed
     with the popover, because a listener added during the click that opened it sees that same click
     on its way back up and closes it again immediately. */
  const closeOnOutsideClick = (event: MouseEvent) => {
    if (!appearanceOpen()) return;
    const target = event.target;
    if (target instanceof Node && appearanceAnchor?.contains(target) === true) return;
    setAppearanceOpen(false);
  };
  document.addEventListener("mousedown", closeOnOutsideClick);
  onCleanup(() => document.removeEventListener("mousedown", closeOnOutsideClick));

  let bellAnchor: HTMLDivElement | undefined;
  dismissOnEscapeOrPressElsewhere({
    anchor: () => bellAnchor,
    open: () => props.notificationsOpen === true,
    close: () => props.onCloseNotifications?.(),
    returnFocus: () => bellAnchor?.querySelector<HTMLElement>('[data-testid="notifications"]'),
  });

  const toggleAppearance = () => {
    props.onOpenAppearance?.();
    if (props.appearance !== undefined) setAppearanceOpen(!appearanceOpen());
  };

  return (
    <header class="kui-topbar" data-testid="topbar">
      {/* The trail sits at the band's left edge and takes the slack, so the search pill and the
          controls stay hard against the right however long the trail is. */}
      <div class="kui-topbar__where">
        <Show when={props.crumbs !== undefined && props.crumbs.length > 0}>
          <Breadcrumb trail={props.crumbs ?? []} />
        </Show>
      </div>

      <div class="kui-topbar__actions">
        <SearchField {...props.search} />

        <button
          type="button"
          class="kui-topbar__icon-button"
          aria-label={THEME_LABEL[mode()]}
          onClick={cycleTheme}
          data-testid="theme-control"
        >
          <Icon name={THEME_ICON[mode()]} size="18px" />
        </button>

        {/* Anchored rather than portalled, so it stays under the glyph when the window is resized
            and so Tab moves from the glyph straight into it. Escape is caught on the anchor rather
            than on the panel, so it works while focus is still on the button that opened it. */}
        <div
          class="kui-topbar__appearance-anchor"
          ref={(element) => (appearanceAnchor = element)}
          onKeyDown={(event) => {
            if (event.key !== "Escape" || !appearanceOpen()) return;
            setAppearanceOpen(false);
            /* Focus goes back to the glyph. Escape that left focus on a removed element drops the
               keyboard user at the top of the document. */
            const glyph = '[data-testid="appearance-control"]';
            event.currentTarget.querySelector<HTMLButtonElement>(glyph)?.focus();
          }}
        >
          <button
            type="button"
            class={[
              "kui-topbar__icon-button",
              { "kui-topbar__icon-button--open": appearanceOpen() },
            ]}
            aria-label="Appearance: accent colour, theme and density"
            aria-expanded={
              props.appearance === undefined ? undefined : appearanceOpen() ? "true" : "false"
            }
            aria-haspopup={props.appearance === undefined ? undefined : "dialog"}
            onClick={toggleAppearance}
            data-testid="appearance-control"
          >
            <Icon name="sliders" size="18px" />
          </button>
          <Show when={appearanceOpen() ? props.appearance : undefined}>
            {(preferences) => (
              <div class="kui-topbar__appearance-panel">
                <AppearancePopover
                  preferences={preferences()}
                  onClose={() => setAppearanceOpen(false)}
                />
              </div>
            )}
          </Show>
        </div>

        {/* The panel is anchored to the bell rather than portalled, so that it stays under it when
            the window is resized and so that Tab moves from the bell straight into it. */}
        <div class="kui-topbar__bell-anchor" ref={(element) => (bellAnchor = element)}>
          <NotificationBell
            unreadCount={unread()}
            /* Passed through untouched, `null` included. Collapsing a `null` to a `0` here would
               make the band claim that nothing is open on behalf of a service that has not
               answered — the bell is the one control on this screen that is read as a statement
               about the cluster rather than about the page. */
            openCount={props.alertsOpen}
            unread={props.alertsUnread}
            open={props.notificationsOpen === true}
            onToggle={() => props.onToggleNotifications?.()}
          />
          <Show when={props.notificationsOpen === true}>
            {/* A followed link leaves the panel behind it, so following one closes it. Watched here
                rather than through `onOpenNotice`, which would turn a notice with no address into
                a link to nowhere. */}
            <div
              class="kui-topbar__bell-panel"
              onClick={(event) => {
                if (event.target instanceof Element && event.target.closest("a[href]") !== null) {
                  props.onCloseNotifications?.();
                }
              }}
            >
              <NotificationPanel
                feed={props.notifications ?? { kind: "loading" }}
                onMarkAllRead={props.onMarkAllRead}
                onRetry={props.onRetryNotifications}
              />
            </div>
          </Show>
        </div>
      </div>
    </header>
  );
}
