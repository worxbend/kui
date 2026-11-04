/**
 * Rendering, interaction and accessibility for the application chrome.
 *
 * Every case here is attached to a statement made somewhere in `.agent/design/SPEC.md` or to a
 * defect this project has already paid for. Nothing asserts a colour or a pixel: those are judged in
 * Storybook against the design screenshots, because a test that asserted them in jsdom would be
 * asserting numbers jsdom invented.
 */

import { createSignal, flush } from "solid-js";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { AccountMenu } from "./AccountMenu.jsx";
import { AppearancePopover, type AppearancePreferences } from "./AppearancePopover.jsx";
import { ACCENT_OPTIONS, DENSITY_OPTIONS, THEME_OPTIONS, appearanceHelp } from "./appearance.js";
import { BrandBlock } from "./BrandBlock.jsx";
import { Breadcrumb } from "./Breadcrumb.jsx";
import { ClusterStatusCard } from "./ClusterStatusCard.jsx";
import { EnvRail, tileLetter } from "./EnvRail.jsx";
import { NavDrawer } from "./NavDrawer.jsx";
import { NavItem } from "./NavItem.jsx";
import { NotificationPanel, type Notice } from "./Notifications.jsx";
import { SearchField } from "./SearchField.jsx";
import { StorageMeter, brokerState } from "./StorageMeter.jsx";
import { TabStrip, createRootPreference } from "@kui/kernel";
import type { AccentChoice, DensityChoice, ThemeChoice } from "@kui/kernel";
import { TopBar } from "./TopBar.jsx";
import { RESULT_CLICK_GRACE_MS, shortcutHint } from "./SearchField.jsx";
import {
  CLUSTERS,
  DEFECTIVE_CLUSTER,
  HEALTHY_CLUSTER,
  LONG_TOPIC,
  NAV_GROUPS,
  NAV_GROUPS_EMPTY_ECOSYSTEM,
  NAV_GROUPS_WITH_TREE,
  TOPIC_TABS,
  UNCOUNTED_CLUSTER,
  UNREACHABLE_CLUSTER,
  VERSIONLESS_CLUSTER,
} from "./fixtures.js";
import { describeViolations, findViolations, mount } from "./testing.js";

describe("NavItem", () => {
  it("puts the badge's meaning in the accessible name, not the fragment it shows", () => {
    const { container, dispose } = mount(() => (
      <NavItem
        destination={{
          id: "brokers",
          label: "Brokers",
          icon: "brokers",
          href: "/brokers",
          badge: { text: "3/3", tone: "success", description: "3 of 3 online" },
        }}
      />
    ));
    const link = container.querySelector("a")!;
    expect(link.getAttribute("aria-label")).toBe("Brokers, 3 of 3 online");
    // The visible fragment is hidden from assistive technology, so the number is announced once.
    expect(container.querySelector(".kui-nav-item__badge")!.getAttribute("aria-hidden")).toBe("true");
    dispose();
  });

  it("marks the current destination with aria-current so it is not only a fill", () => {
    const { container, dispose } = mount(() => (
      <NavItem
        destination={{ id: "dashboard", label: "Dashboard", icon: "dashboard", href: "/d" }}
        current={true}
      />
    ));
    expect(container.querySelector("a")!.getAttribute("aria-current")).toBe("page");
    dispose();
  });

  it("renders a disabled destination as present, explained and out of the tab order", () => {
    const { container, dispose } = mount(() => (
      <NavItem
        destination={{
          id: "ksql",
          label: "ksqlDB",
          icon: "ksql",
          href: "/ksql",
          disabled: true,
          disabledReason: "You do not have permission to run ksqlDB statements on this cluster",
        }}
      />
    ));
    // Not an anchor: there is nowhere to go, so there is no link to follow.
    expect(container.querySelector("a")).toBeNull();
    const row = container.querySelector('[data-testid="nav-ksql"]')!;
    expect(row.getAttribute("aria-disabled")).toBe("true");
    expect(row.getAttribute("title")).toBe(
      "You do not have permission to run ksqlDB statements on this cluster",
    );
    expect(row.getAttribute("aria-label")).toBe(
      "ksqlDB, You do not have permission to run ksqlDB statements on this cluster",
    );
    dispose();
  });

  it("omits the badge entirely when the count is unavailable rather than printing a zero", () => {
    const { container, dispose } = mount(() => (
      <NavItem destination={{ id: "topics", label: "Topics", icon: "topics", href: "/t" }} />
    ));
    expect(container.querySelector(".kui-nav-item__badge")).toBeNull();
    expect(container.textContent).not.toContain("0");
    dispose();
  });

  /**
   * The branch predicate, which two comments claimed and no case made.
   *
   * `NavDestination.children` says absent and empty draw the same thing and that this is the
   * renderer's decision; `App.tsx`'s memo said the same thing back, naming this component as "the
   * renderer, which defends itself". Between the two, `children().length > 0` could become
   * `props.destination.children !== undefined` with every case in the package green — a cluster
   * with no topics drawing an expandable Topics row over an empty subtree. Nothing in this file
   * had ever handed a destination an empty array.
   */
  it("draws no disclosure for a destination with an empty children array", () => {
    const { container, dispose } = mount(() => (
      <NavItem
        destination={{
          id: "topics",
          label: "Topics",
          icon: "topics",
          href: "/t",
          children: [],
          /* Seeded open as well, so the case fails on both halves of the predicate rather than
             only on the chevron: a branch that is expanded and holds nothing draws an empty `<ul>`
             beneath the row, which is the same control appearing broken one line down. */
          expanded: true,
        }}
      />
    ));
    // A chevron that opens onto nothing is a control that appears broken.
    expect(container.querySelector('[data-testid="nav-topics-disclosure"]')).toBeNull();
    expect(container.querySelector('[data-testid="nav-topics-subtree"]')).toBeNull();
    // And the row itself is still a link to the list, which is what a leaf Topics row is.
    expect(container.querySelector('a[data-testid="nav-topics"]')).not.toBeNull();
    dispose();
  });
});


describe("BrandBlock", () => {
  /* `SCREENS-V4.md` §2.1. The three parts of the caption are three independent figures, and the
   * rule the whole component exists for is that a part nobody supplied is dropped rather than
   * filled in — every one of the cases below is a way of getting that wrong that reads as a
   * rendering fault or, worse, as a fact. */

  it("writes the three-part caption with one separator between each part", () => {
    const { container, dispose } = mount(() => <BrandBlock cluster={DEFECTIVE_CLUSTER} />);
    expect(container.querySelector(".kui-brand__caption")!.textContent).toBe("1 URP · v3.7.0 · 3 brokers");
    dispose();
  });

  it("takes the health word as the first token when there is no defect to report", () => {
    const { container, dispose } = mount(() => <BrandBlock cluster={HEALTHY_CLUSTER} />);
    // A green dot beside the word "healthy" says one thing twice; "1 URP" above says what the dot
    // cannot. The token is variable in kind, which is the whole reason the caption exists.
    expect(container.querySelector(".kui-brand__caption")!.textContent).toBe("healthy · v3.7.0 · 3 brokers");
    dispose();
  });

  it("says nothing about brokers when nobody has counted them", () => {
    const { container, dispose } = mount(() => <BrandBlock cluster={UNCOUNTED_CLUSTER} />);
    // Not `— brokers`, which reads as a missing dash rather than as a missing figure, and not
    // `0 brokers`, which is an assertion about the cluster nobody made.
    expect(container.textContent).not.toContain("brokers");
    expect(container.textContent).not.toContain("—");
    expect(container.querySelector(".kui-brand__name")!.textContent).toBe("prod-kyiv-01");
    expect(container.querySelector(".kui-brand__dot")).not.toBeNull();
    dispose();
  });

  it("drops the version alone rather than leaving an empty slot for it", () => {
    const { container, dispose } = mount(() => <BrandBlock cluster={VERSIONLESS_CLUSTER} />);
    expect(container.querySelector(".kui-brand__caption")!.textContent).toBe("healthy");
    dispose();
  });

  it("draws no caption at all before the first scrape has answered", () => {
    const { container, dispose } = mount(() => (
      <BrandBlock cluster={{ id: "c", name: "prod-kyiv-01", health: "unknown" }} />
    ));
    expect(container.querySelector(".kui-brand__caption")).toBeNull();
    expect(container.querySelector(".kui-brand__dot--unknown")).not.toBeNull();
    dispose();
  });

  it("takes the registration link from the caller and omits it when there is none", () => {
    // Never a literal: this is the only route to cluster registration in the product, and a
    // hand-written address goes on compiling after a route segment is renamed.
    const withHref = mount(() => <BrandBlock cluster={HEALTHY_CLUSTER} manageHref="/ui/clusters/manage" />);
    const add = withHref.container.querySelector('[data-testid="brand-add-cluster"]') as HTMLAnchorElement;
    expect(add.getAttribute("href")).toBe("/ui/clusters/manage");
    expect(add.textContent).toContain("Add a cluster");
    withHref.dispose();

    const without = mount(() => <BrandBlock cluster={HEALTHY_CLUSTER} />);
    expect(without.container.querySelector('[data-testid="brand-add-cluster"]')).toBeNull();
    without.dispose();
  });

  it("says there is no cluster rather than drawing an empty head", () => {
    const { container, dispose } = mount(() => <BrandBlock manageHref="/ui/clusters/manage" />);
    expect(container.querySelector(".kui-brand__name")!.textContent).toBe("no cluster");
    dispose();
  });

  it("has no accessibility violations", async () => {
    const { container, dispose } = mount(() => (
      <BrandBlock cluster={DEFECTIVE_CLUSTER} manageHref="/ui/clusters/manage" />
    ));
    expect(describeViolations(await findViolations(container))).toBe("");
    dispose();
  });
});

describe("StorageMeter", () => {
  it("draws its 'not known' rendering for an empty list, and no zeroed bars", () => {
    // The rendering it has drawn in every deployment since it was built, because until this wave
    // nothing had ever handed it data. An empty bar reads as 0% — "your disks are empty" — which is
    // both wrong and the most comforting available misreading.
    const { container, dispose } = mount(() => <StorageMeter brokers={[]} />);
    expect(container.textContent).toContain("Disk usage could not be read for this cluster.");
    expect(container.querySelector(".kui-storage__percent--unknown")!.textContent).toBe("—");
    expect(container.textContent).not.toContain("0%");
    dispose();
  });

  /**
   * A broker whose capacity is zero, drawn where the meter actually decides its colour.
   *
   * `brokerState`'s guard is `<= 0`, and `< 0` left green every case the acceptance list of this
   * packet runs — the six relevant paths under `packages/shell/src`. The
   * arithmetic is the whole finding: `0 / 0` is `NaN`, `NaN >= dangerAt` and `NaN >= warnAt` are
   * both false, so the fall-through paints a **healthy green segment** for a disk about which
   * nothing at all is known — an unconfigured log directory reported as capacity in hand. The
   * assertion is on the rendered segment rather than on the exported function alone, because the
   * segment is what an operator sees and `segments()` is the line that calls the rule.
   */
  it("draws a broker whose capacity is zero as unknown and never as healthy", () => {
    const { container, dispose } = mount(() => (
      <StorageMeter
        brokers={[
          { id: "broker-1", usedBytes: 50, totalBytes: 200 },
          { id: "broker-2", usedBytes: 0, totalBytes: 0 },
        ]}
      />
    ));
    const drawn = [...container.querySelectorAll(".kui-segbar__seg")].map((segment) => ({
      title: segment.getAttribute("title"),
      classes: segment.className,
    }));
    const zeroCapacity = drawn.find((segment) => segment.title?.startsWith("broker-2"));
    expect(zeroCapacity?.title).toBe("broker-2 · capacity not known");
    expect(zeroCapacity?.classes).toContain("kui-segbar__seg--idle");
    expect(zeroCapacity?.classes).not.toContain("kui-segbar__seg--ok");
    // And the arithmetic on its own, so the rule is legible without a DOM.
    expect(brokerState({ id: "broker-2", usedBytes: 0, totalBytes: 0 })).toBe("idle");
    dispose();
  });

  /**
   * The two thresholds, which are the only thing that turns a fraction into a colour.
   *
   * `DEFAULT_WARN` and `DEFAULT_DANGER` could be moved to 0.30 and 0.35 with every one of the 477
   * cases `pnpm -C frontend test packages/shell` runs still green — measured, not supposed — so a
   * cluster at 40% would have been drawn as a disk about to fill up, and the amber the reader is
   * meant to act on would appear on almost every cluster. That is the same defect as an amber count
   * on a large-but-healthy topic list, one file over: a marker that is always on is a marker nobody
   * looks at, and this one is the drawer's last line about storage.
   *
   * The boundaries are asserted as well as the middles, because `>=` and `>` differ by exactly one
   * broker sitting on the line.
   */
  it("takes its colour from the two thresholds, at their boundaries and between them", () => {
    const at = (used: number) => brokerState({ id: "b", usedBytes: used, totalBytes: 100 });
    expect(at(50)).toBe("ok");
    expect(at(74)).toBe("ok");
    // 75% is warning, not the last of ok: the guard is `>=`.
    expect(at(75)).toBe("warning");
    expect(at(89)).toBe("warning");
    expect(at(90)).toBe("failed");
    expect(at(100)).toBe("failed");
    /* And a caller's own thresholds still win, which is what the props are for — a deployment on
       thin-provisioned storage warns earlier. */
    expect(brokerState({ id: "b", usedBytes: 40, totalBytes: 100 }, 0.3, 0.35)).toBe("failed");
  });

  /**
   * A disk that has been read and is empty, which is the inversion of the rule above.
   *
   * `<Show when={percent()}>` is falsy at zero, so this exact state — `usedBytes: 0` over a
   * capacity the cluster reported perfectly well — drew `—` in `kui-storage__percent--unknown`
   * with `title="Disk usage could not be read"`, one line above a caption reading `0 B of
   * 500.0 GB`. The card contradicted itself, and the em dash asserted an unreadable disk that had
   * been read exactly. It is the file header's own "Unknown is a track, not a zero", inverted.
   */
  it("draws 0% for a disk that has used none of a known capacity, and not an em dash", () => {
    const { container, dispose } = mount(() => (
      <StorageMeter brokers={[{ id: "broker-1", usedBytes: 0, totalBytes: 500_000_000_000 }]} />
    ));
    expect(container.querySelector(".kui-storage__percent")!.textContent).toBe("0%");
    expect(container.querySelector(".kui-storage__percent--unknown")).toBeNull();
    /* And the caption is the other half of the contradiction: the two lines have to agree, and
       until now they did not. */
    expect(container.querySelector(".kui-storage__caption")!.textContent).toBe("0 B of 500.0 GB");
    dispose();
  });
});

describe("NavDrawer", () => {
  it("contains the whole cluster drawer in a named complementary landmark", () => {
    const { container, dispose } = mount(() => (
      <NavDrawer groups={NAV_GROUPS} cluster={HEALTHY_CLUSTER} />
    ));
    const drawer = container.querySelector("[data-testid='nav-drawer']");
    expect(drawer?.tagName).toBe("ASIDE");
    expect(drawer?.getAttribute("aria-label")).toBe("Cluster navigation");
    dispose();
  });

  it("labels each group's list with its own heading", () => {
    const { container, dispose } = mount(() => <NavDrawer groups={NAV_GROUPS} cluster={HEALTHY_CLUSTER} />);
    const lists = container.querySelectorAll(".kui-nav-group__list");
    expect(lists.length).toBe(2);
    const headingId = lists[0]!.getAttribute("aria-labelledby")!;
    expect(container.querySelector(`#${headingId}`)!.textContent).toBe("CLUSTER");
    dispose();
  });

  it("writes the group headings uppercase in the markup rather than transforming them", () => {
    const { container, dispose } = mount(() => <NavDrawer groups={NAV_GROUPS} cluster={HEALTHY_CLUSTER} />);
    const headings = [...container.querySelectorAll(".kui-nav-group__heading")].map((h) => h.textContent);
    expect(headings).toEqual(["CLUSTER", "ECOSYSTEM"]);
    dispose();
  });

  it("renders nothing at all for a group with no destinations", async () => {
    // ECOSYSTEM's state on a deployment that configured none of its three upstreams, and its only
    // state at all until M9. A lettered heading over an empty list reads as a list that
    // failed to load, and sends an operator hunting for an outage that does not exist — the same
    // misreading ADR-032's `not_configured → hidden` rule prevents one level up.
    const { container, dispose } = mount(() => (
      <NavDrawer groups={NAV_GROUPS_EMPTY_ECOSYSTEM} cluster={HEALTHY_CLUSTER} />
    ));
    const headings = [...container.querySelectorAll(".kui-nav-group__heading")].map((h) => h.textContent);
    expect(headings).toEqual(["CLUSTER"]);
    expect(container.querySelectorAll(".kui-nav-group__list").length).toBe(1);
    expect(container.textContent).not.toContain("ECOSYSTEM");
    dispose();
  });

  it("orders a nested tree by rank: the padlocked row last, whatever order it arrived in", () => {
    // The fixture is handed over in the wrong order on purpose — `internal` first — because
    // `nav/topicTree.ts` already emits them sorted and a renderer that merely preserved its input
    // would pass every test until somebody assembled the children from two sources the other way
    // round.
    const { container, dispose } = mount(() => (
      <NavDrawer groups={NAV_GROUPS_WITH_TREE} currentId="topics" cluster={HEALTHY_CLUSTER} />
    ));
    const subtree = container.querySelector('[data-testid="nav-topics-subtree"]')!;
    const labels = [...subtree.querySelectorAll(".kui-nav-item__label")].map((el) => el.textContent);
    expect(labels[0]).toBe("orders.*");
    expect(labels.at(-1)).toBe("internal");
    dispose();
  });

  it("opens and closes a branch from its disclosure, and removes the subtree rather than hiding it", async () => {
    const collapsed = NAV_GROUPS_WITH_TREE.map((group) => ({
      ...group,
      destinations: group.destinations.map((destination) =>
        destination.id === "topics" ? { ...destination, expanded: false } : destination,
      ),
    }));
    const { container, dispose } = mount(() => (
      <NavDrawer groups={collapsed} currentId="topics" cluster={HEALTHY_CLUSTER} />
    ));
    const disclosure = container.querySelector('[data-testid="nav-topics-disclosure"]') as HTMLButtonElement;
    expect(disclosure.getAttribute("aria-expanded")).toBe("false");
    // Not merely hidden: a collapsed subtree left in the document is still in the tab order and
    // still read aloud, which is the whole failure a disclosure exists to prevent.
    expect(container.querySelector('[data-testid="nav-topics-subtree"]')).toBeNull();

    await userEvent.click(disclosure);
    flush();
    expect(disclosure.getAttribute("aria-expanded")).toBe("true");
    expect(container.querySelector('[data-testid="nav-topics-subtree"]')).not.toBeNull();
    dispose();
  });

  it("keeps the label a link and the disclosure a separate control", () => {
    const { container, dispose } = mount(() => (
      <NavDrawer groups={NAV_GROUPS_WITH_TREE} currentId="topics" cluster={HEALTHY_CLUSTER} />
    ));
    // Two affordances, both wanted: an operator who knows which prefix they want expands, and one
    // who wants the whole list clicks the label. Merging them costs whichever loses.
    expect((container.querySelector('[data-testid="nav-topics"]') as HTMLAnchorElement).tagName).toBe("A");
    expect((container.querySelector('[data-testid="nav-topics-disclosure"]') as HTMLElement).tagName).toBe(
      "BUTTON",
    );
    dispose();
  });

  it("puts the cluster at its head, with the registration link the caller supplied", () => {
    const { container, dispose } = mount(() => (
      <NavDrawer groups={NAV_GROUPS} cluster={DEFECTIVE_CLUSTER} manageHref="/ui/clusters/manage" />
    ));
    expect(container.querySelector(".kui-brand__name")!.textContent).toBe("staging-eu-01");
    expect(container.querySelector('[data-testid="brand-add-cluster"]')!.getAttribute("href")).toBe(
      "/ui/clusters/manage",
    );
    // The product wordmark is gone from the drawer entirely (§2.1); the rail marks the product.
    expect(container.textContent).not.toContain("Kafka UI");
    dispose();
  });

  it("has no accessibility violations with a tree expanded", async () => {
    const { container, dispose } = mount(() => (
      <NavDrawer
        groups={NAV_GROUPS_WITH_TREE}
        currentId="topics"
        cluster={HEALTHY_CLUSTER}
        manageHref="/ui/clusters/manage"
      />
    ));
    expect(describeViolations(await findViolations(container))).toBe("");
    dispose();
  });

  it("has no accessibility violations in the healthy case", async () => {
    const { container, dispose } = mount(() => (
      <NavDrawer groups={NAV_GROUPS} currentId="dashboard" cluster={HEALTHY_CLUSTER} />
    ));
    const violations = await findViolations(container);
    expect(describeViolations(violations)).toBe("");
    dispose();
  });
});

describe("ClusterStatusCard", () => {
  it("says an unknown version in words, never as a dash", () => {
    const { container, dispose } = mount(() => <ClusterStatusCard cluster={VERSIONLESS_CLUSTER} />);
    expect(container.textContent).toContain("healthy · version unknown");
    expect(container.textContent).not.toContain("—");
    dispose();
  });

  it("turns an unreachable cluster into a control that asks again", async () => {
    let retried = 0;
    const { container, dispose } = mount(() => (
      <ClusterStatusCard cluster={UNREACHABLE_CLUSTER} onRetry={() => (retried += 1)} />
    ));
    const card = container.querySelector('[data-testid="cluster-status-card"]') as HTMLButtonElement;
    expect(card.tagName).toBe("BUTTON");
    expect(card.getAttribute("aria-label")).toContain("Check again");
    expect(container.textContent).toContain("last seen 4m ago");
    await userEvent.click(card);
    expect(retried).toBe(1);
    dispose();
  });

  it("offers to add a cluster when none is configured, and does not call it an error", () => {
    const { container, dispose } = mount(() => <ClusterStatusCard configureHref="/settings/clusters" />);
    expect(container.textContent).toContain("no cluster · add one");
    expect(container.querySelector(".kui-cluster-card__tile--danger")).toBeNull();
    dispose();
  });
});

describe("SearchField", () => {
  it("shows the platform's own shortcut", () => {
    expect(shortcutHint("apple")).toBe("⌘K");
    expect(shortcutHint("other")).toBe("Ctrl K");
  });

  it("keeps the same input element across a results update, so typing is never interrupted", async () => {
    /* The defect this guards: a field rebuilt mid-word loses the caret position, the composition
     * state of an input-method editor, and sometimes the text. If somebody wraps the input in a
     * conditional, the node identity changes and this fails. */
    const [status, setStatus] = createSignal<"idle" | "searching" | "ready">("idle");
    const { container, dispose } = mount(() => (
      <SearchField value="ord" onInput={() => {}} status={status()} results={[]} platform="other" />
    ));
    const before = container.querySelector("input")!;
    setStatus("searching");
    flush();
    setStatus("ready");
    flush();
    expect(container.querySelector("input")).toBe(before);
    dispose();
  });

  it("reports what it typed", async () => {
    const seen: string[] = [];
    const { container, dispose } = mount(() => (
      <SearchField value="" onInput={(v) => seen.push(v)} platform="other" />
    ));
    await userEvent.type(container.querySelector("input")!, "ord");
    expect(seen.at(-1)).toBe("ord");
    dispose();
  });

  it("stays enabled and explains itself when search is not answering", async () => {
    const { container, dispose } = mount(() => (
      <SearchField value="orders" onInput={() => {}} status="failed" platform="other" />
    ));
    const input = container.querySelector("input")!;
    expect(input.disabled).toBe(false);
    input.focus();
    flush();
    expect(container.textContent).toContain("Search is not answering right now.");
    dispose();
  });

  it("distinguishes searching, empty and failed from one another", () => {
    for (const [status, expected] of [
      ["searching", "Searching"],
      ["empty", "Nothing matches"],
      ["failed", "Search is not answering"],
    ] as const) {
      const { container, dispose } = mount(() => (
        <SearchField value="q" onInput={() => {}} status={status} platform="other" />
      ));
      container.querySelector("input")!.focus();
      flush();
      expect(container.textContent).toContain(expected);
      dispose();
    }
  });

  /**
   * Half an answer, reported as half an answer.
   *
   * The search is a fold at the gateway over three services and the distributed stack routes only
   * two of them, so a `partial` naming the schema service is a *normal* answer. Showing the two
   * lists that came back and nothing else would tell an operator their subject does not exist, and
   * a false negative in a search box is indistinguishable from a true one.
   */
  it("names a service that could not be asked, beside the results that did arrive", () => {
    const { container, dispose } = mount(() => (
      <SearchField
        value="orders"
        onInput={() => {}}
        status="ready"
        results={[
          {
            heading: "TOPICS",
            items: [{ id: "t", label: "orders.v1", href: "/ui/clusters/prod/topics/orders.v1" }],
          },
        ]}
        unavailable={["Schema Registry"]}
        platform="other"
      />
    ));
    container.querySelector("input")!.focus();
    flush();

    expect(container.textContent).toContain("orders.v1");
    expect(container.textContent).toContain("Schema Registry");
    /* "missing, not empty" is the whole sentence: the reader has to know the difference between a
       search that found nothing and a search that never asked. */
    expect(container.textContent).toContain("missing, not empty");
    dispose();
  });

  it("says who was not asked even when nobody answered at all", () => {
    const { container, dispose } = mount(() => (
      <SearchField
        value="orders"
        onInput={() => {}}
        status="ready"
        results={[]}
        unavailable={["Schema Registry", "Topics"]}
        platform="other"
      />
    ));
    container.querySelector("input")!.focus();
    flush();
    expect(container.textContent).toContain("Schema Registry, Topics");
    dispose();
  });

  it("claims the listbox role on the rows and never on the panel around them", async () => {
    /* The sentence above sits in the same overlay as the options. A listbox whose children are not
       options is `aria-required-children`, and it leaves a screen-reader user with a list box whose
       announced count does not match what is in it. */
    const { container, dispose } = mount(() => (
      <SearchField
        value="orders"
        onInput={() => {}}
        status="ready"
        results={[{ heading: "TOPICS", items: [{ id: "t", label: "orders.v1", href: "/t" }] }]}
        unavailable={["Schema Registry"]}
        platform="other"
      />
    ));
    container.querySelector("input")!.focus();
    flush();

    const listbox = container.querySelector('[role="listbox"]')!;
    expect(listbox).not.toBeNull();
    expect(listbox.textContent).not.toContain("Schema Registry");
    // And the input still points at it, which is what makes it a combobox rather than a text field.
    const input = container.querySelector("input")!;
    expect(input.getAttribute("aria-controls")).toBe(listbox.id);

    expect(describeViolations(await findViolations(container))).toBe("");
    dispose();
  });

  it("does not claim the listbox role while it is showing a sentence", async () => {
    for (const status of ["searching", "empty", "failed"] as const) {
      const { container, dispose } = mount(() => (
        <SearchField value="q" onInput={() => {}} status={status} platform="other" />
      ));
      container.querySelector("input")!.focus();
      flush();
      expect(container.querySelector('[role="listbox"]')).toBeNull();
      // The element is still there, so `aria-controls` never points at nothing.
      const input = container.querySelector("input")!;
      const controls = CSS.escape(input.getAttribute("aria-controls")!);
      expect(container.querySelector(`#${controls}`)).not.toBeNull();
      dispose();
    }
  });

  it("holds a query to the length the endpoint accepts", () => {
    /* A 201-character `q` is a 400, and the only failure this overlay can draw says "search is not
       answering" — a sentence that sends somebody to look at a gateway that is working. */
    const { container, dispose } = mount(() => (
      <SearchField value="" onInput={() => {}} maxLength={200} platform="other" />
    ));
    expect(container.querySelector("input")!.getAttribute("maxlength")).toBe("200");
    dispose();
  });

  /**
   * The 120 ms the overlay stays open after the field loses focus.
   *
   * A pointer press on a result focuses the link, which blurs the input; the `click` only arrives
   * when the button comes back up. `onBlur={() => setFocused(false)}` therefore removes the row
   * from under the cursor before it can be clicked, and every result in the panel becomes
   * unclickable while looking perfectly normal — a defect with no visible symptom at all. That
   * mutation left every case `pnpm -C frontend test packages/shell/src/chrome` runs green.
   *
   * The clock is faked so the wait is a fact rather than a race, and only the timer functions are
   * faked: Solid 2 batches to a microtask, and a fake `queueMicrotask` would stop the renderer
   * rather than the component. The two waits are absolute milliseconds and not the exported
   * constant, so shrinking the grace period to zero fails here too.
   *
   * The constant is nevertheless *read* here, and that is the second half of the same argument.
   * `RESULT_CLICK_GRACE_MS` was exported and imported by nothing anywhere in the repository — the
   * only other mention is a sentence of prose in `e2e/search.spec.ts` — so the export was an
   * assertion nobody had written. Bracketing it against the two absolute waits is what those two
   * numbers *mean*: the panel is open at 100 ms and shut by 300, and a constant outside that band
   * would make one of the two waits assert nothing while this case went on passing.
   */
  it("keeps the results panel open long enough for a click on a result to land", () => {
    expect(RESULT_CLICK_GRACE_MS).toBeGreaterThan(100);
    expect(RESULT_CLICK_GRACE_MS).toBeLessThan(300);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    try {
      const { container, dispose } = mount(() => (
        <SearchField
          value="orders"
          onInput={() => {}}
          status="ready"
          results={[{ heading: "TOPICS", items: [{ id: "t", label: "orders.v1", href: "/t" }] }]}
          platform="other"
        />
      ));
      const panel = container.querySelector(".kui-global-search__results")!;
      const input = container.querySelector("input")!;
      input.focus();
      flush();
      expect(panel.hasAttribute("hidden")).toBe(false);

      /* What a pointer press on a result actually does first: focus moves to the link, and the
         input is blurred before any click exists. */
      const result = container.querySelector<HTMLAnchorElement>('[role="option"]')!;
      result.focus();
      flush();
      expect(document.activeElement).toBe(result);
      expect(panel.hasAttribute("hidden")).toBe(false);

      // Still there a hundred milliseconds later, which is longer than a mouse button is held.
      vi.advanceTimersByTime(100);
      flush();
      expect(panel.hasAttribute("hidden")).toBe(false);

      // And it does close: the grace period is a deferral, not a decision to stay open.
      vi.advanceTimersByTime(200);
      flush();
      expect(panel.hasAttribute("hidden")).toBe(true);
      dispose();
    } finally {
      vi.useRealTimers();
    }
  });

  /**
   * The overlay needs text, not just focus.
   *
   * `open()` is `focused() && props.value.length > 0`, and dropping the second half left every
   * case `pnpm -C frontend test packages/shell/src/chrome` runs green. What it costs is a panel
   * that appears the moment the box is
   * tabbed through — over an empty query it can only be a boundary with nothing in it, and the ⌘K
   * shortcut puts it there on every use.
   */
  it("draws no overlay over an empty box, however long it is focused", () => {
    const { container, dispose } = mount(() => (
      <SearchField value="" onInput={() => {}} status="idle" platform="other" />
    ));
    const input = container.querySelector("input")!;
    input.focus();
    flush();
    expect(container.querySelector(".kui-global-search__results")!.hasAttribute("hidden")).toBe(
      true,
    );
    // And the combobox says so, because that is what a screen reader is told about the panel.
    expect(input.getAttribute("aria-expanded")).toBe("false");
    dispose();
  });

  it("has a real label and not only a placeholder", () => {
    const { container, dispose } = mount(() => <SearchField value="" onInput={() => {}} platform="other" />);
    const input = container.querySelector("input")!;
    const label = container.querySelector(`label[for="${input.id}"]`);
    expect(label?.textContent).toContain("Search topics");
    dispose();
  });
});

/**
 * The notification panel's two axes.
 *
 * Everything else about this component is checked in a story, and a story is not an assertion: the
 * shipped component picked its glyph from the *severity*, `SCREENS-V4.md` §3.9 says why that cannot
 * draw the design, wave 2 repaired it — and replacing `glyphOf` with `SEVERITY_GLYPH[…]` afterwards
 * left every one of the 281 shell tests green, because nothing but a Storybook page had ever looked
 * at a glyph. These cases look at one.
 */
describe("the notifications panel", () => {
  const at = new Date("2026-09-06T09:00:00.000Z");
  const now = new Date("2026-09-06T09:05:00.000Z");

  const glyphOf = (container: Element, id: string): string | null | undefined =>
    container.querySelector(`[data-testid="notice-${id}"] [data-icon]`)?.getAttribute("data-icon");

  it("draws two notices of one severity with the glyphs their categories ask for", () => {
    /* This is `M06`'s own pair: two warnings, a rebalance and a filling disk. A component that
       derived the glyph from the severity draws the same triangle twice and loses the half an
       operator scans for — the severity is already in the colour. */
    const notices: readonly Notice[] = [
      { id: "a", severity: "warning", category: "rebalance", title: "Group is rebalancing", at },
      { id: "b", severity: "warning", category: "storage", title: "Log directory is filling", at },
    ];
    const { container, dispose } = mount(() => (
      <NotificationPanel feed={{ kind: "ready", notices }} now={now} />
    ));

    expect(glyphOf(container, "a")).toBe("refresh");
    expect(glyphOf(container, "b")).toBe("disk");
    expect(glyphOf(container, "a")).not.toBe(glyphOf(container, "b"));
    dispose();
  });

  /**
   * The two rules `services/alerts` opens at the same severity, which is §3.9's correction again.
   *
   * A partition with no leader and a partition short of replicas are both `warning`, and they are
   * the two an operator most needs to tell apart at a glance: the first is data unavailable now,
   * the second is data one broker away from it. A glyph table that answered the same mark for both
   * — or that had no mark for either, which is what this component did before these two categories
   * existed — puts two identical amber rows on the panel and loses the half that says what broke.
   */
  it("draws the two alert categories that share a severity with two different glyphs", () => {
    const notices: readonly Notice[] = [
      { id: "p", severity: "warning", category: "partition", title: "orders.v1-3 has no leader", at },
      { id: "r", severity: "warning", category: "replication", title: "orders.v1-7 is under-replicated", at },
    ];
    const { container, dispose } = mount(() => (
      <NotificationPanel feed={{ kind: "ready", notices }} now={now} />
    ));

    expect(glyphOf(container, "p")).toBe("partitions");
    expect(glyphOf(container, "r")).toBe("topology");
    expect(glyphOf(container, "p")).not.toBe(glyphOf(container, "r"));
    dispose();
  });

  it("falls back to the severity's glyph only for a notice that recorded no category", () => {
    /* Not a placeholder to be removed later: a notification whose category nothing recorded is a
       real case, and inventing one for it would be worse than the generic mark — a disk icon over a
       rebalance is a confident lie about what broke. */
    const notices: readonly Notice[] = [
      { id: "c", severity: "warning", title: "Something is not right", at },
      { id: "d", severity: "warning", category: "topic", title: "Topic created", at },
    ];
    const { container, dispose } = mount(() => (
      <NotificationPanel feed={{ kind: "ready", notices }} now={now} />
    ));

    expect(glyphOf(container, "c")).toBe("warning");
    expect(glyphOf(container, "d")).toBe("topics");
    dispose();
  });

  it("takes the tile's tone from the severity, which is the other axis", () => {
    // The two axes are separate in both directions: one severity with two categories above, and
    // here two severities with one category, which must not draw the same tone.
    const notices: readonly Notice[] = [
      { id: "e", severity: "warning", category: "cluster", title: "A broker is slow", at },
      { id: "f", severity: "danger", category: "cluster", title: "A broker is gone", at },
    ];
    const { container, dispose } = mount(() => (
      <NotificationPanel feed={{ kind: "ready", notices }} now={now} />
    ));

    const tone = (id: string) =>
      container.querySelector(`[data-testid="notice-${id}"] .kui-icon-tile`)?.className;
    expect(tone("e")).toContain("kui-icon-tile--warning");
    expect(tone("f")).toContain("kui-icon-tile--danger");
    expect(glyphOf(container, "e")).toBe(glyphOf(container, "f"));
    dispose();
  });

  /**
   * A severity this build has no colour for, and the two rules that keep it honest.
   *
   * The word travels from `services/alerts` verbatim — the kernel folds nothing and neither does
   * `data/alerts.ts`, which matches by name — so a `blocker` shipped by a later service reaches
   * this component as `unknown`. It takes the **neutral** tile, and both alternatives are worse in
   * opposite directions: `danger` claims a seriousness nothing established, and `info` denies one.
   *
   * Its glyph is the same mark `info` takes, and that is not a collapse of the two: the fallback
   * glyph is reached only when the notification carried no category either, and its whole claim is
   * that a notification happened. The seriousness is the tone, and the tone is different.
   */
  it("draws a severity it has no colour for on a neutral tile, and never as info", () => {
    const notices: readonly Notice[] = [
      { id: "i", severity: "unknown", title: "Sent with a word this build does not know", at },
      { id: "j", severity: "info", title: "Cluster reconnected", at },
    ];
    const { container, dispose } = mount(() => (
      <NotificationPanel feed={{ kind: "ready", notices }} now={now} />
    ));

    const tone = (id: string) =>
      container.querySelector(`[data-testid="notice-${id}"] .kui-icon-tile`)?.className;
    expect(tone("i")).toContain("kui-icon-tile--neutral");
    expect(tone("i")).not.toContain("kui-icon-tile--primary");
    expect(tone("i")).not.toContain("kui-icon-tile--danger");
    /* Its fallback glyph is the same neutral mark `info` takes — the picture claims only that a
       notification happened, and the seriousness is carried by the tile's tone, which differs. A
       shape invented for the unknown case would be a picture of something nobody said. */
    expect(glyphOf(container, "i")).toBe("info");
    expect(glyphOf(container, "j")).toBe("info");
    // And the row is drawn at all: an event nobody can see is an event nobody acts on.
    expect(container.textContent).toContain("Sent with a word this build does not know");
    // `info` still draws its own tone, so the two are not one rendering under two names.
    expect(tone("j")).toContain("kui-icon-tile--primary");
    dispose();
  });

  it("has no accessibility violations with a mixed feed", async () => {
    const notices: readonly Notice[] = [
      { id: "g", severity: "warning", category: "rebalance", title: "Group is rebalancing", at },
      {
        id: "h",
        severity: "success",
        category: "schema",
        title: "Schema registered",
        at,
        read: true,
      },
    ];
    const { container, dispose } = mount(() => (
      <NotificationPanel feed={{ kind: "ready", notices }} now={now} onMarkAllRead={() => {}} />
    ));
    expect(describeViolations(await findViolations(container))).toBe("");
    dispose();
  });
});

describe("Breadcrumb", () => {
  it("does not make the current page a link", () => {
    const { container, dispose } = mount(() => (
      <Breadcrumb trail={[{ label: "Topics", href: "/topics" }, { label: "orders.payments.v2" }]} />
    ));
    expect(container.querySelectorAll("a").length).toBe(1);
    expect(container.querySelector('[aria-current="page"]')!.textContent).toBe("orders.payments.v2");
    dispose();
  });

  it("collapses a long trail behind a real button and expands it when pressed", async () => {
    const trail = [
      { label: "Clusters", href: "/c" },
      { label: "prod-kyiv-01", href: "/c/p" },
      { label: "Topics", href: "/t" },
      { label: LONG_TOPIC, href: "/t/x" },
      { label: "Partition 7" },
    ];
    const { container, dispose } = mount(() => <Breadcrumb trail={trail} />);
    const more = container.querySelector('[data-testid="breadcrumb-expand"]') as HTMLButtonElement;
    expect(more.getAttribute("aria-label")).toBe("Show 2 hidden levels");
    await userEvent.click(more);
    flush();
    expect(container.querySelector('[data-testid="breadcrumb-expand"]')).toBeNull();
    expect(container.querySelectorAll("li").length).toBe(5);
    dispose();
  });

  it("keeps its landmark when the trail is empty, so the page's structure does not change", () => {
    const { container, dispose } = mount(() => <Breadcrumb trail={[]} />);
    expect(container.querySelector('nav[aria-label="Application breadcrumb"]')).not.toBeNull();
    dispose();
  });

  it("has no accessibility violations when collapsed", async () => {
    const { container, dispose } = mount(() => (
      <Breadcrumb
        trail={[
          { label: "Clusters", href: "/c" },
          { label: "prod-kyiv-01", href: "/c/p" },
          { label: "Topics", href: "/t" },
          { label: "orders.payments.v2", href: "/t/x" },
          { label: "Partition 7" },
        ]}
      />
    ));
    expect(describeViolations(await findViolations(container))).toBe("");
    dispose();
  });
});

describe("TabStrip", () => {
  it("is a navigation landmark of links, and never claims the ARIA tab pattern", () => {
    const { container, dispose } = mount(() => (
      <TabStrip tabs={TOPIC_TABS} currentId="messages" label="Topic sections" />
    ));
    expect(container.querySelector('nav[aria-label="Topic sections"]')).not.toBeNull();
    expect(container.querySelector('[role="tablist"]')).toBeNull();
    expect(container.querySelectorAll("a").length).toBe(TOPIC_TABS.length);
    dispose();
  });

  it("marks the current tab with aria-current as well as with a fill", () => {
    const { container, dispose } = mount(() => (
      <TabStrip tabs={TOPIC_TABS} currentId="messages" label="Topic sections" />
    ));
    const current = container.querySelector('[aria-current="page"]')!;
    expect(current.textContent).toContain("Messages");
    expect(current.classList.contains("kui-page-tabs__tab--current")).toBe(true);
    dispose();
  });

  it("prints a zero count, because zero consumers is a fact", () => {
    const tabs = TOPIC_TABS.map((t) => (t.id === "consumers" ? { ...t, count: 0 } : t));
    const { container, dispose } = mount(() => (
      <TabStrip tabs={tabs} currentId="consumers" label="Topic sections" />
    ));
    expect(container.querySelector('[data-testid="tab-consumers"]')!.textContent).toContain("0");
    dispose();
  });

  it("has no accessibility violations", async () => {
    const { container, dispose } = mount(() => (
      <TabStrip tabs={TOPIC_TABS} currentId="messages" label="Topic sections" />
    ));
    expect(describeViolations(await findViolations(container))).toBe("");
    dispose();
  });
});

describe("TopBar", () => {
  const base = {
    crumbs: [{ label: "prod-kyiv-01", href: "#c" }, { label: "Topics" }],
    search: { value: "", onInput: () => {}, platform: "other" as const },
  };

  it("says which theme mode is in force, in words, for all three modes", () => {
    for (const [theme, expected] of [
      ["auto", "Theme: follows system"],
      ["light", "Theme: light"],
      ["dark", "Theme: dark"],
    ] as const) {
      const { container, dispose } = mount(() => <TopBar {...base} theme={theme} />);
      const control = container.querySelector('[data-testid="theme-control"]')!;
      expect(control.getAttribute("aria-label")).toContain(expected);
      dispose();
    }
  });

  it("puts the unread count in the accessible name rather than only in a badge", () => {
    const { container, dispose } = mount(() => <TopBar {...base} theme="dark" unreadCount={3} />);
    expect(container.querySelector('[data-testid="notifications"]')!.getAttribute("aria-label")).toBe(
      "Notifications, 3 unread",
    );
    expect(container.querySelector(".kui-bell__badge")).not.toBeNull();
    dispose();
  });

  it("draws no marker at all when nothing is unread", () => {
    const { container, dispose } = mount(() => <TopBar {...base} theme="dark" unreadCount={0} />);
    expect(container.querySelector(".kui-bell__badge")).toBeNull();
    dispose();
  });

  /**
   * The bell over an alerts feed, which is three values and three renderings.
   *
   * The badge is the *open count the server sent* and the read marker is its tone. The two are
   * separate fields because they answer separate questions and the failure of collapsing them is
   * silent: hiding the count once somebody had read it would leave the bell saying nothing while
   * the alerts card beside it, reading the same kernel store, went on drawing `2 open`.
   *
   * Zero and `null` both draw no badge and they are **not** the same state. The absence of a badge
   * cannot say which, so the accessible name does — "no open alerts" for a service that counted
   * none, and "the number of open alerts is not known" for one that has not answered. Reading the
   * second as the first is the reassuring misreading this whole product is built against.
   */
  it("carries no badge when nothing is open, and says so in words", () => {
    const { container, dispose } = mount(() => (
      <TopBar {...base} theme="dark" alertsOpen={0} alertsUnread={false} />
    ));
    const bell = container.querySelector('[data-testid="notifications"]')!;
    expect(container.querySelector(".kui-bell__badge")).toBeNull();
    // Not a `0`, and not silence either: the sentence is the assertion the badge cannot make.
    expect(bell.getAttribute("aria-label")).toBe("Notifications, no open alerts");
    expect(bell.textContent).not.toContain("0");
    dispose();
  });

  it("carries no badge when nobody has said how many are open, and does not claim none are", () => {
    const { container, dispose } = mount(() => (
      <TopBar {...base} theme="dark" alertsOpen={null} />
    ));
    const bell = container.querySelector('[data-testid="notifications"]')!;
    expect(container.querySelector(".kui-bell__badge")).toBeNull();
    expect(bell.getAttribute("aria-label")).toBe(
      "Notifications, the number of open alerts is not known",
    );
    dispose();
  });

  it("draws the server's open count, and its read marker as the badge's tone", () => {
    const unread = mount(() => (
      <TopBar {...base} theme="dark" alertsOpen={2} alertsUnread={true} />
    ));
    const badge = unread.container.querySelector(".kui-bell__badge")!;
    expect(badge.textContent).toBe("2");
    expect(badge.className).not.toContain("kui-bell__badge--read");
    /* In words as well as in the fill, because the read/unread distinction is otherwise carried by
       a background colour and nothing else. */
    expect(
      unread.container.querySelector('[data-testid="notifications"]')!.getAttribute("aria-label"),
    ).toBe("Notifications, 2 open alerts, unread");
    unread.dispose();

    const read = mount(() => <TopBar {...base} theme="dark" alertsOpen={2} alertsUnread={false} />);
    const tallied = read.container.querySelector(".kui-bell__badge")!;
    // Still counted. An alert that is open is open whether or not anybody has looked at it.
    expect(tallied.textContent).toBe("2");
    expect(tallied.className).toContain("kui-bell__badge--read");
    expect(
      read.container.querySelector('[data-testid="notifications"]')!.getAttribute("aria-label"),
    ).toBe("Notifications, 2 open alerts");
    read.dispose();
  });

  /**
   * The cap, which is a layout rule and was asserted nowhere in this package.
   *
   * `count > 9 ? "9+" : String(count)` could be written `String(count)` with every one of the 506
   * shell cases green, because the only assertion of the cap anywhere in the repository is inside a
   * browser case that needs a stack to run. A cluster mid-incident with 147 open events would then
   * draw a three-digit badge — wider than the bell it is anchored to, so it overhangs the search
   * field beside it — and nothing in this suite would notice.
   *
   * The exact number stays in the accessible name, which is where a figure belongs: the badge is a
   * marker and `9+` is honest about being one, while "Notifications, 147 open alerts" is the
   * sentence a screen reader gets and the one a person can act on.
   */
  it("caps the badge at 9+ and keeps the exact figure in the accessible name", () => {
    const many = mount(() => (
      <TopBar {...base} theme="dark" alertsOpen={147} alertsUnread={true} />
    ));
    expect(many.container.querySelector(".kui-bell__badge")?.textContent).toBe("9+");
    expect(
      many.container.querySelector('[data-testid="notifications"]')!.getAttribute("aria-label"),
    ).toBe("Notifications, 147 open alerts, unread");
    many.dispose();

    // Nine is the last figure drawn as itself, and ten is the first that is not. A cap asserted
    // only well past its boundary is a cap that can move by one and stay green.
    const nine = mount(() => <TopBar {...base} theme="dark" alertsOpen={9} />);
    expect(nine.container.querySelector(".kui-bell__badge")?.textContent).toBe("9");
    nine.dispose();

    const ten = mount(() => <TopBar {...base} theme="dark" alertsOpen={10} />);
    expect(ten.container.querySelector(".kui-bell__badge")?.textContent).toBe("9+");
    ten.dispose();
  });

  it("caps the unread badge too, where no alerts feed sits behind the bell", () => {
    /* The other branch of the same expression. `unreadCount` is what every caller that is not the
       frame passes — the stories, the notification cases — so a cap asserted only over `alertsOpen`
       would leave half of `badgeText` free. */
    const { container, dispose } = mount(() => <TopBar {...base} theme="dark" unreadCount={23} />);
    expect(container.querySelector(".kui-bell__badge")?.textContent).toBe("9+");
    const marked = container.querySelector('[data-testid="notifications"]')!;
    expect(marked.getAttribute("aria-label")).toBe("Notifications, 23 unread");
    dispose();
  });

  it("opens the panel only when the caller says it is open", () => {
    // The panel's open state is the caller's, so that Escape and a click elsewhere can close it
    // from outside the bar. A bar that owned it would be a panel nothing else could dismiss.
    const closed = mount(() => <TopBar {...base} theme="dark" />);
    expect(closed.container.querySelector('[data-testid="notification-panel"]')).toBeNull();
    closed.dispose();

    const open = mount(() => (
      <TopBar {...base} theme="dark" notificationsOpen notifications={{ kind: "ready", notices: [] }} />
    ));
    expect(open.container.querySelector('[data-testid="notification-panel"]')).not.toBeNull();
    open.dispose();
  });

  /* The kernel's dialogs listen for Escape on `document` as well, and they register after the bar,
     so one key would otherwise close the dialog and the panel beneath it together. */
  it("leaves Escape to a modal surface open over the panel", () => {
    const close = vi.fn();
    const { dispose } = mount(() => (
      <TopBar {...base} theme="dark" notificationsOpen notifications={{ kind: "ready", notices: [] }}
        onCloseNotifications={close} />
    ));
    const escape = () => document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    const modal = document.createElement("div");
    modal.setAttribute("aria-modal", "true");
    document.body.appendChild(modal);
    escape();
    expect(close).not.toHaveBeenCalled();
    modal.remove();
    escape();
    expect(close).toHaveBeenCalledTimes(1);
    dispose();
  });

  it("says where you are, and does not link the page you are on", () => {
    const { container, dispose } = mount(() => <TopBar {...base} theme="dark" />);
    const current = container.querySelector('[aria-current="page"]')!;
    expect(current.textContent).toBe("Topics");
    expect(current.tagName).not.toBe("A");
    dispose();
  });

  it("has no accessibility violations", async () => {
    const { container, dispose } = mount(() => <TopBar {...base} theme="dark" unreadCount={2} />);
    expect(describeViolations(await findViolations(container))).toBe("");
    dispose();
  });
});

describe("the appearance popover", () => {
  /**
   * Preferences that paint a detached element and remember nothing.
   *
   * The application hands the popover the kernel's module-level singletons; a suite that drove
   * those would share `localStorage` with the next suite, need a browser that has one, and repaint
   * the test runner. The point of the props is exactly that this substitution is possible — and the
   * substitution is the *object*, never a second storage key, because two spellings of one
   * preference is the failure this control exists to avoid.
   */
  const preferences = (): AppearancePreferences => {
    const root = document.createElement("div");
    return {
      theme: createRootPreference<ThemeChoice>({
        attribute: "data-theme",
        storageKey: "kui.theme",
        values: ["auto", "light", "dark"],
        fallback: "auto",
        attributeValue: (chosen) => (chosen === "auto" ? null : chosen),
        storage: null,
        root,
      }),
      accent: createRootPreference<AccentChoice>({
        attribute: "data-accent",
        storageKey: "kui.accent",
        values: ["blue", "teal", "green", "amber"],
        fallback: "blue",
        attributeValue: (chosen) => (chosen === "blue" ? null : chosen),
        storage: null,
        root,
      }),
      density: createRootPreference<DensityChoice>({
        attribute: "data-density",
        storageKey: "kui.density",
        values: ["comfortable", "compact"],
        fallback: "comfortable",
        attributeValue: (chosen) => (chosen === "compact" ? "compact" : null),
        storage: null,
        root,
      }),
    };
  };

  /**
   * One vocabulary, drawn rather than duplicated.
   *
   * The words were written twice — here and in `pages/SettingsPage.tsx` — and had already drifted
   * on the one option that most needed explaining: this control said "Auto" and the settings said
   * "Match the system", which is one preference under two names with no way for a reader to tell
   * that it is one preference. The table is published from `chrome/appearance.ts` now, and this is
   * the case that fails if a copy grows back here.
   */
  it("draws the published appearance vocabulary and no words of its own", () => {
    const chosen = preferences();
    const { container, dispose } = mount(() => <AppearancePopover preferences={chosen} />);

    const labels = (testId: string) =>
      [...container.querySelectorAll(`[data-testid="${testId}"] label`)].map((label) =>
        label.textContent?.trim(),
      );

    expect(labels("appearance-theme")).toEqual(THEME_OPTIONS.map((option) => option.label));
    expect(labels("appearance-accent")).toEqual(ACCENT_OPTIONS.map((option) => option.label));
    expect(labels("appearance-density")).toEqual(DENSITY_OPTIONS.map((option) => option.label));

    /* And the sentence the short label leaves out. "Auto" is a segment, not an explanation, and
       nobody guesses that it keeps following the system rather than resolving once at load. */
    expect(container.textContent).toContain(appearanceHelp(THEME_OPTIONS));
    dispose();
  });

  it("offers three theme segments, because the preference has three values", async () => {
    // `SCREENS-V4.md` §7.4, settled in favour of keeping `auto`: it is the default, and it is the
    // one the other two cannot express — a laptop that turns dark at sunset turns KUI with it.
    const chosen = preferences();
    const { container, dispose } = mount(() => <AppearancePopover preferences={chosen} />);
    const group = container.querySelector('[data-testid="appearance-theme"]')!;
    const labels = [...group.querySelectorAll("label")].map((label) => label.textContent?.trim());
    expect(labels).toEqual(["Auto", "Light", "Dark"]);
    dispose();
  });

  it("writes each choice to the preference the settings page writes to", async () => {
    const chosen = preferences();
    const { container, dispose } = mount(() => <AppearancePopover preferences={chosen} />);

    await userEvent.click(container.querySelector('[data-testid="appearance-theme"] input[value="dark"]')!);
    await userEvent.click(container.querySelector('[data-testid="appearance-accent"] input[value="teal"]')!);
    await userEvent.click(
      container.querySelector('[data-testid="appearance-density"] input[value="compact"]')!,
    );
    flush();

    expect(chosen.theme.choice()).toBe("dark");
    expect(chosen.accent.choice()).toBe("teal");
    expect(chosen.density.choice()).toBe("compact");
    dispose();
  });

  it("has no accessibility violations", async () => {
    const { container, dispose } = mount(() => <AppearancePopover preferences={preferences()} />);
    expect(describeViolations(await findViolations(container))).toBe("");
    dispose();
  });

  it("opens under the top bar's sliders glyph and closes on Escape", async () => {
    const chosen = preferences();
    const { container, dispose } = mount(() => (
      <TopBar
        crumbs={[{ label: "prod-kyiv-01" }]}
        search={{ value: "", onInput: () => {}, platform: "other" }}
        appearance={chosen}
      />
    ));
    const control = container.querySelector('[data-testid="appearance-control"]') as HTMLButtonElement;
    expect(control.getAttribute("aria-expanded")).toBe("false");

    await userEvent.click(control);
    flush();
    expect(container.querySelector('[data-testid="appearance-popover"]')).not.toBeNull();
    expect(control.getAttribute("aria-expanded")).toBe("true");

    await userEvent.keyboard("{Escape}");
    flush();
    expect(container.querySelector('[data-testid="appearance-popover"]')).toBeNull();
    // Focus goes back to the glyph. Escape that left focus on a removed element drops the keyboard
    // user at the top of the document.
    expect(document.activeElement).toBe(control);
    dispose();
  });

  it("closes when the pointer goes down outside it, and not when it goes down inside", async () => {
    const chosen = preferences();
    const { container, dispose } = mount(() => (
      <TopBar search={{ value: "", onInput: () => {}, platform: "other" }} appearance={chosen} />
    ));
    await userEvent.click(container.querySelector('[data-testid="appearance-control"]')!);
    flush();

    // A press that lands on a control inside the panel must not close it: `mousedown` and not
    // `click` is exactly so that a drag which begins on a segment and overshoots it is still the
    // gesture the operator meant.
    await userEvent.pointer({
      keys: "[MouseLeft>]",
      target: container.querySelector('[data-testid="appearance-theme"]')!,
    });
    flush();
    expect(container.querySelector('[data-testid="appearance-popover"]')).not.toBeNull();

    await userEvent.pointer({ keys: "[MouseLeft>]", target: document.body });
    flush();
    expect(container.querySelector('[data-testid="appearance-popover"]')).toBeNull();
    dispose();
  });

  it("cycles the theme preference from the top bar's glyph, and names the mode in words", async () => {
    const chosen = preferences();
    const { container, dispose } = mount(() => (
      <TopBar search={{ value: "", onInput: () => {}, platform: "other" }} appearance={chosen} />
    ));
    const control = container.querySelector('[data-testid="theme-control"]') as HTMLButtonElement;
    expect(control.getAttribute("aria-label")).toContain("Theme: follows system");

    await userEvent.click(control);
    flush();
    expect(chosen.theme.choice()).toBe("light");
    expect(control.getAttribute("aria-label")).toContain("Theme: light");

    await userEvent.click(control);
    flush();
    expect(chosen.theme.choice()).toBe("dark");

    await userEvent.click(control);
    flush();
    // Back to `auto`, which is a state a two-way toggle cannot return to at all.
    expect(chosen.theme.choice()).toBe("auto");
    dispose();
  });
});

describe("EnvRail", () => {
  it("contains its destinations and account in a named navigation landmark", () => {
    const { container, dispose } = mount(() => (
      <EnvRail environments={CLUSTERS} currentId="prod-kyiv-01" />
    ));
    const rail = container.querySelector("[data-testid='env-rail']");
    expect(rail?.tagName).toBe("NAV");
    expect(rail?.getAttribute("aria-label")).toBe("Environment navigation");
    dispose();
  });

  it("admits it does not know who is signed in rather than inventing initials", () => {
    // Moved here from `TopBar` with the avatar itself. The property is the point, not where it
    // lives: two invented initials are worse than admitting we do not know, in a product where the
    // avatar is how you check whose credentials are about to purge a topic.
    const { container, dispose } = mount(() => <EnvRail environments={CLUSTERS} currentId="prod-kyiv-01" />);
    const avatar = container.querySelector(".kui-avatar")!;
    expect(avatar.getAttribute("aria-label")).toContain("unavailable");
    expect(avatar.textContent).toBe("");
    dispose();
  });

  it("names every environment in full, because a single letter is not an identifier", () => {
    // `prod-kyiv-01` and `prod-eu-02` are both drawn as "P". The accessible name is the only thing
    // that distinguishes them, so it is asserted rather than left to the design's discretion.
    const { container, dispose } = mount(() => (
      <EnvRail
        environments={[
          { id: "1", name: "prod-kyiv-01", health: "healthy" },
          { id: "2", name: "prod-eu-02", health: "degraded" },
        ]}
        currentId="1"
      />
    ));
    const tiles = [...container.querySelectorAll('[data-testid^="env-tile-"]')];
    expect(tiles.map((tile) => tile.getAttribute("aria-label"))).toEqual([
      "prod-kyiv-01 — healthy",
      "prod-eu-02 — degraded",
    ]);
    /* And the letter itself, which is the other half of the sentence above: the comment has said
       "both drawn as P" since the rail was built and nothing had ever looked. Dropping
       `tileLetter`'s `.toUpperCase()` left every case
       `pnpm -C frontend test packages/shell` runs green over a rail of lowercase tiles. */
    expect(tiles.map((tile) => tile.querySelector(".kui-rail__letter")?.textContent)).toEqual([
      "P",
      "P",
    ]);
    dispose();
  });

  it("takes one whole character for the tile, not half of an astral-plane one", () => {
    /* The second rule the function states and the second nothing asserted: `name[0]` would cut a
       surrogate pair in two and render a replacement glyph, and environment names come out of
       configuration files that people write. `?` for a name with nothing in it, because a blank
       tile reads as a rail that failed to load. */
    expect(tileLetter("\u{1F6D2}shop")).toBe("\u{1F6D2}");
    expect(tileLetter("   ")).toBe("?");
  });

  it("keeps its width when no cluster has arrived yet", () => {
    // The frame's geometry must not depend on how many clusters exist: a rail that appeared with
    // the first response would shift the whole page sideways at an arbitrary moment.
    const { container, dispose } = mount(() => <EnvRail environments={[]} />);
    expect(container.querySelector('[data-testid="env-rail"]')).not.toBeNull();
    dispose();
  });

  it("has no accessibility violations", async () => {
    const { container, dispose } = mount(() => (
      <EnvRail environments={CLUSTERS} currentId="prod-kyiv-01" accountName="Olena Petrenko" />
    ));
    expect(describeViolations(await findViolations(container))).toBe("");
    dispose();
  });
});

describe("the account panel", () => {
  it("does not draw a panel the caller did not supply", () => {
    // A deployment with `authType: "disabled"` passes no handler and no panel. The avatar has to
    // stay a picture: a button that opens an empty panel, or a "Sign out" that clears a session
    // that never existed, both teach the operator that the rail's controls do nothing.
    const { container, dispose } = mount(() => (
      <EnvRail environments={CLUSTERS} currentId="prod-kyiv-01" accountName="Olena Petrenko" />
    ));
    expect(container.querySelector(".kui-rail__account button")).toBeNull();
    expect(container.querySelector('[data-testid="account-menu"]')).toBeNull();
    dispose();
  });

  it("keeps the panel shut until the caller says it is open", () => {
    const { container, dispose } = mount(() => (
      <EnvRail
        environments={CLUSTERS}
        currentId="prod-kyiv-01"
        accountName="Olena Petrenko"
        onOpenAccount={() => undefined}
        accountPanel={<AccountMenu name="olena" onSignOut={() => undefined} />}
      />
    ));
    // Openness belongs to whoever can also close it — Escape, a click elsewhere. A panel that owned
    // its own openness would be one nothing else could dismiss.
    expect(container.querySelector('[data-testid="account-menu"]')).toBeNull();
    dispose();
  });

  it("shows the panel, anchored to the avatar, when it is open", () => {
    const { container, dispose } = mount(() => (
      <EnvRail
        environments={CLUSTERS}
        currentId="prod-kyiv-01"
        accountName="Olena Petrenko"
        onOpenAccount={() => undefined}
        accountOpen
        accountPanel={<AccountMenu name="olena" onSignOut={() => undefined} />}
      />
    ));
    const panel = container.querySelector(".kui-rail__account .kui-rail__account-panel");
    expect(panel).not.toBeNull();
    expect(panel!.querySelector('[data-testid="account-name"]')!.textContent).toBe("olena");
    dispose();
  });

  it("says who is signed in, and offers exactly one way out", async () => {
    let signedOut = 0;
    const { container, dispose } = mount(() => (
      <AccountMenu name="olena.petrenko" authType="form" onSignOut={() => (signedOut += 1)} />
    ));
    const button = container.querySelector("button")!;
    expect(button.textContent).toContain("Sign out");
    await userEvent.click(button);
    await flush();
    expect(signedOut).toBe(1);
    dispose();
  });

  it("refuses a second click while the first sign-out is still out", async () => {
    // Two logouts are harmless at the gateway, but the second one's answer arrives against a page
    // that is already reloading, and its failure would be reported as though the first had failed.
    let signedOut = 0;
    const { container, dispose } = mount(() => (
      <AccountMenu name="olena.petrenko" busy onSignOut={() => (signedOut += 1)} />
    ));
    await userEvent.click(container.querySelector("button")!);
    await flush();
    expect(signedOut).toBe(0);
    dispose();
  });

  it("says the session is still live when signing out did not work", () => {
    const { container, dispose } = mount(() => (
      <AccountMenu
        name="olena.petrenko"
        failure="Signing out did not work. You are still signed in."
        onSignOut={() => undefined}
      />
    ));
    expect(container.textContent).toContain("still signed in");
    // And the way out is still there to try again. A failure that removed the button would leave
    // the operator with a live session and nothing to press.
    expect(container.querySelector("button")!.textContent).toContain("Sign out");
    dispose();
  });

  it("has no accessibility violations", async () => {
    const { container, dispose } = mount(() => (
      <AccountMenu name="olena.petrenko" authType="form" onSignOut={() => undefined} />
    ));
    const violations = await findViolations(container);
    expect(describeViolations(violations)).toBe("");
    dispose();
  });
});
