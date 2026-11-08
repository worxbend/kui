/**
 * The rendering rules this feature shipped with nothing holding them, closed by mutation.
 *
 * ## How each case below was arrived at, and how to check it
 *
 * Every one names a single edit to a file in `src/`. The edit was applied to the source, the whole
 * of `pnpm -C frontend test packages/feature-clusters packages/feature-schemas` was run, **162 of
 * 162 stayed green**, and the edit was then reverted and the case below written. Re-apply the edit
 * named in a case's comment and that case — and, in the ones that say so, only that case — goes
 * red. That is the measurement this file exists to make repeatable; nothing here is a claim about
 * code that was not run in both directions.
 *
 * ## Why they are all in `.tsx` files
 *
 * Because that is where the survivors are. Twelve of twelve rules in `model.ts` are gated and the
 * rules that decide what a component *draws* were not, which is the same split the previous
 * adversarial pass over this package found and the reason it was swept a second time. A pure
 * function gets a test because calling it is one line; a branch inside JSX needs a mount, a flush
 * and a query, and the rule quietly goes unasserted while the file's header argues for it at
 * length.
 *
 * ## One production repair, in `BrokerList.tsx`
 *
 * The first case is not only a missing test. `totalLeaders` was a `reduce`, `reduce` over an empty
 * list is `0`, and the tile drew a bare `0` for a cluster that named no broker — while the chip
 * written to say `no broker answered` could not run at all, because it is read only when the memo
 * answers `undefined` and only a broker carrying a null count could make it so. The memo now
 * refuses an empty list, which makes the figure honest and the chip reachable in the same edit.
 */
import { describe, expect, it, vi } from "vitest";
import { flush } from "solid-js";
import { KuiProvider, sharedQueries } from "@kui/kernel";
import type { KuiApiClient } from "@kui/api";
import { createRouter, memoryHistory, type RouteSectionProps } from "@solidjs/router";
import { mount, settle, testContext } from "./testing.js";
import { failureOf } from "./BrokersScreen.jsx";
import { BrokerList } from "./BrokerList.jsx";
import { BrokerCard } from "./BrokerCard.jsx";
import { BrokerDetail } from "./BrokerDetail.jsx";
import { BrokersScreen } from "./BrokersScreen.jsx";
import { ClusterList } from "./ClusterList.jsx";
import { ClusterAdmin, type ManagedCluster } from "./ClusterAdmin.jsx";
import Clusters from "./ClustersRoute.jsx";
import { EMPTY_CLUSTER_FORM, HOSTNAME_WARNING, PROPERTIES_WARNING, suggestId } from "./clusterForm.js";
import { SAMPLE_BROKERS, SAMPLE_CLUSTERS, SAMPLE_CONFIGS, SAMPLE_LOG_DIRS } from "./fixtures.js";
import type { Broker } from "./model.js";

const idle = { kind: "idle" } as const;

/** The tile whose label reads `label`, whatever order the tiles are drawn in. */
function tile(container: HTMLElement, label: string): HTMLElement | undefined {
  return [...container.querySelectorAll<HTMLElement>(".kui-tile")].find(
    (one) => one.querySelector(".kui-tile__label")?.textContent === label,
  );
}

function brokerList(brokers: readonly Broker[], loading = false) {
  return mount(() => (
    <BrokerList
      clusterName="prod-kyiv-01"
      brokers={brokers}
      underReplicatedPartitions={0}
      observedAgo="2s ago"
      loading={loading}
      clustersHref="/clusters"
      hrefFor={(id) => `/b/${id}`}
    />
  ));
}

describe("the broker tiles, when the cluster named no broker", () => {
  it("says no broker answered and never draws a zero", async () => {
    /*
     * The repair, and the case that holds it. Restore `totalLeaders` to
     * `props.brokers.reduce(...)` with no length test — the shape this file shipped with — and this
     * case fails on both assertions at once: the figure becomes the string `0` and the chip
     * disappears, because the chip's `no broker answered` arm is only read when the memo is
     * `undefined`.
     *
     * `loading` is deliberately false. While the request is out the tile is `pending` and says
     * nothing, which was already gated; the state nobody had built is the answer that came back
     * naming nothing.
     */
    const { container, dispose } = brokerList([]);
    await flush();
    const leaders = tile(container, "TOTAL LEADERS");
    expect(leaders).toBeDefined();
    expect(leaders?.querySelector(".kui-tile__value")).toBeNull();
    expect(leaders?.textContent).not.toContain("0");
    expect(leaders?.querySelector(".kui-tile__chip")?.textContent).toBe("no broker answered");
    dispose();
  });

  it("still adds up the leaderships when every broker reported one", async () => {
    // The other direction, because a memo that answered `undefined` for everything would pass the
    // case above. 512 + 498 + 526, from the fixture's own three brokers.
    const { container, dispose } = brokerList(SAMPLE_BROKERS);
    await flush();
    expect(tile(container, "TOTAL LEADERS")?.querySelector(".kui-tile__value")?.textContent).toBe(
      "1,536",
    );
    dispose();
  });

  it("says a skew needs two brokers rather than repeating the general caption", async () => {
    /*
     * Mutation: replace the whole chip expression with `{ text: skewCaption(skew()) }`. All 162
     * cases stayed green, and a single-broker cluster then reads "Replicas per broker, as a share
     * of the average." under an em dash — a caption describing a measurement that cannot exist.
     */
    const { container, dispose } = brokerList([SAMPLE_BROKERS[0] as Broker]);
    await flush();
    expect(tile(container, "PARTITION SKEW")?.querySelector(".kui-tile__chip")?.textContent).toBe(
      "needs at least two brokers",
    );
    dispose();
  });
});

describe("a broker card's settings", () => {
  function card(props: {
    readonly configs?: readonly { readonly name: string; readonly value: string | undefined }[];
    readonly configsMore?: number;
  }) {
    return mount(() => (
      <BrokerCard broker={SAMPLE_BROKERS[0] as Broker} expanded {...props} />
    ));
  }

  it("says nothing about more settings when it is showing all of them", async () => {
    /*
     * Mutation: `<Show when={(props.configsMore ?? 0) > 0}>` → `>= 0`. Green over 162 cases, and
     * the card then reads "0 more settings. Open this broker to see all of them." — the bare zero
     * in a sentence that this product's central rule forbids, on every card of every cluster whose
     * brokers report fewer settings than the card draws.
     *
     * The producer of that zero is gated beside it, in `BrokersScreen.moreFor`: see the screen case
     * below. Both ends, because either alone puts the sentence back.
     */
    const { container, dispose } = card({ configs: [{ name: "compression.type", value: "producer" }], configsMore: 0 });
    await flush();
    expect(container.textContent).toContain("compression.type");
    expect(container.textContent).not.toContain("more settings");
    dispose();
  });

  it("draws an em dash for a broker with no rack in a rack-aware cluster", async () => {
    /*
     * Mutation: `{broker().rack === null ? <Unknown what="rack" /> : broker().rack}` →
     * `{broker().rack}`. Green over 162 cases. The gap is the point — the comment beside it says a
     * rack-less broker in a rack-aware cluster "has a gap worth seeing" — and Solid renders `null`
     * as nothing at all, so the figure's value simply disappears and the card reads as though the
     * RACK label had no field under it.
     */
    const { container, dispose } = mount(() => (
      <BrokerCard broker={{ ...(SAMPLE_BROKERS[0] as Broker), rack: null }} showRack expanded />
    ));
    await flush();
    const rack = [...container.querySelectorAll<HTMLElement>(".kui-brkcard__figure")].find(
      (one) => one.querySelector(".kui-brkcard__figure-label")?.textContent === "RACK",
    );
    expect(rack?.textContent).toContain("\u2014");
    expect(rack?.querySelector("[title]")?.getAttribute("title")).toBe("The rack could not be read");
    dispose();
  });

  it("says a broker reported no settings rather than drawing an empty chip row", async () => {
    /*
     * Mutation: `when={(props.configs ?? []).length > 0}` → `>= 0`. Green over 162 cases, and the
     * card then draws the CONFIGURATION heading over nothing at all — which the file's own header
     * says reads as "this broker has no settings", a claim it calls never true. An empty answer and
     * an unread one must not render as the same shape of thing.
     */
    const { container, dispose } = card({ configs: [] });
    await flush();
    expect(container.textContent).toContain("This broker reported no settings.");
    expect(container.querySelector(".kui-config-chips")).toBeNull();
    dispose();
  });
});

describe("a broker's configuration table", () => {
  function detail(configs: readonly (typeof SAMPLE_CONFIGS)[number][]) {
    return mount(() => (
      <BrokerDetail
        broker={SAMPLE_BROKERS[0] as Broker}
        clusterName="prod-kyiv-01"
        clustersHref="/clusters"
        brokersHref="/clusters/prod/brokers"
        logDirs={{ kind: "ready", value: SAMPLE_LOG_DIRS }}
        configuration={{ kind: "ready", value: configs }}
        tab="configuration"
        onTabChange={() => undefined}
      />
    ));
  }

  it("draws a setting with an empty value as an em dash with a reason, and never as a blank cell", async () => {
    /*
     * Mutation: `row.value === null || row.value === ""` → `row.value === null`. Green over 162
     * cases. `unclean.leader.election.enable` — a real Kafka setting that an ordinary broker
     * reports with an empty string — then draws an empty monospaced cell, which is the one
     * rendering this feature refuses everywhere else: a blank says nothing about whether the value
     * was read, and this one *was*.
     */
    const { container, dispose } = detail(SAMPLE_CONFIGS);
    await flush();
    const row = [...container.querySelectorAll("tr")].find((one) =>
      one.textContent?.includes("unclean.leader.election.enable"),
    );
    expect(row).toBeDefined();
    const missing = row?.querySelector(".kui-brk-missing");
    expect(missing).not.toBeNull();
    expect(missing?.getAttribute("title")).toBe("This setting has no value.");
    dispose();
  });
});

describe("a broker's log directories", () => {
  it("draws each bar against the largest directory on the broker", async () => {
    /*
     * Mutation: `Math.max(max, dir.sizeBytes ?? 0)` → `Math.min(...)`. Green over 162 cases,
     * because nothing in this package asserted any bar's *length* — the figure the bar is. With
     * `min` the accumulator never leaves `0`, `share(x, 0)` answers `0` by its own guard, and every
     * directory is drawn empty: the "which of these is the big one" question the header says the
     * bar exists to answer is silently deleted, on a page that still looks finished.
     *
     * The fixture's four directories are 412 GB, 198 GB, 0 and one that did not answer, so the
     * largest is the first: its bar is full and the second's is 198/412.
     */
    const { container, dispose } = mount(() => (
      <BrokerDetail
        broker={SAMPLE_BROKERS[0] as Broker}
        clusterName="prod-kyiv-01"
        clustersHref="/clusters"
        brokersHref="/clusters/prod/brokers"
        logDirs={{ kind: "ready", value: SAMPLE_LOG_DIRS }}
        configuration={{ kind: "loading" }}
        tab="logdirs"
        onTabChange={() => undefined}
      />
    ));
    await flush();
    const fills = [...container.querySelectorAll<HTMLElement>(".kui-magnitude__fill")].map((one) =>
      one.style.width,
    );
    expect(fills.length).toBeGreaterThanOrEqual(3);
    expect(fills[0]).toBe("100%");
    // 198 of 412, to the same precision the component writes it in.
    expect(Number.parseFloat(fills[1] ?? "0")).toBeCloseTo((198 / 412) * 100, 1);
    expect(fills[2]).toBe("0%");
    dispose();
  });
});

describe("the cluster list", () => {
  function list(clusters = SAMPLE_CLUSTERS) {
    return mount(() => (
      <ClusterList clusters={clusters} hrefFor={(id) => `/ui/clusters/${id}/brokers`} />
    ));
  }

  it("draws any under-replication at all as critical, and only zero as normal", async () => {
    /*
     * Mutation: `level={row.underReplicatedPartitions === 0 ? "normal" : "critical"}` →
     * `level="normal"`. Green over 162 cases. The column's own comment calls this "the cheapest
     * possible alarm" and says the threshold is one rather than a round number because
     * under-replication has no comfortable range; with the mutation a cluster with 47 partitions at
     * risk is drawn exactly like a healthy one, and the file header's argument for keeping a column
     * of zeroes — that it turns amber — becomes false.
     */
    const { container, dispose } = list();
    await flush();
    /* prod-kyiv-01 reports 0, staging-fra reports 47, archive-eu reports nothing at all — so the
       column holds two thresholds and one em dash, and `--critical` must be on exactly the 47. */
    const cells = [...container.querySelectorAll<HTMLElement>(".kui-threshold")];
    expect(cells.map((one) => one.textContent?.trim())).toEqual(["0", "47 (partitions under-replicated)"]);
    expect(cells[0]?.classList.contains("kui-threshold--critical")).toBe(false);
    expect(cells[1]?.classList.contains("kui-threshold--critical")).toBe(true);
    // The mark, which is the half of the signal that is not a colour.
    expect(cells[0]?.querySelector(".kui-threshold__mark")).toBeNull();
    expect(cells[1]?.querySelector(".kui-threshold__mark")).not.toBeNull();
    dispose();
  });

  it("marks a broker fraction short when a broker is missing, and not when none is", async () => {
    /*
     * Mutation: drop the conditional class and always write `kui-brk-fraction`. Green over 162
     * cases: the existing case asserts the text `2/3` and never the class, so the one signal that
     * separates "two of three answered" from "three of three" at a glance was carried by nothing.
     */
    const { container, dispose } = list();
    await flush();
    const fractions = [...container.querySelectorAll<HTMLElement>(".kui-brk-fraction")];
    const short = fractions.filter((one) => one.classList.contains("kui-brk-fraction--short"));
    expect(fractions.map((one) => one.textContent)).toEqual(["3/3", "2/3"]);
    expect(short.map((one) => one.textContent)).toEqual(["2/3"]);
    dispose();
  });

  it("leaves a modifier-click to the browser rather than routing it", async () => {
    /*
     * Mutation: delete
     * `if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;`. Green over
     * 162 cases, and ⌘-click, ctrl-click and middle-of-the-road "open in a new tab" then call
     * `preventDefault()` and navigate the current tab instead — the browser affordance the anchor
     * exists to keep.
     */
    const opened: string[] = [];
    const { container, dispose } = mount(() => (
      <ClusterList
        clusters={SAMPLE_CLUSTERS}
        hrefFor={(id) => `/ui/clusters/${id}/brokers`}
        onOpen={(id) => opened.push(id)}
      />
    ));
    await flush();
    const link = container.querySelector<HTMLAnchorElement>(".kui-brk-name__link");
    expect(link).not.toBeNull();

    /* `defaultPrevented` is the whole assertion, and it is deliberately not "the router was not
       called". The anchor's guard returns *before* `stopPropagation()`, so the row underneath —
       `DataTable`'s `onRowClick`, which checks no modifier at all — still fires and the current tab
       still navigates. That is a live defect in the kernel's table rather than in this file, it is
       reported rather than repaired here, and a case that asserted `opened` would be asserting the
       defect instead of the rule. */
    const withMeta = new MouseEvent("click", { bubbles: true, cancelable: true, metaKey: true });
    link?.dispatchEvent(withMeta);
    await flush();
    expect(withMeta.defaultPrevented).toBe(false);

    const plain = new MouseEvent("click", { bubbles: true, cancelable: true });
    link?.dispatchEvent(plain);
    await flush();
    expect(opened).toContain("prod-kyiv-01");
    expect(plain.defaultPrevented).toBe(true);
    dispose();
  });
});

describe("managing which clusters KUI knows about", () => {
  const stored: ManagedCluster = {
    id: "spare",
    name: "spare",
    bootstrapServers: "spare:9092",
    readOnly: false,
    origin: "stored" as const,
    version: 4,
    security: { protocol: "PLAINTEXT", mechanism: null },
  };
  const fromTheFile: ManagedCluster = { ...stored, id: "from-file", name: "from-file", origin: "static" };
  const bothWaysRound: ManagedCluster = { ...stored, id: "both", name: "both", origin: "staticThenStored" };

  function admin(
    clusters: readonly ManagedCluster[],
    editing?: { readonly id: string | undefined; readonly form: typeof EMPTY_CLUSTER_FORM },
    connectivity?: { readonly reachable: boolean; readonly status: string; readonly detail?: string },
    onFormChange: (form: typeof EMPTY_CLUSTER_FORM) => void = () => undefined,
  ) {
    return mount(() => (
      <ClusterAdmin
        clusters={clusters}
        editing={editing}
        onAdd={() => undefined}
        onEdit={() => undefined}
        onCancel={() => undefined}
        onFormChange={onFormChange}
        onSave={() => undefined}
        onTest={() => undefined}
        onDelete={() => undefined}
        connectivity={connectivity}
        saveState={idle}
        testState={idle}
        deleteState={idle}
      />
    ));
  }

  const labels = (container: HTMLElement): string[] =>
    [...container.querySelectorAll("button")].map((one) => (one.textContent ?? "").trim());

  /**
   * The sentence under a disabled control.
   *
   * `Button` states the reason through a `Tooltip` that attaches on focus, not as text inside the
   * button — which is the arrangement that makes a disabled control explain itself to a keyboard
   * user rather than only to a mouse. Reading it the way the operator gets it is the only reading
   * that proves the reason is actually reachable.
   */
  async function reasonUnder(button: HTMLButtonElement): Promise<string> {
    button.dispatchEvent(new FocusEvent("focusin", { bubbles: true }));
    await flush();
    const id = button.getAttribute("aria-describedby") ?? "";
    const bubble = id === "" ? null : document.getElementById(id);
    if (bubble === null) throw new Error(`no reason is attached to ${button.textContent ?? ""}`);
    return bubble.textContent ?? "";
  }

  it("lists every reason the form cannot be saved, and not only the first", async () => {
    /*
     * Filed by W7-A3's third pass. Mutation: `<Show when={problems().length > 0}>` around the
     * problem list → `when={false}`. Green over 115 of 115 in this package, because every case in
     * it that touches the form either has a valid one or reads the Save button's tooltip.
     *
     * The Save button's `disabledReason` is `problems()[0]` — one reason — so with the list gone an
     * operator who left the name, the brokers and the username empty is told about the name,
     * fixes it, and is told about the brokers. The comment above the list is the rule: *"Every
     * reason at once. Somebody who got three fields wrong should be told about all three rather
     * than discovering them one save at a time."* Three saves to learn three facts the form
     * already knew is the shape this list exists to prevent.
     */
    const broken = {
      ...EMPTY_CLUSTER_FORM,
      protocol: "SASL_SSL",
      mechanism: "SCRAM-SHA-512",
    };
    const { container, dispose } = admin([stored], { id: undefined, form: broken });
    await flush();

    const listed = [...container.querySelectorAll(".kui-cluster-admin__problems li")].map((one) =>
      (one.textContent ?? "").trim(),
    );
    // Four fields are wrong at once, and all four are on screen together.
    expect(listed).toContain("A name is required.");
    expect(listed).toContain("At least one broker address is required.");
    expect(listed).toContain("A SASL connection needs a username.");
    expect(listed).toContain("A SASL connection needs a password.");
    expect(listed.length).toBeGreaterThan(1);

    // And the Save button's own reason is one of them rather than a different sentence — the
    // tooltip is the summary, the list is the account.
    const save = [...container.querySelectorAll("button")].find(
      (one) => (one.textContent ?? "").trim() === "Save",
    ) as HTMLButtonElement;
    expect(listed).toContain((await reasonUnder(save)).trim());
    dispose();
  });

  it("shows no problem list at all for a form that has none", async () => {
    // The other direction: a permanently present empty list is the marker nobody reads.
    const complete = {
      ...EMPTY_CLUSTER_FORM,
      id: "spare-2",
      name: "spare 2",
      bootstrapServers: "spare-2:9092",
    };
    const { container, dispose } = admin([stored], { id: undefined, form: complete });
    await flush();
    expect(container.querySelector(".kui-cluster-admin__problems")).toBeNull();
    dispose();
  });

  it("offers no Edit for a cluster the deployment's configuration file defines", async () => {
    /*
     * Mutation: `when={isEditable(cluster.origin)}` → `when={true}`. Green over 162 cases, although
     * `isEditable` itself is gated as a function — which is exactly the split this sweep is about:
     * the rule was tested where it is *computed* and not where it is *applied*, so the screen could
     * stop calling it and nothing would notice.
     */
    const { container, dispose } = admin([fromTheFile]);
    await flush();
    expect(container.textContent).toContain("defined in this deployment's configuration file");
    expect(labels(container)).not.toContain("Edit");
    dispose();
  });

  it("offers Remove only for a cluster the file does not also name", async () => {
    /*
     * Mutation: `when={isRemovable(cluster.origin)}` → `when={true}`. Green over 162 cases. A
     * `staticThenStored` cluster is editable and not removable — deleting the stored record leaves
     * the file's row on screen, which reads as a delete that silently failed — and the screen says
     * so before the click. With the mutation the danger button is live and the sentence is gone.
     */
    const both = admin([bothWaysRound]);
    await flush();
    /* `aria-disabled`, not the `disabled` attribute: `Button`'s own header explains that a disabled
       control stays focusable so that a keyboard user can reach it and read the reason. */
    const disabled = [...both.container.querySelectorAll<HTMLButtonElement>("button")].find(
      (one) => (one.textContent ?? "").trim() === "Remove",
    );
    expect(disabled?.getAttribute("aria-disabled")).toBe("true");
    expect(await reasonUnder(disabled as HTMLButtonElement)).toContain(
      "Remove it from the file instead.",
    );
    both.dispose();

    const only = admin([stored]);
    await flush();
    const live = [...only.container.querySelectorAll<HTMLButtonElement>("button")].find(
      (one) => (one.textContent ?? "").trim() === "Remove",
    );
    expect(live?.getAttribute("aria-disabled")).toBeNull();
    only.dispose();
  });

  it("draws a connection test that failed as a warning and never as good news", async () => {
    /*
     * Mutation: `tone={result().reachable ? "info" : "warning"}` → `tone="info"`. Green over 162
     * cases: nothing mounted this banner at all. The two outcomes then differ only in the sentence,
     * and the screen's whole reason for testing before saving is that somebody reads the answer.
     */
    const bad = admin(
      [stored],
      { id: undefined, form: EMPTY_CLUSTER_FORM },
      { reachable: false, status: "UNREACHABLE", detail: "no route to host" },
    );
    await flush();
    const banner = bad.container.querySelector(".kui-banner");
    expect(banner?.className).toContain("kui-banner--warning");
    expect(banner?.textContent).toContain("no route to host");
    bad.dispose();

    const good = admin(
      [stored],
      { id: undefined, form: EMPTY_CLUSTER_FORM },
      { reachable: true, status: "OK" },
    );
    await flush();
    expect(good.container.querySelector(".kui-banner")?.className).toContain("kui-banner--info");
    good.dispose();
  });

  it("leaves an existing cluster's id alone while its name is retyped", async () => {
    /*
     * Mutation: replace the whole spread with `...{ id: suggestId(name) }`. Green over 162 cases,
     * and renaming a registered cluster then silently moves its id — which is in every link, every
     * bookmark and every audit line for it, as the help text two lines below says. The form even
     * goes on rendering that help text while the id under it changes.
     */
    const seen: (typeof EMPTY_CLUSTER_FORM)[] = [];
    const existing = { ...EMPTY_CLUSTER_FORM, id: "spare", name: "spare" };
    const { container, dispose } = admin(
      [stored],
      { id: "spare", form: existing },
      undefined,
      (form) => seen.push(form),
    );
    await flush();
    const name = [...container.querySelectorAll<HTMLInputElement>("input")].find(
      (one) => one.value === "spare",
    );
    expect(name).toBeDefined();
    name!.value = "Spare cluster";
    name!.dispatchEvent(new Event("input", { bubbles: true }));
    await flush();
    expect(seen).toHaveLength(1);
    expect(seen[0]?.name).toBe("Spare cluster");
    expect(seen[0]?.id).toBe("spare");
    // And the id the mutation would have written, so the case cannot pass by the suggestion being
    // equal to what was there.
    expect(suggestId("Spare cluster")).toBe("spare-cluster");
    dispose();
  });

  it("will not save over a cluster whose truststore this form cannot keep", async () => {
    /*
     * No confirmation has been given. The row already says a truststore is configured. Save used
     * to be enabled and the request carried neither store, so the next PUT removed it.
     */
    const withStore: ManagedCluster = {
      ...stored,
      id: "edge",
      name: "Edge",
      security: { protocol: "SSL", mechanism: null, truststoreConfigured: true, keystoreConfigured: false },
    };
    const form = {
      ...EMPTY_CLUSTER_FORM,
      id: "edge",
      name: "Edge",
      bootstrapServers: "10.0.0.8:9093",
      protocol: "SSL",
    };
    const { container, dispose } = admin([withStore], { id: "edge", form });
    await flush();
    expect(container.textContent).toMatch(/truststore/i);
    const save = [...container.querySelectorAll("button")].find(
      (one) => (one.textContent ?? "").trim() === "Save",
    ) as HTMLButtonElement;
    expect(save.getAttribute("aria-disabled")).toBe("true");
    expect(await reasonUnder(save)).toMatch(/truststore/i);
    dispose();

    // A keystore alone is the same refusal. The flags are independent on the read model.
    const withKey: ManagedCluster = {
      ...withStore,
      security: { protocol: "SSL", mechanism: null, truststoreConfigured: false, keystoreConfigured: true },
    };
    const keyOnly = admin([withKey], { id: "edge", form });
    await flush();
    const keySave = [...keyOnly.container.querySelectorAll("button")].find(
      (one) => (one.textContent ?? "").trim() === "Save",
    ) as HTMLButtonElement;
    expect(keySave.getAttribute("aria-disabled")).toBe("true");
    expect(await reasonUnder(keySave)).toMatch(/keystore/i);
    keyOnly.dispose();
  });

  it("says an edit is showing defaults for hostname verification and properties", async () => {
    /*
     * The public read model carries neither. The checkbox used to render pre-ticked with no
     * sentence, and Save wrote `verifyHostname: true` and `properties: {}`.
     */
    const ssl: ManagedCluster = { ...stored, security: { protocol: "SSL", mechanism: null } };
    const form = {
      ...EMPTY_CLUSTER_FORM,
      id: "spare",
      name: "spare",
      bootstrapServers: "spare:9092",
      protocol: "SSL",
    };
    const { container, dispose } = admin([ssl], { id: "spare", form });
    await flush();
    expect(container.textContent).toContain(HOSTNAME_WARNING);
    expect(container.textContent).toContain(PROPERTIES_WARNING);
    const box = [...container.querySelectorAll("input")].find(
      (one) => one.getAttribute("type") === "checkbox" && one.checked,
    );
    expect(box).toBeDefined();
    dispose();
  });

  it("will not save a rename whose slug is a different cluster", async () => {
    /*
     * The id stays (the case above). What used to happen next is that Save was enabled: the name
     * still matched the id pattern, the connection test derived its id from the name, and the
     * server then refused the write. The problem has to be on screen before either call.
     */
    const existing = {
      ...EMPTY_CLUSTER_FORM,
      id: "orders-prod",
      name: "Orders Production",
      bootstrapServers: "b:9092",
    };
    const { container, dispose } = admin([stored], { id: "orders-prod", form: existing });
    await flush();
    const listed = [...container.querySelectorAll(".kui-cluster-admin__problems li")].map((one) =>
      (one.textContent ?? "").trim(),
    );
    expect(listed.some((line) => line.includes("orders-production") && line.includes("orders-prod"))).toBe(
      true,
    );
    const save = [...container.querySelectorAll("button")].find(
      (one) => (one.textContent ?? "").trim() === "Save",
    );
    expect(save?.getAttribute("aria-disabled")).toBe("true");
    dispose();
  });
});

describe("the brokers screen", () => {
  const fetchedAt = "2026-09-06T09:00:00.000Z";
  const ok = (data: unknown) => ({ status: "ok", data, fetchedAt });

  function gateway(answers: Readonly<Record<string, unknown>>) {
    const asked: string[] = [];
    const get = vi.fn(async (path: string) => {
      asked.push(path);
      const answer = answers[path];
      return answer === undefined
        ? { ok: false, error: { kind: "unreachable", cause: `nothing stubbed for ${path}` } }
        : { ok: true, value: answer };
    });
    return { asked, api: { get, post: get, put: get, delete: get, patch: get, raw: {} } as unknown as KuiApiClient };
  }

  /**
   * Exactly as many settings as the card draws, and the number is read from the product rather
   * than repeated here.
   *
   * `moreFor` computes `answer.length - CHIPS_PER_CARD`, so the state that separates
   * `rest > 0 ? rest : undefined` from `rest` is `rest === 0` — a broker with precisely a cardful.
   * One fewer and the mutation produces a negative, which the card's own `> 0` swallows, and the
   * case would pass with the rule deleted. This is the shape wave 5 was told to stop writing: a
   * bound whose case computes its own input from the bound, only inverted — so the count is taken
   * from the card's rendered chips instead of from a literal, and asserted to be the whole answer.
   */
  const A_CARDFUL = Array.from({ length: 8 }, (_, index) => ({
    name: `broker.setting.${index}`,
    value: String(index),
    source: "default",
  }));

  it("offers no 'more settings' line for a broker whose settings all fit on the card", async () => {
    /*
     * Mutation: `return rest > 0 ? rest : undefined;` → `return rest;`.
     *
     * **Re-measured by W7-A3, and the sentence that used to be here was wrong.** It claimed that
     * mutation reddens this case. It does not: `moreFor` answers `0` for a broker with exactly a
     * cardful and a negative for one with fewer, and `BrokerCard`'s own `(props.configsMore ?? 0) >
     * 0` swallows both — so the two guards are not two halves of one rule, they are one rule held
     * twice, and either alone keeps "0 more settings" off the card. The mutation that does redden
     * this case is the card's guard, `> 0` → `>= 0`, which is the case above.
     *
     * That is disclosed rather than repaired. Deleting the screen's guard would leave the rule
     * standing on one line in a component three screens share, and redundant depth is worth having
     * where it is measured and said out loud — house rule 17 asks for the measurement, not for the
     * line.
     */
    const { asked, api } = gateway({
      "/api/v1/clusters/{clusterId}/brokers": {
        brokers: ok([{ id: 1, host: "kafka", port: 9092, rack: null, isController: true, leaderCount: 86, replicaCount: 86 }]),
      },
      "/api/v1/clusters/{clusterId}": {
        cluster: { id: "quickstart", name: "Quickstart", readOnly: false, bootstrapServers: "kafka:9092", summary: ok({ version: "4.3", controllerKind: "kraft", brokerCount: 1, underReplicatedPartitionCount: 0, scrapedAt: fetchedAt }) },
      },
      "/api/v1/clusters/{clusterId}/log-dirs": { logDirs: ok([]) },
      "/api/v1/clusters/{clusterId}/brokers/{brokerId}/configs": { configs: ok(A_CARDFUL) },
    });
    sharedQueries.invalidateWhere(() => true);
    const { container, dispose } = mount(() => (
      <KuiProvider value={testContext(api)}>
        <BrokersScreen clusterId="quickstart" clustersHref="/ui/clusters" hrefFor={(id) => `/b/${id}`} now={() => new Date(fetchedAt)} />
      </KuiProvider>
    ));
    try {
      await settle(container);
      (container.querySelector('[data-testid="broker-1"] .kui-brkcard__toggle') as HTMLButtonElement).click();
      await settle(container);
      expect(asked.filter((path) => path.endsWith("/configs"))).toHaveLength(1);
      // Every setting the broker answered is on the card, so there is nothing left to be "more".
      expect(container.querySelectorAll(".kui-config-chip")).toHaveLength(A_CARDFUL.length);
      expect(container.textContent).not.toContain("more settings");
    } finally {
      dispose();
    }
  });
});

describe("how old the brokers page says its picture is", () => {
  const fetchedAt = "2026-09-06T09:00:00.000Z";
  const ok = (data: unknown) => ({ status: "ok", data, fetchedAt });

  function gateway(answers: Readonly<Record<string, unknown>>) {
    const get = vi.fn(async (path: string) => {
      const answer = answers[path];
      return answer === undefined
        ? { ok: false, error: { kind: "unreachable", cause: `nothing stubbed for ${path}` } }
        : { ok: true, value: answer };
    });
    return { get, post: get, put: get, delete: get, patch: get, raw: {} } as unknown as KuiApiClient;
  }

  /** `scrapedAt` omitted entirely is how this decoder is told the scrape did not say when. */
  function answers(scrapedAt: string | undefined): Readonly<Record<string, unknown>> {
    return {
      "/api/v1/clusters/{clusterId}/brokers": {
        brokers: ok([
          { id: 1, host: "kafka", port: 9092, rack: null, isController: true, leaderCount: 86, replicaCount: 86 },
        ]),
      },
      "/api/v1/clusters/{clusterId}": {
        cluster: {
          id: "quickstart",
          name: "Quickstart",
          readOnly: false,
          bootstrapServers: "kafka:9092",
          summary: ok({
            version: "4.3",
            controllerKind: "kraft",
            brokerCount: 1,
            underReplicatedPartitionCount: 0,
            ...(scrapedAt === undefined ? {} : { scrapedAt }),
          }),
        },
      },
      "/api/v1/clusters/{clusterId}/log-dirs": { logDirs: ok([]) },
    };
  }

  function screen(scrapedAt: string | undefined) {
    sharedQueries.invalidateWhere(() => true);
    const api = gateway(answers(scrapedAt));
    return mount(() => (
      <KuiProvider value={testContext(api)}>
        <BrokersScreen clusterId="quickstart" clustersHref="/ui/clusters" hrefFor={(id) => `/b/${id}`} now={() => new Date(fetchedAt)} />
      </KuiProvider>
    ));
  }

  it("says nothing at all when the scrape did not say when it happened", async () => {
    /*
     * Filed by W7-A3's third pass. Mutation: `if (at === null || at === undefined) return
     * undefined;` → `return "unknown";` in `BrokersScreen.observedAgo`. Green over 114 of 114 in
     * this package, because every fixture carries a `scrapedAt`.
     *
     * `observedAgo`'s own comment states the rule and nothing held it: *"Absent rather than
     * 'unknown' when the scrape did not say: the freshness line under the cards exists to make a
     * page that does not refresh acceptable, and a line reading 'Read unknown ago' does the
     * opposite of that."* With the mutation the page prints **Read unknown ago. Nothing on this
     * page refreshes on its own.** — a sentence whose only content is that KUI cannot date what is
     * on the screen, in the line placed there to reassure the reader it can.
     */
    const missing = screen(undefined);
    try {
      await settle(missing.container);
      expect(missing.container.querySelector('[data-testid="brokers-freshness"]')).toBeNull();
      expect(missing.container.textContent).not.toContain("unknown ago");
    } finally {
      missing.dispose();
    }

    // And a scrape that did say when it happened still dates the page, so the guard declines an
    // absence rather than the line.
    const dated = screen("2026-09-06T08:59:36.000Z");
    try {
      await settle(dated.container);
      const line = dated.container.querySelector('[data-testid="brokers-freshness"]');
      expect(line?.textContent).toContain("Read");
      expect(line?.textContent).toContain("Nothing on this page refreshes on its own.");
      expect(line?.textContent).not.toContain("unknown");
    } finally {
      dated.dispose();
    }
  });
});

describe("one broker's own page, reached through the route", () => {
  const fetchedAt = "2026-09-06T09:00:00.000Z";
  const ok = (data: unknown) => ({ status: "ok", data, fetchedAt });
  const ANSWERS: Readonly<Record<string, unknown>> = {
    "/api/v1/clusters/{clusterId}/brokers": {
      brokers: ok([{ id: 1, host: "kafka", port: 9092, rack: null, isController: true, leaderCount: 86, replicaCount: 86 }]),
    },
    "/api/v1/clusters/{clusterId}/log-dirs": {
      logDirs: ok([{ brokerId: 1, path: "/tmp/kafka-logs", error: null, totalBytes: 1_000, usableBytes: 400, partitionCount: 58, replicas: [] }]),
    },
    "/api/v1/clusters/{clusterId}/brokers/{brokerId}/configs": {
      configs: ok([{ name: "compression.type", value: "producer", source: "default" }]),
    },
  };

  /**
   * The real route over a real router, because the rule below is the route's and not a component's.
   *
   * A case that mounts `BrokerDetail` and hands it a `configuration` prop has composed the laziness
   * it means to assert; only the route decides whether the request is made, and only a router can
   * put the tab in the address the route reads it from.
   */
  function host(at: string, answers: Readonly<Record<string, unknown>> = ANSWERS) {
    /*
     * `sharedQueries` is module state by design — two components asking for one key make one
     * request — so it outlives a mount and, in a test file, a case. Without this the second address
     * below is served the first one's answers and reports that the screen asked for nothing at all,
     * which is a green case about the cache rather than about the rule.
     */
    sharedQueries.invalidateWhere(() => true);
    const asked: string[] = [];
    const get = vi.fn(async (path: string) => {
      asked.push(path);
      const answer = answers[path];
      return answer === undefined
        ? { ok: false, error: { kind: "unreachable", cause: `nothing stubbed for ${path}` } }
        : { ok: true, value: answer };
    });
    const api = { get, post: get, put: get, delete: get, patch: get, raw: {} } as unknown as KuiApiClient;
    const history = memoryHistory(at);
    const Router = createRouter({
      /* The patterns carry `/ui` rather than the router carrying a base, because `testContext`'s
         `paths.broker()` answers `/ui/clusters/…` and the tab writer hands that string straight to
         `navigate`. Spelling the prefix in one place keeps what the route matches and what the
         product builds the same string, so this case is about the query and nothing else. */
      routes: [
        { path: "/ui/clusters", component: () => Clusters() },
        { path: "/ui/clusters/:clusterId/brokers", component: () => Clusters() },
        { path: "/ui/clusters/:clusterId/brokers/:brokerId", component: () => Clusters() },
        { path: "*", component: () => null },
      ],
      history,
    });
    const mounted = mount(() => (
      <KuiProvider value={testContext(api)}>
        <Router>{(route: RouteSectionProps) => route.children}</Router>
      </KuiProvider>
    ));
    /*
     * Two settles with a macrotask between them.
     *
     * `settle` drains Solid's queue and returns as soon as the markup stops changing, which on this
     * page happens once the *route* has resolved — before the second round of fetches the tab panel
     * starts. A case that settled once would report "no request was made" for both addresses and
     * pass for a reason that has nothing to do with the rule.
     */
    const quiet = async (): Promise<void> => {
      await settle(mounted.container);
      await new Promise((resolve) => setTimeout(resolve, 0));
      await settle(mounted.container);
    };
    return { ...mounted, asked, quiet, url: () => history.get() };
  }

  it("does not ask for a broker's settings until the configuration tab is in the address", async () => {
    /*
     * Mutation: make the `configs` query's key unconditional —
     * `key: () => \`clusters/${'$'}{props.clusterId}/brokers/${'$'}{props.brokerId}/configs\`` — instead of
     * `undefined` until `tab() === "configuration"`. Green over 162 cases, and every visit to a
     * broker's disks then pays for `describeConfigs`, which two file headers in this package
     * measure at **340 rows and 61,531 bytes** on the quickstart's own broker.
     *
     * The card-level twin of this rule is gated in `clusters.test.tsx`; the *page's* was not, and
     * they are different code — one is a `<For>` over expanded ids, the other is a key that is
     * `undefined` until the address says otherwise.
     */
    const bare = host("/ui/clusters/quickstart/brokers/1");
    await bare.quiet();
    expect(bare.container.querySelector('[data-testid="broker-detail"]')).not.toBeNull();
    expect(bare.asked).toContain("/api/v1/clusters/{clusterId}/log-dirs");
    expect(bare.asked).not.toContain("/api/v1/clusters/{clusterId}/brokers/{brokerId}/configs");
    bare.dispose();

    const onTheTab = host("/ui/clusters/quickstart/brokers/1?tab=configuration");
    await onTheTab.quiet();
    expect(onTheTab.asked).toContain("/api/v1/clusters/{clusterId}/brokers/{brokerId}/configs");
    onTheTab.dispose();
  });

  it("keeps the default tab off the address, so one view has one spelling", async () => {
    /*
     * Mutation: `navigate(next === "logdirs" ? here : \`${here}?tab=${next}\`)` → always append the
     * query. Green over 162 cases, and `/brokers/1` and `/brokers/1?tab=logdirs` then become two
     * addresses for one view — so the link one engineer sends another depends on whether they
     * arrived directly or came back from the other tab, and Back walks through both.
     */
    const page = host("/ui/clusters/quickstart/brokers/1?tab=configuration");
    await page.quiet();
    const logDirs = [...page.container.querySelectorAll<HTMLElement>('[role="tab"]')].find(
      (one) => one.textContent === "Log directories",
    );
    logDirs?.click();
    await page.quiet();
    expect(page.url()).toBe("/ui/clusters/quickstart/brokers/1");

    const configuration = [...page.container.querySelectorAll<HTMLElement>('[role="tab"]')].find(
      (one) => one.textContent === "Configuration",
    );
    configuration?.click();
    await page.quiet();
    expect(page.url()).toBe("/ui/clusters/quickstart/brokers/1?tab=configuration");
    page.dispose();
  });

  it("keeps a deployment that configured nothing apart from a service that broke", async () => {
    /*
     * Mutation: give `loadedOf`'s `not-configured` arm the code `FAILED`, or merge it into the
     * `failed` arm. Green over 162 cases. The two are not interchangeable — one is a deployment
     * that never had the thing and the other is one whose service is down — and the stable code is
     * what an operator escalates with. `loadedOf` is private to the route, so only a case that goes
     * through the route can see it.
     */
    /* Distinct cluster ids, because `sharedQueries` keys on the address and *invalidating* a key
       marks it stale rather than evicting it — a second host on the same id would be handed the
       first one's answer and then asserted against. */
    const notConfigured = host("/ui/clusters/never-configured/brokers/1", {
      ...ANSWERS,
      "/api/v1/clusters/{clusterId}/log-dirs": { logDirs: { status: "not_configured" } },
    });
    await notConfigured.quiet();
    const card = notConfigured.container.querySelector('[data-testid="broker-logdirs"]');
    expect(card?.textContent).toContain("This deployment has not configured it.");
    expect(card?.textContent).toContain("NOT_CONFIGURED");
    notConfigured.dispose();

    const broken = host("/ui/clusters/upstream-down/brokers/1", {
      ...ANSWERS,
      "/api/v1/clusters/{clusterId}/log-dirs": {
        /* The wire shape `decodeSection` really reads: `reason` is the stable code as a string and
           the sentence is its sibling `message`. Written the way the gateway writes it, so that a
           change to either side shows up here. */
        logDirs: { status: "unavailable", reason: "KUI-UPSTREAM-UNAVAILABLE", message: "The cluster service did not answer." },
      },
    });
    await broken.quiet();
    const failed = broken.container.querySelector('[data-testid="broker-logdirs"]');
    expect(failed?.textContent).toContain("KUI-UPSTREAM-UNAVAILABLE");
    expect(failed?.textContent).not.toContain("NOT_CONFIGURED");
    broken.dispose();
  });
});

describe("the three refusals a screen's failure panel keeps apart", () => {
  it("gives each one its own stable code, and gives a refusal no retryable one", () => {
    /*
     * Mutation: change `code: "FORBIDDEN"` to `"FAILED"` in `failureOf`'s forbidden arm, or merge
     * the two arms. Green over 162 cases: `failureOf` is exported from `BrokersScreen.tsx`, is read
     * by three screens, and had no case of its own — the code it hands the panel is the string an
     * operator escalates with, and three different situations sharing one is how an afternoon gets
     * spent on the wrong upstream.
     */
    const reload = (): void => undefined;
    expect(failureOf({ kind: "failed", message: "gone", code: "KUI-UPSTREAM-UNAVAILABLE" }, reload)).toEqual({
      message: "gone",
      code: "KUI-UPSTREAM-UNAVAILABLE",
      onRetry: reload,
    });
    expect(failureOf({ kind: "forbidden" }, reload)?.code).toBe("FORBIDDEN");
    expect(failureOf({ kind: "not-configured" }, reload)?.code).toBe("NOT_CONFIGURED");
    // A refusal's sentence tells the operator not to press Retry; the codes are what separate them.
    expect(failureOf({ kind: "forbidden" }, reload)?.message).toContain("permission");
    expect(failureOf({ kind: "not-configured" }, reload)?.message).toContain("no cluster configured");
    // And the two states that are not failures answer nothing at all.
    expect(failureOf({ kind: "loading" }, reload)).toBeUndefined();
    expect(failureOf({ kind: "ready", value: 1 }, reload)).toBeUndefined();
  });
});
