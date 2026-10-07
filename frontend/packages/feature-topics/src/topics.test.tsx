/**
 * The topic list and the topic page frame.
 *
 * The cases here are the ones that are easy to get wrong in a way nobody notices: a dash drawn as a
 * zero, a count that describes a filtered table as if it were the whole cluster, a destructive
 * action that looks like an ordinary one, and a forbidden action that has been hidden rather than
 * explained.
 *
 * ## Three of the seven suites carry an explicit budget, and which three is measured rather than
 * guessed
 *
 * Vitest's default is five seconds a case. Every case in `the topics screen, wired`, `the export,
 * and the sentence above the list` and `a destructive success says so` waits on **real timers** —
 * `settle()` is eight `setTimeout(0)` hops through the event loop and the create-poll case sleeps
 * 700 ms — so their wall time is set by what else the machine is doing rather than by the work they
 * do. The other four suites mount a component and `flush()` and await no timer at all, so they are
 * left on the default.
 *
 * The figures, and where to read them back: `vitest run packages/feature-topics
 * --reporter=verbose` prints a duration per case, and on an idle machine the whole file is about
 * four seconds with exactly one case over 100 ms — `a created topic is waited for until the list
 * can see it`, at 772 ms. Wave 6's brief reports 7,829 ms for `the consumers tab prints host:port`
 * under whole-repository load, a case that measures 23 ms alone here; nothing in this tree
 * reproduces that reading, which is the reason the budget below is generous rather than tuned to
 * it. Thirty seconds is roughly four times the worst dilation anyone has recorded and still fails a
 * genuinely stuck case while somebody is watching.
 */

import { afterEach, describe, expect, test, vi } from "vitest";
import { createSignal, flush } from "solid-js";
import type { JSX } from "@solidjs/web";
import {
  clearToasts,
  sharedQueries,
  formatBytes as kernelFormatBytes,
  toasts,
  type KuiContextValue,
} from "@kui/kernel";
import { mount } from "./testing.js";
import {
  DEFAULT_TOPIC_QUERY,
  TopicListPage,
  formatBytes,
  matchCount,
  queryFromAddress,
  rememberView,
  storedView,
  type TopicListPageProps,
  type TopicListQuery,
} from "./TopicListPage.jsx";
import { TopicCards } from "./TopicCards.jsx";
import { TopicConsumers } from "./TopicConsumers.jsx";
import { TopicPage, healthChip } from "./TopicPage.jsx";
import { TopicStatisticsRegion } from "./TopicStatisticsRegion.jsx";
import {
  forgetQueries,
  restoreMeasuredRows,
  settle,
  topicsHost,
  withMeasuredRows,
  type StubRequest,
} from "./harness.jsx";
import { topicsCsv, topicsVoice } from "./topicList.js";
import { bulkSentence, pollUntilListed, toTopicQuery } from "./TopicsRoute.jsx";
import type { TopicRow } from "./types.js";

/**
 * The budget for a suite whose cases wait on real timers. See the note at the top of this file for
 * the measurements behind it; it is a ceiling on a stuck case, not a target any case comes near.
 */
const THIRTY_SECONDS = 30_000;

const rows: readonly TopicRow[] = [
  {
    name: "orders.payments.v2",
    internal: false,
    partitions: 12,
    replicationFactor: 3,
    health: "in-sync",
    records: 18442901,
    bytes: 128_000_000_000,
    cleanupPolicy: "delete",
  },
  {
    name: "__consumer_offsets",
    internal: true,
    partitions: 50,
    replicationFactor: 3,
    health: "in-sync",
    records: 12,
    bytes: 4096,
  },
  {
    // The topic KUI could not describe. Its figures are genuinely unknown, which is not zero.
    name: "shipments.v1",
    internal: false,
    partitions: 6,
    replicationFactor: 2,
    health: "unknown",
  },
];

/**
 * A controlled list, with somewhere for its requests to go.
 *
 * The page no longer decides which topics exist: it draws the rows it is handed and *asks* for a
 * different set. So the tests below assert what it asks for, which is the behaviour that is now
 * true of a cluster of any size — the old ones asserted filtering that was correct for one page and
 * wrong for four thousand topics.
 */
function listing(overrides: Partial<TopicListPageProps> = {}): {
  readonly asked: TopicListQuery[];
  readonly node: JSX.Element;
} {
  const asked: TopicListQuery[] = [];
  const [query, setQuery] = createSignal<TopicListQuery>(DEFAULT_TOPIC_QUERY);
  return {
    asked,
    node: (
      <TopicListPage
        topics={rows}
        onOpen={() => undefined}
        viewportHeight={480}
        query={query()}
        onQueryChange={(next) => {
          asked.push(next);
          setQuery(next);
        }}
        {...overrides}
      />
    ),
  };
}

describe("the topic list", () => {
  test("the Internal chip changes the request rather than filtering a page", async () => {
    /*
     * The control used to filter rows the page already held — and the server excludes Kafka's
     * bookkeeping topics by default, so the data it filtered had never contained one and the
     * checkbox could not do anything at all. The chip that replaced it changes the *query*, which
     * `toTopicQuery` turns into `showInternal` (asserted in `write.test.ts`).
     */
    const list = listing();
    const { container, dispose } = mount(() => list.node);
    await flush();

    const internal = [...container.querySelectorAll("button")].find(
      (button) => button.textContent?.trim() === "Internal",
    );
    internal?.click();
    await flush();

    expect(list.asked.at(-1)?.facet).toBe("internal");
    dispose();
  });

  test("a chip the cluster cannot apply says that it narrowed the page", async () => {
    /*
     * The honest half of the four-chip bar. KUI's topic index has no column for compaction, so the
     * chip filters the rows the server sent — and a filter that narrows a page while looking like
     * it narrows a cluster is the exact defect the server-side search box was rebuilt to remove.
     */
    const list = listing({ query: { ...DEFAULT_TOPIC_QUERY, facet: "compacted" } });
    const { container, dispose } = mount(() => list.node);
    await flush();
    expect(container.textContent).toContain("narrows the 3 topics on this page");
    // And it really narrowed: only the row whose policy includes `compact` survives.
    expect(container.textContent).not.toContain("__consumer_offsets");
    dispose();
  });

  test("the facet chips are drawn in the order the design puts them in", async () => {
    /*
     * `SCREENS-V4.md` §4.6 writes the bar out as
     * `✓ All | 🔒 Internal | ⚠ Out of sync | ⇄ Compacted`, and the order is the reading: the two
     * the cluster can apply come first, then the two that narrow the page. Reversing `FACETS` left
     * every case in this package green, so the design's own sequence was carried by nothing but
     * the order somebody typed it in.
     */
    const list = listing();
    const { container, dispose } = mount(() => list.node);
    await flush();
    const chips = [
      ...(container
        .querySelector('[data-testid="topic-facets"]')
        ?.querySelectorAll("button") ?? []),
    ].map((chip) => chip.textContent?.trim());
    expect(chips).toEqual(["All", "Internal", "Out of sync", "Compacted"]);
    dispose();
  });

  test("a row whose cleanupPolicy is absent renders nothing in that column", async () => {
    /*
     * Not `delete`, which is Kafka's default and would be the screen inventing a setting it was not
     * told; and not the em dash either, which everywhere else on this page means "a figure nobody
     * could measure". `shipments.v1` has no policy on it and `orders.payments.v2` has `delete`, so
     * this asserts one tag exists and the other cell is empty rather than asserting a global count.
     */
    const list = listing({ topics: [rows[0] as TopicRow, rows[2] as TopicRow] });
    const { container, dispose } = mount(() => list.node);
    await flush();

    /* `.kui-table__row` and not `tbody tr`: a windowed table pads its scroll height with two
       `role="presentation"` spacer rows, which are layout rather than topics. */
    const cells = [...container.querySelectorAll(".kui-table__row")].map((row) =>
      [...row.querySelectorAll("td")].at(-1)?.textContent?.trim(),
    );
    expect(cells).toEqual(["delete", ""]);
    dispose();
  });

  test("every order the Sort menu offers is one the server can produce", async () => {
    /*
     * The menu's options and the request's field names are the two ends of one vocabulary, and they
     * used to be two hand-written lists in two files. The pair had a hole with a direction to it:
     * *removing* a mapping was caught by a case in `write.test.ts`, and *adding* an option with no
     * mapping was not — which ships a Sort item that redraws the list in the server's own order
     * under an ascending arrow, and looks exactly like a sort.
     *
     * Driven through the rendered control rather than over `SORTABLE_COLUMNS`, so a menu that stops
     * being derived from that list fails here instead of passing against its own source.
     */
    const list = listing();
    const { container, dispose } = mount(() => list.node);
    await flush();

    const openSort = async (): Promise<HTMLElement[]> => {
      const trigger = [...container.querySelectorAll<HTMLElement>('[role="combobox"]')].find(
        (element) => element.textContent?.includes("Sort ·"),
      );
      // Only when it is shut: the trigger toggles, and a click on an open list closes it.
      if (trigger?.getAttribute("aria-expanded") !== "true") trigger?.click();
      await flush();
      return [...container.querySelectorAll<HTMLElement>('[role="option"]')];
    };

    // The first option is the server's own order and is not a column, so it is not one of these.
    const offered = (await openSort()).length;
    expect(offered).toBeGreaterThan(1);

    for (let index = 1; index < offered; index += 1) {
      const options = await openSort();
      const label = options[index]?.textContent?.trim() ?? "";
      options[index]?.dispatchEvent(new MouseEvent("pointerdown", { bubbles: true, cancelable: true }));
      await flush();

      const asked = list.asked.at(-1);
      expect(asked?.sort, `choosing "${label}" should have asked for an order`).toBeTruthy();
      expect(
        toTopicQuery(asked as TopicListQuery).sort,
        `the Sort menu offers "${label}", so the request has to carry a field for it`,
      ).toBeDefined();
    }
    dispose();
  });

  test("counts against the whole list, not against the page it can see", async () => {
    // "12 topics" over a table of twelve rows that is really a cluster of four thousand is the most
    // confidently wrong sentence this page could write.
    const list = listing({ totalItems: 4000 });
    const { container, dispose } = mount(() => list.node);
    await flush();
    expect(container.textContent).toContain("of 4,000 topics");
    dispose();
  });

  test("says how many are shown when the server did not count", async () => {
    // `undefined` is not zero. Printing the page's own length as a total would be a claim about the
    // cluster made from the size of one page.
    const list = listing({ totalItems: undefined });
    const { container, dispose } = mount(() => list.node);
    await flush();
    expect(container.textContent).toContain("topics shown");
    dispose();
  });

  test("draws a value KUI does not know as a dash with a word beside it, never as zero", async () => {
    const list = listing({ topics: [rows[2] as TopicRow] });
    const { container, dispose } = mount(() => list.node);
    await flush();
    // Every cell that has no value draws the dash, and *only* the dash: an assertion that merely
    // looked for one somewhere on the page would still pass if the records cell drew `0`, because
    // the cleanup-policy cell has a dash of its own.
    const absent = [...container.querySelectorAll(".kui-table__cell-muted [aria-hidden]")];
    expect(absent.length).toBeGreaterThan(0);
    for (const cell of absent) expect(cell.textContent).toBe("—");
    // A bare dash is announced as "dash" or as nothing at all depending on the reader; the fact is
    // that the value is not known, and that is what is said.
    expect(container.textContent).toContain("not known");
    // And nothing in this row is a drawn number, because none of its figures is known.
    expect(container.querySelectorAll(".kui-table__cell-number")).toHaveLength(2);
    dispose();
  });

  test("says how many topics are missing rather than quietly being short", async () => {
    const list = listing({ incomplete: 4 });
    const { container, dispose } = mount(() => list.node);
    await flush();
    expect(container.textContent).toContain("4 topics could not be described");
    dispose();
  });

  test("distinguishes an empty cluster from a search that matched nothing", async () => {
    const emptyList = listing({ topics: [] });
    const empty = mount(() => emptyList.node);
    await flush();
    expect(empty.container.textContent).toContain("No topics yet");
    empty.dispose();

    // A search the server has already applied, with nothing to show for it. The distinction is in
    // the query rather than in a local filter, because the search is not local any more.
    const searched = listing({
      topics: [],
      query: { ...DEFAULT_TOPIC_QUERY, search: "nothing-like-this" },
    });
    const listed = mount(() => searched.node);
    await flush();
    expect(listed.container.textContent).toContain("No topic matches that text");
    listed.dispose();
  });

  test("waits for a pause before asking, so a typed word is one request and not eight", async () => {
    vi.useFakeTimers();
    const list = listing();
    const { container, dispose } = mount(() => list.node);
    await flush();

    const search = container.querySelector<HTMLInputElement>('input[type="search"]');
    for (const text of ["p", "pa", "pay", "paym"]) {
      if (search !== null) {
        search.value = text;
        search.dispatchEvent(new Event("input", { bubbles: true }));
      }
      vi.advanceTimersByTime(50);
    }
    // Still nothing: every keystroke cancelled the one before it.
    expect(list.asked).toEqual([]);

    vi.advanceTimersByTime(400);
    expect(list.asked).toHaveLength(1);
    expect(list.asked[0]?.search).toBe("paym");
    vi.useRealTimers();
    dispose();
  });

  test("goes back to the first page whenever the view changes", async () => {
    // Page 7 of one filter is not page 7 of another, and landing on an empty page after typing
    // reads as "no matches" when the matches are on page 1.
    const list = listing({ query: { ...DEFAULT_TOPIC_QUERY, page: 7 } });
    const { container, dispose } = mount(() => list.node);
    await flush();
    [...container.querySelectorAll("button")]
      .find((button) => button.textContent?.trim() === "Internal")
      ?.click();
    await flush();
    expect(list.asked.at(-1)?.page).toBe(1);
    dispose();
  });

  test("the create action is disabled with a reason rather than hidden", async () => {
    const list = listing({
      onCreate: () => undefined,
      createDisabledReason: "This cluster is configured read-only.",
    });
    const { container, dispose } = mount(() => list.node);
    await flush();
    const button = [...container.querySelectorAll("button")].find((b) =>
      b.textContent?.includes("Create topic"),
    );
    expect(button?.getAttribute("aria-disabled")).toBe("true");
    dispose();
  });

  test("the address describes which list, and never where the reader is in it", () => {
    /*
     * The page, the page size and the order are this reader's own controls over a list rather than
     * a description of which list, and nothing in the product links to them. Reading them would be
     * two sources of truth for one number — the very next keystroke resets the page to 1 — and
     * `queryFromAddress` growing a `page:` line left every case in this package green.
     *
     * Asserted against the defaults rather than against literals, so that changing the default page
     * size is one edit and not two.
     */
    const asked = queryFromAddress("?q=orders.&page=7&pageSize=8&sort=size:desc");
    expect(asked.search).toBe("orders.");
    expect(asked.page).toBe(DEFAULT_TOPIC_QUERY.page);
    expect(asked.pageSize).toBe(DEFAULT_TOPIC_QUERY.pageSize);
    expect(asked.sort).toBe(DEFAULT_TOPIC_QUERY.sort);
  });

  test("counts and sizes read as people write them", () => {
    expect(matchCount(3, 3)).toBe("3 topics");
    expect(matchCount(1, 1)).toBe("1 topic");
    expect(matchCount(2, 4000)).toBe("2 of 4,000 topics");
    expect(formatBytes(4096)).toBe("4.1 kB");
    expect(formatBytes(0)).toBe("0 B");
    expect(formatBytes(128_000_000_000)).toBe("128.0 GB");

    // THE MERGE ITSELF, WHICH THE THREE FIGURES ABOVE CANNOT SEE. `TopicListPage.tsx` used to carry a
    // second implementation of this function and now re-exports the kernel's; wave 11 deleted the copy
    // and nothing asserted the deletion. Measured and filed as W11-04/U1: pasting the old body back in
    // left this file, `packages/kernel`, `packages/shell`, `tsc --build` and `lint:boundaries` all green,
    // because all three figures above are IDENTICAL under both implementations. Two assertions, because
    // each catches what the other cannot: the identity catches a re-divergence that happens to agree on
    // every figure anybody writes down, and the figure catches a re-export that is re-pointed at a third
    // function. `999.96` is the promotion threshold the kernel's header argues for at length -- the
    // deleted copy promoted at a bare `1000` and printed `999.96 B` here.
    expect(formatBytes).toBe(kernelFormatBytes);
    expect(formatBytes(999.96)).toBe("1.0 kB");
  });
});

describe("the topic page frame", () => {
  test("names the topic in full and says how it is doing", async () => {
    const long = "orders.payments.v2.dead-letter.retry-5m.eu-central-1.reprocessing";
    const { container, dispose } = mount(() => <TopicPage name={long} health="in-sync" />);
    await flush();
    // Never shortened: a heading that ended in an ellipsis would name a different topic.
    expect(container.querySelector("h1")?.textContent).toBe(long);
    expect(container.textContent).toContain("in sync");
    dispose();
  });

  test("a topic KUI could not describe is not drawn as a broken one", () => {
    // "unknown" is a failure to describe, not a failure of the topic. Danger colours here would
    // tell an operator their topic is offline when what is offline is the broker that would say.
    expect(healthChip("unknown")).toEqual({ tone: "neutral", label: "not described" });
    expect(healthChip("offline")).toEqual({ tone: "danger", label: "offline" });
    expect(healthChip("under-replicated").tone).toBe("warning");
  });

  test("the destructive action does not share a shape with the constructive one", async () => {
    const purge = vi.fn();
    const { container, dispose } = mount(() => (
      <TopicPage
        name="orders.payments.v2"
        health="in-sync"
        onProduce={{ label: "Produce message", onClick: () => undefined }}
        onPurge={{ label: "Purge", onClick: purge }}
      />
    ));
    await flush();
    const buttons = [...container.querySelectorAll("button")];
    const produce = buttons.find((b) => b.textContent?.includes("Produce message"));
    const trash = buttons.find((b) => b.textContent?.includes("Purge"));
    // Different variants, which is what makes them different silhouettes rather than two buttons
    // that differ only in their words.
    expect(produce?.className).toContain("secondary");
    expect(trash?.className).toContain("danger");
    // And a glyph as well as the outline, because an outline alone is a colour-only distinction.
    expect(trash?.querySelector("svg")).not.toBeNull();
    trash?.click();
    expect(purge).toHaveBeenCalledOnce();
    dispose();
  });

  test("an action this principal may not take is disabled with the reason, not hidden", async () => {
    const { container, dispose } = mount(() => (
      <TopicPage
        name="t"
        health="in-sync"
        onPurge={{
          label: "Purge",
          onClick: () => undefined,
          disabledReason: "You do not hold a role that permits purging this topic.",
        }}
      />
    ));
    await flush();
    const trash = [...container.querySelectorAll("button")].find((b) =>
      b.textContent?.includes("Purge"),
    );
    expect(trash?.getAttribute("aria-disabled")).toBe("true");
    dispose();
  });

  test("renders the chrome it is handed and nothing when it is handed none", async () => {
    const bare = mount(() => <TopicPage name="t" health="in-sync" />);
    await flush();
    // A breadcrumb with a single item is a line that tells nobody anything; none is drawn.
    expect(bare.container.querySelector("nav")).toBeNull();
    bare.dispose();

    const dressed = mount(() => (
      <TopicPage name="t" health="in-sync" breadcrumb={<nav aria-label="Breadcrumb">Topics</nav>} />
    ));
    await flush();
    expect(dressed.container.querySelector("nav")).not.toBeNull();
    dressed.dispose();
  });
});

describe("the view toggle", () => {
  test("remembers the choice, because a control that forgets reads as broken", async () => {
    /*
     * `SCREENS.md` §2.12 is explicit: the choice persists per user, not per visit — "an operator who
     * prefers cards and gets a table on every navigation will conclude the control does not work".
     * It is a preference about how this person reads a list, not a property of the list, which is
     * why it is not in the query string: a link somebody sends should show the recipient their own
     * preferred view.
     */
    window.localStorage.removeItem("kui.topics.view");
    expect(storedView()).toBe("table");

    rememberView("cards");
    expect(storedView()).toBe("cards");

    rememberView("table");
    expect(storedView()).toBe("table");
  });

  test("falls back to the table when storage cannot be read at all", () => {
    // A private window, or a browser set to block site data. Both `getItem` and `setItem` can throw
    // rather than return null, and a preference that cannot be read is not an error — it is the
    // default. A screen that threw here would fail to render a topic list over a stored preference.
    const original = window.localStorage.getItem;
    Object.defineProperty(window.localStorage, "getItem", {
      configurable: true,
      value: () => {
        throw new Error("The operation is insecure.");
      },
    });
    expect(storedView()).toBe("table");
    Object.defineProperty(window.localStorage, "getItem", { configurable: true, value: original });
  });

  test("a card carries the topic's shape, its health and its measurements", async () => {
    /*
     * `SCREENS-V4.md` §4.7's composition — the three tags, the size and the rate — plus the health
     * pill, which the design leaves off cards and this keeps: an operator who switches to cards and
     * can no longer see that a topic is offline has been given decoration in exchange for
     * information. The deviation is argued at the top of `TopicCards.tsx`.
     */
    const { container, dispose } = mount(() => (
      <TopicCards topics={rows} onOpen={() => undefined} formatBytes={formatBytes} />
    ));
    await flush();
    expect(container.textContent).toContain("12 partitions");
    expect(container.textContent).toContain("RF 3");
    expect(container.textContent).toContain("delete");
    // The health the design omits, kept.
    expect(container.textContent).toContain("in sync");
    expect(container.textContent).toContain("not described");
    // `rows[2]` has no size and none of them has a rate: words, never a zero.
    expect(container.textContent).toContain("not measured");
    expect(container.textContent).not.toContain("0 B");
    dispose();
  });

  test("the magnitude bar is this page's scale, and absent where nothing was read", async () => {
    /*
     * Two rules that share one `Show`, and neither could be made to fail.
     *
     * A bar needs a denominator and the only one the cards hold is the largest topic among the rows
     * the server sent, so the caller supplies it. Dividing by a constant instead — a cluster-wide
     * maximum this component invented — left every case green and draws a picture of a number
     * nobody measured. And a topic whose size could not be read must draw **no bar**: an empty
     * track and a topic of zero bytes are indistinguishable, and one of them is a measurement.
     *
     * `rows` holds exactly the pair that separates them: `orders.payments.v2` at 128 GB, and
     * `shipments.v1`, the topic KUI could not describe.
     */
    const { container, dispose } = mount(() => (
      <TopicCards topics={rows} onOpen={() => undefined} formatBytes={formatBytes} />
    ));
    await flush();

    const cards = [...container.querySelectorAll(".kui-topic-card")];
    const barIn = (index: number): HTMLElement | null =>
      cards[index]?.querySelector<HTMLElement>(".kui-magnitude__fill") ?? null;

    // The largest row on the page is the denominator, so it fills the track exactly.
    expect(barIn(0)?.style.width).toBe("100%");
    /* And the row beside it is scaled against that same 128 GB rather than against a constant:
       4,096 bytes of 128 GB rounds to nothing, which is the honest picture. */
    expect(barIn(1)?.style.width).toBe("0%");
    // The topic with no size draws no track at all.
    expect(cards).toHaveLength(3);
    expect(barIn(2)).toBeNull();
    dispose();
  });
});

/**
 * The seam, rather than the components.
 *
 * Everything below mounts the real route over the real router and a stubbed gateway, because every
 * rule here is about *which document reaches which component*. A case that handed `TopicListPage` a
 * `statistics` element would assert the arrangement the case itself made, and would keep passing if
 * `TopicsRoute` started computing those totals from the rows on screen — which is the one change
 * these cases exist to catch.
 */
describe("the topics screen, wired", () => {
  /* Toasts are a module-level stack and the view preference is `localStorage`: both outlive a case,
     and a case that ticked rows in a table would otherwise inherit `cards` from the one before it. */
  afterEach(() => {
    clearToasts();
    forgetQueries();
    restoreMeasuredRows();
    window.localStorage.removeItem("kui.topics.view");
  });

  /** One page of three topics, so a page count and a cluster count can differ. */
  const threeRows = {
    topics: {
      status: "ok",
      fetchedAt: "2026-09-06T00:00:00Z",
      data: {
        items: [
          {
            name: "orders.payments.v2",
            internal: false,
            partitionCount: 12,
            replicationFactor: 3,
            outOfSyncReplicas: 0,
            offlinePartitions: 0,
            messageCount: 18_442_901,
            sizeBytes: 128_000_000_000,
            cleanupPolicy: "delete",
          },
          {
            name: "orders.refunds.v1",
            internal: false,
            partitionCount: 6,
            replicationFactor: 3,
            outOfSyncReplicas: 0,
            offlinePartitions: 0,
            messageCount: 12,
            sizeBytes: 4096,
            cleanupPolicy: "compact",
          },
          {
            name: "orders.audit.v1",
            internal: false,
            partitionCount: 3,
            replicationFactor: 3,
            outOfSyncReplicas: 0,
            offlinePartitions: 0,
            messageCount: 4,
            sizeBytes: 512,
          },
        ],
        page: { page: 1, pageSize: 32, totalItems: 3 },
      },
    },
    incompleteTopics: 0,
  };

  test("a first paint is a placeholder and never the sentence for a refusal", async () => {
    /*
     * Two absences that look alike and mean opposite things: "we have not asked yet" and "we asked
     * and there is no answer". Collapsing the pending branch left this package green, and the
     * result is a page that says *"not measured"* under all three totals for as long as the first
     * request takes — a cluster reported as unmeasurable while it is being measured, which is the
     * one wrong reading a screen built on this distinction must not produce.
     *
     * The region is rendered directly because the rule is the component's own: the route decides
     * *whether* it is loading, and this decides what loading looks like.
     */
    const { container, dispose } = mount(() => <TopicStatisticsRegion loading />);
    await flush();

    expect(container.querySelectorAll(".kui-skeleton").length).toBeGreaterThan(0);
    expect(container.textContent).not.toContain("not measured");
    dispose();
  });

  test("the statistics region shows the cluster total and not the page's", async () => {
    /*
     * `SCREENS-V4.md` §4.6 calls this the load-bearing fact of the screen: the capture shows 128
     * under TOTAL TOPICS while the table below it holds three. The stub answers three rows and a
     * statistics document saying 128, so a region that folded the rows would print `3` and this
     * would fail — which is exactly what the mutation line asks for.
     */
    const host = topicsHost({
      at: "/clusters/stats-cluster/topics",
      answers: {
        "/api/v1/clusters/{clusterId}/topics": threeRows,
        "/api/v1/clusters/{clusterId}/topics/statistics": {
          statistics: {
            status: "ok",
            fetchedAt: "2026-09-06T00:00:00Z",
            data: { topicCount: 128, partitionCount: 1536, sizeBytes: 842_000_000_000, incompleteTopics: 0 },
          },
        },
      },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    const tile = container.querySelector('[data-testid="topic-stat-topics"]');
    expect(tile?.textContent).toContain("128");
    // And it is not the page's three, dressed as a cluster total.
    expect(tile?.textContent).not.toContain("3");
    /* And the page really did hold three, so the two figures genuinely disagree rather than the
       stub having answered the same number twice. The count beside the controls is the page's own
       — the windowed table draws no rows in a DOM with no layout engine, which is what
       `viewportHeight` exists for and is not what this case is about. */
    expect(container.querySelector(".kui-topic-list__count")?.textContent).toBe("3 topics");
    dispose();
  });

  test("the statistics document is fetched once and not per keystroke", async () => {
    /*
     * The other half of the load-bearing fact. The tile must not *move* when the search box does,
     * and the case above proves the figure; this proves the request. Keying the statistics query by
     * the query — which is the natural thing to write, and which left all 134 cases green — issues
     * a request per control change for a document that cannot change because of one, and on a slow
     * registry the totals then flicker to `not measured` while somebody types.
     *
     * Counted on the wire rather than on the tile, because a cache that answered from memory would
     * hide the extra request and the operator's cluster would still be answering it.
     */
    withMeasuredRows();
    const host = topicsHost({
      at: "/clusters/one-statistics-cluster/topics",
      answers: {
        "/api/v1/clusters/{clusterId}/topics": threeRows,
        "/api/v1/clusters/{clusterId}/topics/statistics": {
          statistics: {
            status: "ok",
            fetchedAt: "2026-09-06T00:00:00Z",
            data: {
              topicCount: 128,
              partitionCount: 1536,
              sizeBytes: 842_000_000_000,
              incompleteTopics: 0,
            },
          },
        },
      },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    const countOf = (path: string): number =>
      host.stub.calls.filter((call) => call === path).length;
    expect(countOf("/api/v1/clusters/{clusterId}/topics/statistics")).toBe(1);
    const listedBefore = countOf("/api/v1/clusters/{clusterId}/topics");

    // The chip is the control that needs no debounce; any control that moves the query would do.
    [...container.querySelectorAll("button")]
      .find((button) => button.textContent?.trim() === "Internal")
      ?.click();
    await settle();

    // The list moved, so the query genuinely changed…
    expect(countOf("/api/v1/clusters/{clusterId}/topics")).toBeGreaterThan(listedBefore);
    // …and the cluster was not asked a second time for figures the change cannot alter.
    expect(countOf("/api/v1/clusters/{clusterId}/topics/statistics")).toBe(1);
    dispose();
  });

  test("a refused total renders the sentence and not 0", async () => {
    /*
     * `partitionCount` and `sizeBytes` are each `Option` on the wire and each refuses on its own: a
     * topic the scrape could not describe removes both sums and leaves the count. `0 B` under TOTAL
     * STORAGE is the most reassuring possible rendering of the least reassuring possible state.
     */
    const host = topicsHost({
      at: "/clusters/refused-cluster/topics",
      answers: {
        "/api/v1/clusters/{clusterId}/topics": threeRows,
        "/api/v1/clusters/{clusterId}/topics/statistics": {
          statistics: {
            status: "ok",
            fetchedAt: "2026-09-06T00:00:00Z",
            data: { topicCount: 10, partitionCount: null, sizeBytes: null, incompleteTopics: 2 },
          },
        },
      },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    const partitions = container.querySelector('[data-testid="topic-stat-partitions"]');
    const storage = container.querySelector('[data-testid="topic-stat-storage"]');
    expect(partitions?.textContent).toContain("not measured");
    expect(storage?.textContent).toContain("not measured");
    expect(partitions?.textContent).not.toContain("0");
    expect(storage?.textContent).not.toContain("0 B");
    // The count survives its own sums, and the screen says why they are gone.
    expect(container.querySelector('[data-testid="topic-stat-topics"]')?.textContent).toContain("10");
    expect(container.textContent).toContain("2 topics on this cluster could not be described");
    dispose();
  });

  test("the Overview tab renders a body", async () => {
    /*
     * The tab the strip opens by default, and until now the one that drew nothing at all: the route
     * declared `id: "overview"` and had no `<Show when={tab() === \"overview\"}>` anywhere in it.
     * Mounted at the topic's bare address, which is the address that tab lives at.
     */
    const host = topicsHost({
      at: "/clusters/overview-cluster/topics/orders.v1",
      answers: {
        "/api/v1/clusters/{clusterId}/topics/{topicName}/overview": {
          topic: {
            status: "ok",
            fetchedAt: "2026-09-06T00:00:00Z",
            data: {
              row: {
                name: "orders.v1",
                internal: false,
                partitionCount: 6,
                replicationFactor: 3,
                outOfSyncReplicas: 0,
                offlinePartitions: 0,
                messageCount: 16,
                sizeBytes: null,
                produceRate: null,
                cleanupPolicy: "delete",
              },
              partitions: [
                { partition: 0, leader: 1, replicas: [{ broker: 1, leader: true, inSync: true }], earliestOffset: 0, latestOffset: 1 },
              ],
            },
          },
          consumerGroups: { status: "ok", fetchedAt: "2026-09-06T00:00:00Z", data: [{}, {}] },
        },
      },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    // The four tiles the design names, and the partition table under them.
    expect(container.querySelector('[data-testid="topic-overview-partitions"]')?.textContent).toContain("6");
    expect(container.querySelector('[data-testid="topic-overview-groups"]')?.textContent).toContain("2");
    expect(container.querySelector('[data-testid="topic-partitions-table"]')).not.toBeNull();
    // The two figures this cluster does not report say so in words. Never `0 B` and never `0 /s`.
    expect(container.querySelector('[data-testid="topic-overview-size"]')?.textContent).toContain("not measured");
    expect(container.querySelector('[data-testid="topic-overview-rate"]')?.textContent).toContain("not measured");
    // And the trail the design draws inside the content.
    expect(container.querySelector("nav[aria-label='Breadcrumb']")?.textContent).toContain("Topics");
    dispose();
  });

  test("the consumers tab prints host:port", async () => {
    /*
     * `coordinatorHost` and `coordinatorPort` are on the wire together with `coordinatorId`, and the
     * screen printed `broker 1` — an id is not an address. Driven at `?tab=consumers`, so the tab's
     * own request is the one that produces the row.
     */
    const host = topicsHost({
      at: "/clusters/coord-cluster/topics/orders.v1?tab=consumers",
      answers: {
        "/api/v1/clusters/{clusterId}/topics/{topicName}/overview": {
          topic: {
            status: "ok",
            fetchedAt: "2026-09-06T00:00:00Z",
            data: { row: { name: "orders.v1", internal: false, partitionCount: 6, replicationFactor: 1, outOfSyncReplicas: 0, offlinePartitions: 0 }, partitions: [] },
          },
        },
        "/api/v1/clusters/{clusterId}/topics/{topic}/consumer-groups": {
          rows: [
            {
              group: {
                groupId: "order-fulfilment",
                state: "EMPTY",
                members: 0,
                topics: 1,
                coordinatorId: 1,
                coordinatorHost: "kafka",
                coordinatorPort: 9092,
                totalLag: 9,
              },
              topicLag: 9,
              partitions: 6,
              dormant: true,
            },
            {
              // The coordinator could not be described. No address, and no invented broker id.
              group: { groupId: "nightly-batch", state: "EMPTY", members: 0, topics: 1, totalLag: 0 },
              topicLag: 0,
              partitions: 1,
              dormant: true,
            },
          ],
        },
      },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    expect(container.textContent).toContain("kafka:9092");
    expect(container.textContent).not.toContain("broker 1");
    // The row with no coordinator says nothing rather than half an address.
    expect(container.textContent).toContain("no coordinator address");
    dispose();
  });

  test("reopening a tab re-reads it, because lag moves while somebody is looking", async () => {
    /*
     * `TAB_QUERIES` is a registry of its own at `staleAfterMs: 0`, and the reason is written above
     * it: consumer lag and partition offsets move while the page is open, so a figure carried over
     * from four minutes ago is wrong in the direction that matters — it says a group has caught up
     * when it has not. Raising the entry to the general cache's thirty seconds left every case in
     * this package green, which made the whole registry deletable.
     *
     * The tabs are read out of the address, so this is one mounted route and three navigations, not
     * three mounts: a case that remounted would prove nothing about a cache that outlives a mount.
     */
    const at = "/clusters/reopened-cluster/topics/orders.v1";
    const host = topicsHost({
      at: `${at}?tab=consumers`,
      answers: {
        "/api/v1/clusters/{clusterId}/topics/{topicName}/overview": {
          topic: {
            status: "ok",
            fetchedAt: "2026-09-06T00:00:00Z",
            data: {
              row: {
                name: "orders.v1",
                internal: false,
                partitionCount: 6,
                replicationFactor: 1,
                outOfSyncReplicas: 0,
                offlinePartitions: 0,
              },
              partitions: [],
            },
          },
        },
        "/api/v1/clusters/{clusterId}/topics/{topic}/consumer-groups": { rows: [] },
      },
    });
    const { dispose } = mount(host.view);
    await settle();

    const opens = (): number =>
      host.stub.calls.filter(
        (call) => call === "/api/v1/clusters/{clusterId}/topics/{topic}/consumer-groups",
      ).length;
    expect(opens()).toBe(1);

    // Away, and the closed tab's key is `undefined`, so nothing is bound and nothing is asked.
    host.goTo(`${at}?tab=settings`);
    await settle();
    // And back, well inside the thirty seconds the general cache would have called this fresh.
    host.goTo(`${at}?tab=consumers`);
    await settle();

    expect(opens()).toBe(2);
    dispose();
  });

  test("the table and the cards the route renders share one selection set", async () => {
    /*
     * `SCREENS-V4.md` §3.7: the design's two ticks are on cards and the same set has to survive the
     * switch to the table. Nothing about that rule lives in either treatment. It lives in the fact
     * that `TopicsRoute` holds **one** signal and `TopicListPage` forwards it into whichever branch
     * is drawn — so it can only be asserted where the product makes the arrangement.
     *
     * The case that used to claim this built its own signal and handed it to a bare `TopicListPage`
     * and a bare `TopicCards` side by side, so it asserted the arrangement the case itself made:
     * giving the cards branch a private `createSignal` left it green, along with all 118 others.
     * This ticks a row in the table the *route* drew, works the *route's* own view control, and
     * reads the checkbox on the card the *route* drew.
     */
    withMeasuredRows();
    const host = topicsHost({
      at: "/clusters/one-set-cluster/topics",
      answers: { "/api/v1/clusters/{clusterId}/topics": threeRows },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    const tick = container.querySelector<HTMLInputElement>('tbody input[type="checkbox"]');
    expect(tick, "the route's table should have drawn rows to tick").not.toBeNull();
    tick?.click();
    await settle();

    // The route's own reading of the set, before the switch: one row, and the bar knows it.
    const bar = container.querySelector('[data-testid="topic-bulk-bar"]');
    expect(bar?.textContent).toContain("1 topic selected");

    const cards = [...container.querySelectorAll<HTMLInputElement>('input[type="radio"]')].find(
      (radio) => radio.value === "cards",
    );
    cards?.click();
    await settle();

    // The table is gone and the cards are drawn, so this really is the other treatment.
    expect(container.querySelector("tbody")).toBeNull();
    /* Scoped to the card grid: the `Show statistics` switch is a checked checkbox too, and a count
       over the whole page would be counting a control that has nothing to do with selection. */
    const ticked = [
      ...container.querySelectorAll<HTMLInputElement>('.kui-topic-cards input[type="checkbox"]'),
    ].filter((box) => box.checked);
    expect(ticked).toHaveLength(1);
    // And it is the same topic, not merely the same number of ticks.
    expect(ticked[0]?.getAttribute("aria-label") ?? ticked[0]?.closest("label")?.textContent).toContain(
      "orders.payments.v2",
    );
    // The bar is the route's, and it did not reset when the treatment changed.
    expect(container.querySelector('[data-testid="topic-bulk-bar"]')?.textContent).toContain(
      "1 topic selected",
    );

    dispose();
  });

  test("the overview says how short its partition table is, and only when it is short", async () => {
    /*
     * The notice used to fire on `partitions.length >= 500`, a hand-copy of the gateway's
     * `TopicDetailResponse.EmbeddedPartitionLimit` with nothing comparing the two — so a topic with
     * exactly 500 partitions and a complete table was told its table was short, and a topic whose
     * embedded list was short for any other reason was told nothing. It now subtracts what arrived
     * from what the topic has, which is the question the sentence claims to answer.
     */
    const overviewFor = (partitionCount: number, rows: number) => ({
      topic: {
        status: "ok",
        fetchedAt: "2026-09-06T00:00:00Z",
        data: {
          row: {
            name: "orders.v1",
            internal: false,
            partitionCount,
            replicationFactor: 1,
            outOfSyncReplicas: 0,
            offlinePartitions: 0,
          },
          partitions: Array.from({ length: rows }, (_unused, index) => ({
            partition: index,
            leader: 1,
            replicas: [{ broker: 1, leader: true, inSync: true }],
            earliestOffset: 0,
            latestOffset: 1,
          })),
        },
      },
    });

    const short = topicsHost({
      at: "/clusters/short-table-cluster/topics/orders.v1",
      answers: {
        "/api/v1/clusters/{clusterId}/topics/{topicName}/overview": overviewFor(9, 4),
      },
    });
    const shortMount = mount(short.view);
    await settle();
    expect(shortMount.container.textContent).toContain("This table shows 4 of 9 partitions");
    shortMount.dispose();
    forgetQueries();

    const whole = topicsHost({
      at: "/clusters/whole-table-cluster/topics/orders.v1",
      answers: {
        "/api/v1/clusters/{clusterId}/topics/{topicName}/overview": overviewFor(4, 4),
      },
    });
    const wholeMount = mount(whole.view);
    await settle();
    // Every partition arrived, so there is nothing to warn about and nothing is said.
    expect(wholeMount.container.textContent).not.toContain("The Partitions tab");
    wholeMount.dispose();
  });

  test("a refused consumer-group count renders the sentence and not 0", async () => {
    /*
     * The overview's five sections refuse independently: the consumer service can be down while the
     * topic service is not. `0` under CONSUMER GROUPS would say "nothing reads this topic", which
     * is
     * a real and completely different fact — and it is the fact an operator acts on.
     *
     * `data.ts` maps the section, `TopicOverviewTab` draws it, and until now no case fed the route
     * a
     * refused one: `consumerGroups: groupCount ?? 0` left the whole suite green.
     */
    const host = topicsHost({
      at: "/clusters/no-groups-cluster/topics/orders.v1",
      answers: {
        "/api/v1/clusters/{clusterId}/topics/{topicName}/overview": {
          topic: {
            status: "ok",
            fetchedAt: "2026-09-06T00:00:00Z",
            data: {
              row: {
                name: "orders.v1",
                internal: false,
                partitionCount: 1,
                replicationFactor: 1,
                outOfSyncReplicas: 0,
                offlinePartitions: 0,
                messageCount: 16,
                sizeBytes: 5114,
              },
              partitions: [
                { partition: 0, leader: 1, replicas: [{ broker: 1, leader: true, inSync: true }], earliestOffset: 0, latestOffset: 1 },
              ],
            },
          },
          // The consumer service did not answer. It said so; it did not say "none".
          consumerGroups: { status: "unavailable", reason: "circuit_open" },
        },
      },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    const tile = container.querySelector('[data-testid="topic-overview-groups"]');
    expect(tile?.textContent).toContain("not measured");
    // Not a zero anywhere in the tile, and not the em dash the table uses for a cell either.
    expect(tile?.textContent).not.toMatch(/\b0\b/);
    // The tiles that *were* answered still carry their figures: one refusal costs one tile.
    expect(container.querySelector('[data-testid="topic-overview-partitions"]')?.textContent).toContain("1");
    dispose();
  });

  test("a bulk action that partly refused raises a warning toast", async () => {
    /*
     * The tone is the whole content of this rendering. A green toast over a set that half refused
     * is
     * the reassuring rendering of the state that needs attention, and `tone: "success"` hard-coded
     * in place of the expression left every case in this package green — only the pure
     * `bulkSentence` helper was asserted, and it says nothing about colour.
     *
     * The refusal is a real one from `eachTopic`: the server plans the delete and withholds the
     * token, which is ADR-045's own way of saying no. `orders.payments.v2` gets a token and goes
     * through; `orders.refunds.v1` does not.
     */
    withMeasuredRows();
    const host = topicsHost({
      at: "/clusters/half-refused-cluster/topics",
      answers: {
        "/api/v1/clusters/{clusterId}/topics": threeRows,
        "/api/v1/clusters/{clusterId}/topics/{topicName}/deletion/plan": (request: StubRequest) => ({
          topic: request.params.path?.["topicName"] ?? "",
          partitions: 6,
          records: 16,
          autoCreateEnabled: false,
          warnings: [],
          ...(request.params.path?.["topicName"] === "orders.payments.v2"
            ? { token: "tok-1", expiresAt: "2026-09-06T00:05:00Z" }
            : {}),
        }),
        "/api/v1/clusters/{clusterId}/topics/{topicName}": {
          topic: "orders.payments.v2",
          partitions: 6,
          records: 16,
          warnings: [],
        },
      },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    /* Re-queried between the two clicks: ticking a row re-renders the windowed rows, so the second
       element of the first query is a node that is no longer in the document. */
    const tickRow = async (index: number): Promise<void> => {
      const boxes = [...container.querySelectorAll<HTMLInputElement>('tbody input[type="checkbox"]')];
      expect(boxes.length).toBeGreaterThan(index);
      boxes[index]?.click();
      await settle();
    };
    await tickRow(0);
    await tickRow(1);

    [...(container.querySelector('[data-testid="topic-bulk-bar"]')?.querySelectorAll("button") ?? [])]
      .find((button) => button.textContent?.trim() === "Delete")
      ?.click();
    await settle();

    const gate = document.querySelector<HTMLInputElement>(".kui-confirm__input");
    expect(gate).not.toBeNull();
    if (gate !== null) {
      gate.value = "delete";
      gate.dispatchEvent(new Event("input", { bubbles: true }));
    }
    await settle();

    [...document.querySelectorAll("button")]
      .find((button) => button.textContent?.trim() === "Delete topics")
      ?.click();
    await settle();

    const raised = toasts().at(-1);
    expect(raised?.title).toContain("1 topic deleted");
    expect(raised?.title).toContain("1 refused");
    // The rendering this case exists for.
    expect(raised?.tone).toBe("warning");

    dispose();
  });

  test("a bulk action that wholly succeeded raises a success toast", async () => {
    // The other half of the same expression: with both branches asserted, the tone cannot be a
    // constant of either value. Same arrangement, and both topics are issued a token.
    withMeasuredRows();
    const host = topicsHost({
      at: "/clusters/all-deleted-cluster/topics",
      answers: {
        "/api/v1/clusters/{clusterId}/topics": threeRows,
        "/api/v1/clusters/{clusterId}/topics/{topicName}/deletion/plan": (request: StubRequest) => ({
          topic: request.params.path?.["topicName"] ?? "",
          partitions: 6,
          records: 16,
          autoCreateEnabled: false,
          warnings: [],
          token: "tok-1",
          expiresAt: "2026-09-06T00:05:00Z",
        }),
        "/api/v1/clusters/{clusterId}/topics/{topicName}": {
          topic: "orders.payments.v2",
          partitions: 6,
          records: 16,
          warnings: [],
        },
      },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    const tickRow = async (index: number): Promise<void> => {
      const boxes = [...container.querySelectorAll<HTMLInputElement>('tbody input[type="checkbox"]')];
      boxes[index]?.click();
      await settle();
    };
    await tickRow(0);
    await tickRow(1);

    [...(container.querySelector('[data-testid="topic-bulk-bar"]')?.querySelectorAll("button") ?? [])]
      .find((button) => button.textContent?.trim() === "Delete")
      ?.click();
    await settle();

    const gate = document.querySelector<HTMLInputElement>(".kui-confirm__input");
    if (gate !== null) {
      gate.value = "delete";
      gate.dispatchEvent(new Event("input", { bubbles: true }));
    }
    await settle();

    [...document.querySelectorAll("button")]
      .find((button) => button.textContent?.trim() === "Delete topics")
      ?.click();
    await settle();

    const raised = toasts().at(-1);
    expect(raised?.title).toBe("2 topics deleted");
    expect(raised?.tone).toBe("success");

    dispose();
  });

  test("a finished bulk delete drops the ticks and re-reads the list", async () => {
    /*
     * The two lines after the toast in `TopicsRoute`'s bulk `onConfirm`, and both were carried by
     * nothing.
     *
     * Deleting `setSelected(new Set())` leaves the bar up, still reading "2 topics selected", over
     * rows that have just been destroyed — and the next gesture an operator makes from that bar
     * acts on a set whose members no longer exist. Deleting `reload()` leaves the screen showing
     * the topics it has just deleted, which is the most convincing kind of wrong data there is: the
     * operator confirms twice, and the second attempt fails with "topic does not exist".
     *
     * Both are asserted off the screen and off the stub's own request log rather than off the
     * mutation's return value, because what the operator sees afterwards is the whole rule.
     */
    withMeasuredRows();
    /* The list answers with the three rows until the delete has run, and with one row afterwards —
       which is what a re-read is *for*. Counting reads alone would be satisfied by a `reload()`
       that fetched and threw the answer away. */
    let deleted = false;
    let listReads = 0;
    const host = topicsHost({
      at: "/clusters/bulk-aftermath-cluster/topics",
      answers: {
        "/api/v1/clusters/{clusterId}/topics": (request: StubRequest) => {
          if (request.body !== undefined) return { name: "unused", partitions: 1 };
          listReads += 1;
          if (!deleted) return threeRows;
          return {
            topics: {
              status: "ok",
              fetchedAt: "2026-09-06T00:00:00Z",
              data: {
                items: threeRows.topics.data.items.slice(2),
                page: { page: 1, pageSize: 32, totalItems: 1 },
              },
            },
            incompleteTopics: 0,
          };
        },
        "/api/v1/clusters/{clusterId}/topics/{topicName}/deletion/plan": (
          request: StubRequest,
        ) => ({
          topic: request.params.path?.["topicName"] ?? "",
          partitions: 6,
          records: 16,
          autoCreateEnabled: false,
          warnings: [],
          token: "tok-1",
          expiresAt: "2026-09-06T00:05:00Z",
        }),
        "/api/v1/clusters/{clusterId}/topics/{topicName}": () => {
          deleted = true;
          return { topic: "orders.payments.v2", partitions: 6, records: 16, warnings: [] };
        },
      },
    });
    const { container, dispose } = mount(host.view);
    await settle();
    const readsBefore = listReads;

    const tickRow = async (index: number): Promise<void> => {
      const boxes = [
        ...container.querySelectorAll<HTMLInputElement>('tbody input[type="checkbox"]'),
      ];
      expect(boxes.length).toBeGreaterThan(index);
      boxes[index]?.click();
      await settle();
    };
    await tickRow(0);
    await tickRow(1);
    expect(container.querySelector('[data-testid="topic-bulk-bar"]')).not.toBeNull();

    [
      ...(container.querySelector('[data-testid="topic-bulk-bar"]')?.querySelectorAll("button") ??
        []),
    ]
      .find((button) => button.textContent?.trim() === "Delete")
      ?.click();
    await settle();

    const gate = document.querySelector<HTMLInputElement>(".kui-confirm__input");
    expect(gate).not.toBeNull();
    if (gate !== null) {
      gate.value = "delete";
      gate.dispatchEvent(new Event("input", { bubbles: true }));
    }
    await settle();

    [...document.querySelectorAll("button")]
      .find((button) => button.textContent?.trim() === "Delete topics")
      ?.click();
    await settle();

    /* The bar is gone because the selection is empty — §3.7's "absent at zero selection" — and not
       because it was hidden: the ticks that are left on screen are the assertion underneath it. */
    expect(container.querySelector('[data-testid="topic-bulk-bar"]')).toBeNull();
    const stillTicked = [
      ...container.querySelectorAll<HTMLInputElement>('tbody input[type="checkbox"]'),
    ].filter((box) => box.checked);
    expect(stillTicked).toHaveLength(0);

    // And the list was asked again, and drew the answer: the two deleted rows are off the screen.
    expect(listReads).toBeGreaterThan(readsBefore);
    expect(container.textContent).not.toContain("orders.payments.v2");
    expect(container.textContent).toContain("orders.audit.v1");

    dispose();
  });

  test("the bulk Empty asks for a different word and promises the topics survive", async () => {
    /*
     * The bar's two destructive actions share one `ConfirmDialog`, and every word in it is a
     * ternary on which one was pressed: the title, the sentence, the button's label and the word
     * the operator has to type. Collapsing any of them onto the delete branch left this package
     * green, because the only bulk confirmation ever driven here was the delete one. What that
     * ships is an Empty that demands the word "delete", offers a button reading "Delete topics",
     * and says it removes the topics — over a dialog that removes no topic at all. On the one
     * screen whose whole purpose is that its words are read before an irreversible thing happens,
     * that is the worst possible place to be wrong.
     *
     * Nothing is confirmed here. What is asserted is the dialog, which is where the difference is,
     * and the fact that the gate will not open on the other action's word.
     */
    withMeasuredRows();
    const host = topicsHost({
      at: "/clusters/bulk-empty-cluster/topics",
      answers: { "/api/v1/clusters/{clusterId}/topics": threeRows },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    const tickRow = async (index: number): Promise<void> => {
      const boxes = [
        ...container.querySelectorAll<HTMLInputElement>('tbody input[type="checkbox"]'),
      ];
      expect(boxes.length).toBeGreaterThan(index);
      boxes[index]?.click();
      await settle();
    };
    await tickRow(0);
    await tickRow(1);

    [
      ...(container.querySelector('[data-testid="topic-bulk-bar"]')?.querySelectorAll("button") ??
        []),
    ]
      .find((button) => button.textContent?.trim() === "Empty")
      ?.click();
    await settle();

    /*
     * Everything is read off the screen first and asserted after the dialog is gone. A modal lives
     * on `document.body` rather than inside this case's container, so an assertion that throws
     * mid-dialog leaves it in the document for every case that follows — which turns one red into
     * seven and hides which one is the finding. Ask the questions, tear down, then answer them.
     */
    const confirmState = (): string | undefined =>
      [...document.querySelectorAll<HTMLButtonElement>("button")]
        .find((one) => one.textContent?.trim() === "Empty topics")
        ?.getAttribute("aria-disabled") ?? "enabled";
    const gate = document.querySelector<HTMLInputElement>(".kui-confirm__input");
    const type = async (word: string): Promise<void> => {
      if (gate === null) return;
      gate.value = word;
      gate.dispatchEvent(new Event("input", { bubbles: true }));
      await settle();
    };

    const seen = {
      // The word the gate is asking for, off its own label rather than out of the props.
      expected: document.querySelector(".kui-confirm__expected")?.textContent,
      labels: [...document.querySelectorAll("button")].map((one) => one.textContent?.trim()),
      consequence: document.querySelector(".kui-confirm__consequence")?.textContent ?? "",
      /* The largest text on the dialog, and the last thing left ungated when the word, the label
         and the consequence were closed. Forced to the Delete branch it reads "Delete 2 topics?"
         over a button reading "Empty topics" — and the heading is what an operator skims before
         they read anything else, so it is the half of the dialog most likely to be the only half
         that is read. It is also `Dialog`'s `aria-labelledby`, so it is the accessible name of the
         modal: a screen reader announces this sentence and nothing else on arrival. */
      title: document.querySelector(".kui-modal__title")?.textContent ?? "",
      /* And the glyph beside the confirm label, which is the other half of "the same gesture must
         not look like two different irreversible things": `trash` on an Empty says records and the
         topic are both going. */
      confirmGlyph: [...document.querySelectorAll<HTMLButtonElement>("button")]
        .find((one) => one.textContent?.trim() === "Empty topics")
        ?.querySelector("[data-icon]")
        ?.getAttribute("data-icon"),
      hasGate: gate !== null,
      untyped: confirmState(),
      // The other action's word, which a dialog with one spelling for both would have accepted.
      afterDelete: (await type("delete"), confirmState()),
      afterEmpty: (await type("empty"), confirmState()),
      // Nothing left for the server while the operator was reading: the gate is before the send.
      sent: host.stub.requests.filter((request) => request.body !== undefined).length,
    };
    dispose();

    expect(seen.expected).toBe("empty");
    expect(seen.labels).toContain("Empty topics");
    expect(seen.labels).not.toContain("Delete topics");
    expect(seen.title).toBe("Empty 2 topics?");
    expect(seen.confirmGlyph).toBe("minus");
    /* Named rather than counted, because a confirmation that says "2 topics" is one the operator
       cannot check — and the promise that distinguishes this dialog from the other one. */
    expect(seen.consequence).toContain("orders.payments.v2");
    expect(seen.consequence).toContain("orders.refunds.v1");
    expect(seen.consequence).toContain("left as they are");
    expect(seen.hasGate).toBe(true);
    expect(seen.untyped).toBe("true");
    expect(seen.afterDelete).toBe("true");
    expect(seen.afterEmpty).toBe("enabled");
    expect(seen.sent).toBe(0);
  });

  test("the bulk bar's Export hands over the ticked rows and not the page", async () => {
    /*
     * Found by mutation and gated afterwards: `onSelect: () => exportRows(result().topics)` — the
     * bulk bar exporting the whole page instead of the selection — left every case in this package
     * green. The header action above the table is the one that exports the page; the bar's is about
     * the ticks, and the two are one line apart in the same file.
     */
    let handed: Blob | undefined;
    const original = URL.createObjectURL;
    Object.defineProperty(URL, "createObjectURL", {
      configurable: true,
      value: (blob: Blob) => {
        handed = blob;
        return "blob:test";
      },
    });
    Object.defineProperty(URL, "revokeObjectURL", { configurable: true, value: () => undefined });

    withMeasuredRows();
    const host = topicsHost({
      at: "/clusters/ticked-export-cluster/topics",
      answers: { "/api/v1/clusters/{clusterId}/topics": threeRows },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    container.querySelector<HTMLInputElement>('tbody input[type="checkbox"]')?.click();
    await settle();

    [...(container.querySelector('[data-testid="topic-bulk-bar"]')?.querySelectorAll("button") ?? [])]
      .find((button) => button.textContent?.trim() === "Export")
      ?.click();

    expect(handed).toBeDefined();
    const text = await (handed as Blob).text();
    expect(text).toContain('"orders.payments.v2"');
    // The two rows nobody ticked are not in the file, which is the whole difference.
    expect(text).not.toContain('"orders.refunds.v1"');
    expect(text).not.toContain('"orders.audit.v1"');

    Object.defineProperty(URL, "createObjectURL", { configurable: true, value: original });
    dispose();
  });

  test("changing the query drops the ticks it can no longer show", async () => {
    /*
     * Also found by mutation: deleting `setSelected(new Set())` from `changeQuery` left 124 cases
     * green, under a three-sentence comment arguing for it. A bar reading "1 topic selected" over a
     * page holding no such row is a control whose subject the operator cannot see, and the first
     * thing they would do to find out what it means is press `Delete`.
     */
    withMeasuredRows();
    const host = topicsHost({
      at: "/clusters/requeried-cluster/topics",
      answers: { "/api/v1/clusters/{clusterId}/topics": threeRows },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    container.querySelector<HTMLInputElement>('tbody input[type="checkbox"]')?.click();
    await settle();
    expect(container.querySelector('[data-testid="topic-bulk-bar"]')?.textContent).toContain(
      "1 topic selected",
    );

    // Any control that changes the query will do; the chip is the one that needs no debounce.
    [...container.querySelectorAll("button")]
      .find((button) => button.textContent?.trim() === "Internal")
      ?.click();
    await settle();

    /* The bar is absent at zero selection rather than reading "0 selected", so the reading is
       taken defensively — and the ticks themselves are checked, not only the bar above them. */
    expect(container.querySelector('[data-testid="topic-bulk-bar"]')?.textContent ?? "").not.toContain(
      "selected",
    );
    expect(
      [...container.querySelectorAll<HTMLInputElement>('tbody input[type="checkbox"]')].filter(
        (box) => box.checked,
      ),
    ).toHaveLength(0);
    dispose();
  });

  test("the create dialog will not submit a name the broker would refuse", async () => {
    /*
     * `write.test.ts` asserts the *validator*; nothing asserted that the dialog is wired to it.
     * Loosening `canCreate` to `!busy()` left every case in this package green, and the result is a
     * POST of an empty name — a request whose 400 arrives as an error envelope the operator has to
     * read to learn something the form already knew.
     *
     * Three states in one case, because the rule is the transition: closed on an empty form, closed
     * on a name Kafka reserves, and open on a name it will take. Any one of the three alone passes
     * against a button that is always disabled or always enabled.
     */
    const host = topicsHost({
      at: "/clusters/create-guard-cluster/topics",
      answers: { "/api/v1/clusters/{clusterId}/topics": threeRows },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    [...container.querySelectorAll("button")]
      .find((button) => button.textContent?.trim() === "Create topic")
      ?.click();
    await settle();

    const dialog = document.querySelector('[role="dialog"]');
    const confirm = (): HTMLButtonElement | undefined =>
      [...(dialog?.querySelectorAll<HTMLButtonElement>("button") ?? [])].find(
        (button) => button.textContent?.trim() === "Create topic",
      );
    const type = async (value: string): Promise<void> => {
      const field = dialog?.querySelector<HTMLInputElement>('input[type="text"]');
      if (field !== null && field !== undefined) {
        field.value = value;
        field.dispatchEvent(new Event("input", { bubbles: true }));
      }
      await settle();
    };

    // Nothing typed: closed.
    expect(confirm()?.getAttribute("aria-disabled")).toBe("true");
    // A name Kafka reserves: still closed, and the form says which rule it broke.
    await type(".");
    expect(confirm()?.getAttribute("aria-disabled")).toBe("true");
    // A name it will take: open.
    await type("orders.new.v1");
    expect(confirm()?.getAttribute("aria-disabled")).not.toBe("true");

    // And nothing was posted along the way — the guard is before the request, not after it.
    expect(host.stub.requests.filter((request) => request.body !== undefined)).toHaveLength(0);
    dispose();
  });
  test("an undescribed topic gets no partition badge, because 0 is a claim", async () => {
    /*
     * `count: overview()?.topic.partitions ?? 0` is the natural thing to write and left this whole
     * package green. It draws `0` beside "Partitions" for a topic KUI could not describe — a claim
     * no Kafka topic satisfies, made about the one topic whose shape nobody actually knows. Absent
     * is the honest rendering, and this is the case that distinguishes them.
     *
     * The overview path is unstubbed, so the section refuses: the frame still draws, which is the
     * other half of the rule.
     */
    const host = topicsHost({
      at: "/clusters/undescribed-cluster/topics/orders.v1",
      answers: {},
    });
    const { container, dispose } = mount(host.view);
    await settle();

    const strip = container.querySelector(".kui-page-tabs");
    expect(strip?.textContent).toContain("Partitions");
    // No badge at all — not a badge reading zero.
    expect(strip?.querySelectorAll(".kui-page-tabs__count")).toHaveLength(0);
    dispose();
  });
  test("each control on the topic page is gated on its own action", async () => {
    /*
     * Two controls and two actions — this page's header, and no more than that. `TopicsRoute` calls
     * `writeBlockedReason` seven times over four actions across its two screens (`grep -c` in
     * `TopicsRoute.tsx` counts the seven), and this case reaches two of them: the list screen's
     * three are "each control on the topic list screen is gated on its own action" below, and the
     * two behind the tabs are the case after that.
     *
     * `writeBlockedReason` takes whichever action it is handed — swapping `TopicMessagesDelete` for
     * `TopicDelete` on the purge left this package green, because every case that had ever
     * exercised permissions answered one `false` for everything. With one answer for both, a
     * control wired to the wrong action is disabled at exactly the moments the right one would be,
     * and no assertion can tell the two apart.
     *
     * So the principal here holds exactly one of them: they may empty this topic and may not delete
     * it. That is a real role — an operator trusted to reclaim disk and not to destroy a stream —
     * and it is the only arrangement in which the wiring is observable.
     */
    const overview = {
      "/api/v1/clusters/{clusterId}/topics/{topicName}/overview": {
        topic: {
          status: "ok",
          fetchedAt: "2026-09-06T00:00:00Z",
          data: {
            row: {
              name: "orders.v1",
              internal: false,
              partitionCount: 6,
              replicationFactor: 1,
              outOfSyncReplicas: 0,
              offlinePartitions: 0,
            },
            partitions: [],
          },
        },
      },
    };

    /**
     * Whether each control is offered, for one principal.
     *
     * `aria-disabled` rather than the `disabled` attribute, which is `Button`'s own deliberate
     * choice: a disabled element is skipped by Tab and fires no pointer events, so its explanation
     * would be unreachable by keyboard and unreachable by hover. The control is present either
     * way — §3.7's rule is that a forbidden action is explained, never hidden.
     */
    const offered = async (
      cluster: string,
      held: string,
    ): Promise<Record<string, boolean | undefined>> => {
      const host = topicsHost({
        at: `/clusters/${cluster}/topics/orders.v1`,
        permits: (action) => action.action === held,
        answers: overview,
      });
      const { container, dispose } = mount(host.view);
      await settle();
      const state = Object.fromEntries(
        ["Empty topic", "Delete topic"].map((label) => [
          label,
          [...container.querySelectorAll<HTMLButtonElement>("button")]
            .find((one) => one.textContent?.includes(label))
            ?.getAttribute("aria-disabled") !== "true",
        ]),
      );
      dispose();
      forgetQueries();
      return state;
    };

    // May empty, may not delete.
    expect(await offered("purger-cluster", "MESSAGES_DELETE")).toEqual({
      "Empty topic": true,
      "Delete topic": false,
    });
    /* And the mirror image, which is what makes the pair a gate: with only one arrangement, wiring
       both controls to the same action passes both assertions. */
    expect(await offered("deleter-cluster", "DELETE")).toEqual({
      "Empty topic": false,
      "Delete topic": true,
    });
  });
  test("each control on the topic list screen is gated on its own action", async () => {
    /*
     * The same hole as the case above, forty lines further up the same file and left there when
     * that one was closed. `createBlocked`, `purgeBlocked` and `deleteBlocked` on the **list**
     * screen each name an action, and nothing in this package could tell one naming from another:
     * wiring Create to `TopicDelete`, Empty to `TopicDelete` and Delete to `TopicMessagesDelete`
     * left all 145 cases green. What that ships is an enabled bulk **Empty** for a principal
     * trusted to remove a topic and not to destroy the records in one, an enabled bulk **Delete**
     * for the mirror of that person, and a Create button gated on an action about deletion.
     *
     * Three principals, each holding exactly one grant, because two arrangements cannot separate
     * three actions: given only "may create" and "may delete", an Empty wired to `TopicDelete` is
     * indistinguishable from an Empty wired to `TopicMessagesDelete`. Each row below is a real
     * role, which is why the wiring is observable at all.
     */
    withMeasuredRows();

    /**
     * Whether each of the list screen's three write controls is offered, for one principal.
     *
     * `aria-disabled` rather than the `disabled` attribute, which is `Button`'s own deliberate
     * choice: a disabled element is skipped by Tab and fires no pointer events, so the sentence
     * explaining the refusal would be unreachable by hover and by keyboard both. The control is
     * present either way — §3.7's rule is that a forbidden action is explained, never hidden — so
     * a control this cannot find fails the case rather than reading as a quiet `false`.
     */
    const offered = async (cluster: string, held: string): Promise<Record<string, boolean>> => {
      const host = topicsHost({
        at: `/clusters/${cluster}/topics`,
        permits: (action) => action.action === held,
        answers: { "/api/v1/clusters/{clusterId}/topics": threeRows },
      });
      const { container, dispose } = mount(host.view);
      await settle();

      /* The bulk bar does not exist at zero selection (§3.7), so two of the three controls cannot
         be read until a row is ticked. One tick is enough: the wiring does not vary with the size
         of the set, and what the bar counts is asserted elsewhere. */
      container.querySelector<HTMLInputElement>('tbody input[type="checkbox"]')?.click();
      await settle();

      const bar = container.querySelector('[data-testid="topic-bulk-bar"]');
      expect(bar, "the bulk bar carries two of the three controls this case reads").not.toBeNull();

      const enabled = (within: ParentNode, label: string): boolean => {
        const button = [...within.querySelectorAll<HTMLButtonElement>("button")].find(
          (one) => one.textContent?.trim() === label,
        );
        expect(button, `${label} must be on screen whoever is looking at it`).toBeDefined();
        return button?.getAttribute("aria-disabled") !== "true";
      };
      const state = {
        "Create topic": enabled(container, "Create topic"),
        Empty: bar !== null && enabled(bar, "Empty"),
        Delete: bar !== null && enabled(bar, "Delete"),
      };
      dispose();
      forgetQueries();
      return state;
    };

    // May add a topic, and may destroy nothing that is in one.
    expect(await offered("list-creator-cluster", "CREATE")).toEqual({
      "Create topic": true,
      Empty: false,
      Delete: false,
    });
    // May reclaim disk, may not remove a stream.
    expect(await offered("list-purger-cluster", "MESSAGES_DELETE")).toEqual({
      "Create topic": false,
      Empty: true,
      Delete: false,
    });
    // And the mirror, which is what makes the set a gate rather than three assertions of `false`.
    expect(await offered("list-deleter-cluster", "DELETE")).toEqual({
      "Create topic": false,
      Empty: false,
      Delete: true,
    });
  });
  test("a refused control on the list screen says why, in a reachable sentence", async () => {
    /*
     * The other half of the case above. That one asserts *which* action each control is gated on;
     * this one asserts that the refusal is **explained**. §3.7: "an action the principal may not
     * perform is disabled with a stated reason, not hidden" — and a control that is disabled with
     * an empty reason is hidden in the only sense that matters, because the operator is left with a
     * dead button and no idea whose problem it is.
     *
     * Both `disabledReason` expressions could be emptied with all 148 cases in this package green:
     * `TopicListPage`'s `disabledReason: props.createDisabledReason` and `TopicsRoute`'s
     * `{ disabledReason: purgeBlocked() }` on the bulk bar. Every case that had ever looked at a
     * refused control read `aria-disabled`, which stays `"true"` either way.
     *
     * The sentence is read through the button's own `aria-describedby` rather than through the
     * first `[role="tooltip"]` in the document: bubbles are portalled into `body` and an earlier
     * case's can still be there, so a query across the body can answer with somebody else's words.
     */
    withMeasuredRows();
    const host = topicsHost({
      at: "/clusters/list-reasons-cluster/topics",
      // Holds nothing. One arrangement is enough here because the case is about the sentence being
      // present at all; which action each control names is the case above.
      permits: false,
      answers: { "/api/v1/clusters/{clusterId}/topics": threeRows },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    container.querySelector<HTMLInputElement>('tbody input[type="checkbox"]')?.click();
    await settle();

    const bar = container.querySelector('[data-testid="topic-bulk-bar"]');
    expect(bar, "the bulk bar carries two of the three controls this case reads").not.toBeNull();

    const reasonUnder = async (within: ParentNode, label: string): Promise<string> => {
      const button = [...within.querySelectorAll<HTMLButtonElement>("button")].find(
        (one) => one.textContent?.trim() === label,
      );
      expect(button, `${label} must be on screen whoever is looking at it`).toBeDefined();
      if (button === undefined) return "";
      expect(button.getAttribute("aria-disabled")).toBe("true");
      button.dispatchEvent(new FocusEvent("focusin", { bubbles: true }));
      await settle();
      const described = button.getAttribute("aria-describedby") ?? "";
      const bubble = described === "" ? null : document.getElementById(described);
      expect(bubble, `${label} is disabled and says nothing about why`).not.toBeNull();
      return bubble?.textContent ?? "";
    };

    expect(await reasonUnder(container, "Create topic")).toBe(
      "You do not have permission to create a topic on this cluster.",
    );
    if (bar !== null) {
      expect(await reasonUnder(bar, "Empty")).toBe(
        "You do not have permission to empty topics on this cluster.",
      );
      expect(await reasonUnder(bar, "Delete")).toBe(
        "You do not have permission to delete topics on this cluster.",
      );
    }

    dispose();
  });

  test("a read-only cluster refuses every write, and blames the deployment", async () => {
    /*
     * ADR-047's flag, which reached none of these screens until wave 7: all seven of
     * `TopicsRoute`'s `writeBlockedReason` calls passed a hard-coded `readOnly: false`, so a
     * cluster somebody had deliberately registered read-only offered a live `Create topic`, a live
     * bulk `Delete` and a live `Empty` — and the refusal arrived from the server *after* the
     * operator had read a consequence and typed the word "delete". The permission half of the same
     * gate was closed in wave 5 and this half was wired to a constant on the screen being audited.
     *
     * The principal here holds **everything**, which is what makes the case a gate rather than a
     * second reading of the permission one: only the cluster's own flag can refuse these controls,
     * and the sentence has to say so. `writeBlockedReason` prints a different sentence for each
     * because they need different actions from the reader — asking an administrator for a
     * permission you already hold wastes an afternoon.
     */
    withMeasuredRows();
    const host = topicsHost({
      at: "/clusters/readonly-flag-cluster/topics",
      permits: true,
      answers: {
        "/api/v1/clusters/{clusterId}/topics": threeRows,
        "/api/v1/clusters/{clusterId}": {
          cluster: {
            id: "readonly-flag-cluster",
            name: "Read only",
            readOnly: true,
            bootstrapServers: "kafka:9092",
            origin: "file",
            security: {
              protocol: "PLAINTEXT",
              keystoreConfigured: false,
              truststoreConfigured: false,
            },
            summary: { status: "unavailable" },
          },
        },
      },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    container.querySelector<HTMLInputElement>('tbody input[type="checkbox"]')?.click();
    await settle();
    const bar = container.querySelector('[data-testid="topic-bulk-bar"]');
    expect(bar).not.toBeNull();

    const reasonUnder = async (within: ParentNode, label: string): Promise<string> => {
      const button = [...within.querySelectorAll<HTMLButtonElement>("button")].find(
        (one) => one.textContent?.trim() === label,
      );
      expect(button, `${label} must be on screen whoever is looking at it`).toBeDefined();
      if (button === undefined) return "";
      expect(button.getAttribute("aria-disabled")).toBe("true");
      button.dispatchEvent(new FocusEvent("focusin", { bubbles: true }));
      await settle();
      const described = button.getAttribute("aria-describedby") ?? "";
      const bubble = described === "" ? null : document.getElementById(described);
      expect(bubble, `${label} is disabled and says nothing about why`).not.toBeNull();
      return bubble?.textContent ?? "";
    };

    // The deployment's sentence, not the principal's: this account holds every one of them.
    expect(await reasonUnder(container, "Create topic")).toBe(
      "This cluster is configured read-only in KUI, so nothing here can create a topic on this " +
        "cluster.",
    );
    if (bar !== null) {
      expect(await reasonUnder(bar, "Empty")).toContain("configured read-only in KUI");
      expect(await reasonUnder(bar, "Delete")).toContain("configured read-only in KUI");
    }
    dispose();
    forgetQueries();

    /* And the same cluster answering `readOnly: false` leaves all three live — without this the
       case is met by a screen that refuses everybody, which is the shape the permission gate above
       already rules out and which this one would otherwise re-introduce. */
    const writable = topicsHost({
      at: "/clusters/writable-flag-cluster/topics",
      permits: true,
      answers: {
        "/api/v1/clusters/{clusterId}/topics": threeRows,
        "/api/v1/clusters/{clusterId}": {
          cluster: {
            id: "writable-flag-cluster",
            name: "Writable",
            readOnly: false,
            bootstrapServers: "kafka:9092",
            origin: "file",
            security: {
              protocol: "PLAINTEXT",
              keystoreConfigured: false,
              truststoreConfigured: false,
            },
            summary: { status: "unavailable" },
          },
        },
      },
    });
    const open = mount(writable.view);
    await settle();
    open.container.querySelector<HTMLInputElement>('tbody input[type="checkbox"]')?.click();
    await settle();
    const liveBar = open.container.querySelector('[data-testid="topic-bulk-bar"]');
    const live = (within: ParentNode, label: string): boolean =>
      [...within.querySelectorAll<HTMLButtonElement>("button")]
        .find((one) => one.textContent?.trim() === label)
        ?.getAttribute("aria-disabled") !== "true";
    expect(live(open.container, "Create topic")).toBe(true);
    expect(liveBar !== null && live(liveBar, "Empty")).toBe(true);
    expect(liveBar !== null && live(liveBar, "Delete")).toBe(true);
    open.dispose();
  });

  test("adding partitions and changing settings are gated on the topic-edit action", async () => {
    /*
     * The last two of `TopicsRoute`'s seven `writeBlockedReason` calls, and the two the case above
     * this one does not reach because they live behind tabs. Both are `TopicEdit` — there is no
     * separate "add partitions" action in the server's vocabulary — and neither was observable:
     * pointing either at `TopicMessagesDelete` left this package green, and what that ships is an
     * Add-partitions button offered to whoever may empty a topic and withheld from the person the
     * cluster trusts to change it.
     *
     * Two principals, holding one grant each, because "gated on `TopicEdit`" and "gated on
     * anything at all" are the same assertion under a single arrangement.
     *
     * The Settings control is the one place on these screens where a refusal is not a disabled
     * button, and that is deliberate: there is one Edit control per configuration key, so a
     * disabled one would repeat the same sentence thirty-three times. `TopicSettings` states it
     * once above the table and offers no per-key control, so both halves are read here — the
     * control's absence *and* the sentence, because an absence on its own is also what a broken
     * table looks like.
     */
    const editable = {
      "/api/v1/clusters/{clusterId}/topics/{topicName}/overview": {
        topic: {
          status: "ok",
          fetchedAt: "2026-09-06T00:00:00Z",
          data: {
            row: {
              name: "orders.v1",
              internal: false,
              partitionCount: 1,
              replicationFactor: 1,
              outOfSyncReplicas: 0,
              offlinePartitions: 0,
            },
            partitions: [],
          },
        },
      },
      "/api/v1/clusters/{clusterId}/topics/{topicName}/partitions": {
        partitions: {
          status: "ok",
          data: [
            {
              partition: 0,
              leader: 1,
              replicas: [{ broker: 1, leader: true, inSync: true }],
              earliestOffset: 0,
              latestOffset: 4,
            },
          ],
        },
      },
      /* One key, and one that is neither read-only nor sensitive: `TopicSettings` withholds the
         per-key control for both of those on its own, which would answer this case's question with
         a fact about the key rather than about the principal. */
      "/api/v1/clusters/{clusterId}/topics/{topicName}/config": {
        config: {
          status: "ok",
          data: {
            status: "entries",
            values: [
              {
                name: "retention.ms",
                value: "604800000",
                defaultValue: "604800000",
                source: "dynamic-topic",
                sensitive: false,
                readOnly: false,
                documentation: null,
              },
            ],
          },
        },
      },
    };

    /* One tab, mounted and settled. The two tabs are two mounts rather than one navigation because
       `useTabQuery` opens a tab's own query when the tab becomes current, and a case that walked
       from one to the other would be asserting the second answer through the first tab's cache. */
    const onTab = async (
      cluster: string,
      held: string,
      tab: string,
    ): Promise<ReturnType<typeof mount>> => {
      const host = topicsHost({
        at: `/clusters/${cluster}/topics/orders.v1?tab=${tab}`,
        permits: (action) => action.action === held,
        answers: editable,
      });
      const mounted = mount(host.view);
      await settle();
      return mounted;
    };

    const offered = async (
      cluster: string,
      held: string,
    ): Promise<Record<string, boolean | undefined>> => {
      const partitions = await onTab(cluster, held, "partitions");
      const add = [...partitions.container.querySelectorAll<HTMLButtonElement>("button")].find(
        (one) => one.textContent?.trim() === "Add partitions",
      );
      expect(add, "Add partitions must be on screen whoever is looking at it").toBeDefined();
      const mayAdd = add?.getAttribute("aria-disabled") !== "true";
      partitions.dispose();
      forgetQueries();

      const settings = await onTab(cluster, held, "settings");
      const state = {
        "Add partitions": mayAdd,
        "Edit a setting": [
          ...settings.container.querySelectorAll<HTMLButtonElement>("button"),
        ].some((one) => one.textContent?.trim() === "Edit"),
        "Told why not": settings.container.textContent?.includes(
          "You do not have permission to change this topic's settings.",
        ),
      };
      settings.dispose();
      forgetQueries();
      return state;
    };

    // The cluster trusts this principal to change the topic, and with nothing else.
    expect(await offered("editor-tab-cluster", "EDIT")).toEqual({
      "Add partitions": true,
      "Edit a setting": true,
      "Told why not": false,
    });
    /* A destructive grant and no edit: both controls close, and the screen says which permission
       is missing rather than looking like a topic with one key and no way to reach it. */
    expect(await offered("purger-tab-cluster", "MESSAGES_DELETE")).toEqual({
      "Add partitions": false,
      "Edit a setting": false,
      "Told why not": true,
    });
  });

  /**
   * The subject, which every permission case above this one is blind to.
   *
   * All of them hand the harness a `permits` of the shape `(action) => action.action === held`, so
   * the `name` is thrown away and so a gate asking `permits(action)` and a gate asking
   * `permits(action, "orders.v1")` answer identically to every one of them. All four of the topic
   * page's gates asked the subjectless question until wave 8, and `kernel/src/state/session.ts`
   * says in as many words which question that is: *"the right answer for a list heading and the
   * wrong one for a row's delete button"*.
   *
   * The principal is the one a **pattern** grant makes, which is the only kind the server issues:
   * every action, on `analytics.pageviews` and on nothing else. Asked "do they hold `TOPIC:DELETE`
   * on anything" the answer is yes — that is what `name === undefined` means here, and it is what
   * the server answers too — and asked about `orders.v1` it is no. So a page that names its topic
   * closes all four controls and a page that does not opens all four.
   *
   * Both directions, because a page that refused everybody would satisfy the first half alone. The
   * list screen's three gates are deliberately **not** included: a heading's `Create topic` is
   * about the cluster and naming a topic there would be the opposite mistake.
   */
  test("the topic page's write gates ask about this topic, not about the cluster", async () => {
    const pageAnswers = {
      "/api/v1/clusters/{clusterId}/topics/{topicName}/overview": {
        topic: {
          status: "ok",
          fetchedAt: "2026-09-06T00:00:00Z",
          data: {
            row: {
              name: "orders.v1",
              internal: false,
              partitionCount: 1,
              replicationFactor: 1,
              outOfSyncReplicas: 0,
              offlinePartitions: 0,
            },
            partitions: [],
          },
        },
      },
      "/api/v1/clusters/{clusterId}/topics/{topicName}/partitions": {
        partitions: {
          status: "ok",
          data: [
            {
              partition: 0,
              leader: 1,
              replicas: [{ broker: 1, leader: true, inSync: true }],
              earliestOffset: 0,
              latestOffset: 4,
            },
          ],
        },
      },
      "/api/v1/clusters/{clusterId}/topics/{topicName}/config": {
        config: {
          status: "ok",
          data: {
            status: "entries",
            values: [
              {
                name: "retention.ms",
                value: "604800000",
                defaultValue: "604800000",
                source: "dynamic-topic",
                sensitive: false,
                readOnly: false,
                documentation: null,
              },
            ],
          },
        },
      },
    };

    /** Every action, on the one topic this grant's pattern covers, and on nothing else. */
    const grantedOn =
      (topic: string): KuiContextValue["permits"] =>
      (_action, name) =>
        name === undefined || name === topic;

    const onTab = async (
      cluster: string,
      granted: string,
      tab: string,
    ): Promise<ReturnType<typeof mount>> => {
      const host = topicsHost({
        at: `/clusters/${cluster}/topics/orders.v1?tab=${tab}`,
        permits: grantedOn(granted),
        answers: pageAnswers,
      });
      const mounted = mount(host.view);
      await settle();
      return mounted;
    };

    const live = (within: ParentNode, label: string): boolean => {
      const button = [...within.querySelectorAll<HTMLButtonElement>("button")].find(
        (one) => one.textContent?.trim() === label,
      );
      expect(button, `${label} must be on screen whoever is looking at it`).toBeDefined();
      return button?.getAttribute("aria-disabled") !== "true";
    };

    const offered = async (
      cluster: string,
      granted: string,
    ): Promise<Record<string, boolean | undefined>> => {
      const overview = await onTab(cluster, granted, "overview");
      const header = {
        "Empty topic": live(overview.container, "Empty topic"),
        "Delete topic": live(overview.container, "Delete topic"),
      };
      overview.dispose();
      forgetQueries();

      const partitions = await onTab(cluster, granted, "partitions");
      const mayAdd = live(partitions.container, "Add partitions");
      partitions.dispose();
      forgetQueries();

      const settings = await onTab(cluster, granted, "settings");
      const mayEdit = [
        ...settings.container.querySelectorAll<HTMLButtonElement>("button"),
      ].some((one) => one.textContent?.trim() === "Edit");
      settings.dispose();
      forgetQueries();

      return { ...header, "Add partitions": mayAdd, "Edit a setting": mayEdit };
    };

    // Granted on a topic that is not this one: nothing on this page is theirs to press.
    expect(await offered("elsewhere-cluster", "analytics.pageviews")).toEqual({
      "Empty topic": false,
      "Delete topic": false,
      "Add partitions": false,
      "Edit a setting": false,
    });
    // And the same grant, on this topic: all four live, so the assertions above are about the
    // subject rather than about a page that refuses everybody.
    expect(await offered("here-cluster", "orders.v1")).toEqual({
      "Empty topic": true,
      "Delete topic": true,
      "Add partitions": true,
      "Edit a setting": true,
    });
  });
  test("a cluster that has not answered yet is not treated as read-only", async () => {
    /*
     * The other half of `useClusterReadOnly`, and the half a case that settles first cannot see.
     *
     * The accessor answers `false` until the cluster's own document arrives, which is the same
     * choice `useKui().permits` documents and makes for the same reason: disabling every write
     * control on a fact KUI does not have puts a screenful of refusals in front of every reader for
     * the length of one request, and the server is the authority either way. Made to answer `true`
     * while the question is out, `Create topic` flickers disabled on every single load — a control
     * that is dead when a page opens and alive a moment later is one an operator stops trusting.
     *
     * So the cluster document is **held** here and never released. The list answers, the table
     * draws, and the header action is read while the read-only question is still out.
     */
    withMeasuredRows();
    const held = new Promise<never>(() => {
      /* deliberately never settles: the question is out for the length of this case */
    });
    const host = topicsHost({
      at: "/clusters/unanswered-flag-cluster/topics",
      permits: true,
      answers: {
        "/api/v1/clusters/{clusterId}/topics": threeRows,
        "/api/v1/clusters/{clusterId}": () => held,
      },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    const create = [...container.querySelectorAll<HTMLButtonElement>("button")].find(
      (one) => one.textContent?.trim() === "Create topic",
    );
    expect(create, "the header action must be on screen before the cluster answers").toBeDefined();
    expect(create?.getAttribute("aria-disabled")).not.toBe("true");
    dispose();
  });

  test("a read-only cluster refuses the topic page's four write controls too", async () => {
    /*
     * The other four of `TopicsRoute`'s seven `writeBlockedReason` calls — `Empty topic`,
     * `Delete topic`, `Add partitions` and the settings editor — all of which hard-coded
     * `readOnly: false` alongside the three on the list screen. They are a separate case because
     * two of them live behind tabs, and because the list-screen case cannot fail for them: putting
     * the constant back on these four alone left it green.
     *
     * The principal holds everything again, so the only thing that can refuse these controls is the
     * cluster's own registration, and the sentence has to name it.
     */
    const answers = {
      "/api/v1/clusters/{clusterId}": {
        cluster: {
          id: "readonly-topic-cluster",
          name: "Read only",
          readOnly: true,
          bootstrapServers: "kafka:9092",
          origin: "file",
          security: {
            protocol: "PLAINTEXT",
            keystoreConfigured: false,
            truststoreConfigured: false,
          },
          summary: { status: "unavailable" },
        },
      },
      "/api/v1/clusters/{clusterId}/topics/{topicName}/overview": {
        topic: {
          status: "ok",
          fetchedAt: "2026-09-06T00:00:00Z",
          data: {
            row: {
              name: "orders.v1",
              internal: false,
              partitionCount: 1,
              replicationFactor: 1,
              outOfSyncReplicas: 0,
              offlinePartitions: 0,
            },
            partitions: [],
          },
        },
      },
      "/api/v1/clusters/{clusterId}/topics/{topicName}/partitions": {
        partitions: {
          status: "ok",
          data: [
            {
              partition: 0,
              leader: 1,
              replicas: [{ broker: 1, leader: true, inSync: true }],
              earliestOffset: 0,
              latestOffset: 4,
            },
          ],
        },
      },
      "/api/v1/clusters/{clusterId}/topics/{topicName}/config": {
        config: {
          status: "ok",
          data: {
            status: "entries",
            values: [
              {
                name: "retention.ms",
                value: "604800000",
                defaultValue: "604800000",
                source: "dynamic-topic",
                sensitive: false,
                readOnly: false,
                documentation: null,
              },
            ],
          },
        },
      },
    };

    const reasonUnder = async (within: ParentNode, label: string): Promise<string> => {
      const button = [...within.querySelectorAll<HTMLButtonElement>("button")].find(
        (one) => one.textContent?.trim() === label,
      );
      expect(button, `${label} must be on screen whoever is looking at it`).toBeDefined();
      if (button === undefined) return "";
      expect(button.getAttribute("aria-disabled")).toBe("true");
      button.dispatchEvent(new FocusEvent("focusin", { bubbles: true }));
      await settle();
      const described = button.getAttribute("aria-describedby") ?? "";
      const bubble = described === "" ? null : document.getElementById(described);
      expect(bubble, `${label} is disabled and says nothing about why`).not.toBeNull();
      return bubble?.textContent ?? "";
    };

    const header = topicsHost({
      at: "/clusters/readonly-topic-cluster/topics/orders.v1",
      permits: true,
      answers,
    });
    const page = mount(header.view);
    await settle();
    expect(await reasonUnder(page.container, "Empty topic")).toBe(
      "This cluster is configured read-only in KUI, so nothing here can empty this topic.",
    );
    expect(await reasonUnder(page.container, "Delete topic")).toBe(
      "This cluster is configured read-only in KUI, so nothing here can delete this topic.",
    );
    page.dispose();
    forgetQueries();

    const grow = topicsHost({
      at: "/clusters/readonly-topic-cluster/topics/orders.v1?tab=partitions",
      permits: true,
      answers,
    });
    const partitions = mount(grow.view);
    await settle();
    expect(await reasonUnder(partitions.container, "Add partitions")).toBe(
      "This cluster is configured read-only in KUI, so nothing here can add partitions to this " +
        "topic.",
    );
    partitions.dispose();
    forgetQueries();

    /* The settings editor's refusal is a sentence above the table rather than a disabled control —
       there is one Edit per key and repeating the reason thirty-three times would be unreadable —
       so this is read off the page's own text, and the absence of the control is read beside it
       because an absence on its own is also what a broken table looks like. */
    const settingsHost = topicsHost({
      at: "/clusters/readonly-topic-cluster/topics/orders.v1?tab=settings",
      permits: true,
      answers,
    });
    const settings = mount(settingsHost.view);
    await settle();
    expect(settings.container.textContent).toContain(
      "This cluster is configured read-only in KUI, so nothing here can change this topic's " +
        "settings.",
    );
    expect(
      [...settings.container.querySelectorAll<HTMLButtonElement>("button")].some(
        (one) => one.textContent?.trim() === "Edit",
      ),
    ).toBe(false);
    settings.dispose();
  });

  test("a plan the server withheld a token for cannot be confirmed", async () => {
    /*
     * ADR-045's refusal shape: a read-only cluster answers the *plan* — so the operator can see
     * exactly what would happen — and withholds the token. There is nothing to send, and inventing
     * one would produce a validation envelope that reads like a bug in KUI rather than a policy.
     *
     * `if (token === null) return;` was the whole of that rule and nothing could fail it: made to
     * confirm with an invented token, every case in this package stayed green. The dialog says why
     * in words, and no request leaves.
     */
    const host = topicsHost({
      at: "/clusters/read-only-cluster/topics/orders.v1",
      answers: {
        "/api/v1/clusters/{clusterId}/topics/{topicName}/overview": {
          topic: {
            status: "ok",
            fetchedAt: "2026-09-06T00:00:00Z",
            data: {
              row: {
                name: "orders.v1",
                internal: false,
                partitionCount: 6,
                replicationFactor: 1,
                outOfSyncReplicas: 0,
                offlinePartitions: 0,
              },
              partitions: [],
            },
          },
        },
        // A plan, in full, and no token beside it.
        "/api/v1/clusters/{clusterId}/topics/{topicName}/messages/purge/plan": {
          topic: "orders.v1",
          partitions: [{ partition: 0, lowWatermark: 0, highWatermark: 8 }],
          warnings: [],
        },
      },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    [...container.querySelectorAll("button")]
      .find((button) => button.textContent?.includes("Empty topic"))
      ?.click();
    await settle();

    const confirmation = document.querySelector(
      '[data-testid="planned-action-confirm"], [role="dialog"]',
    );
    // The preview is shown, and the reason it cannot be applied is in it.
    expect(confirmation?.textContent).toContain("read-only");

    const field = confirmation?.querySelector<HTMLInputElement>('input[type="text"]');
    if (field !== null && field !== undefined) {
      field.value = "orders.v1";
      field.dispatchEvent(new Event("input", { bubbles: true }));
    }
    await flush();

    [...(confirmation?.querySelectorAll<HTMLButtonElement>("button") ?? [])]
      .find((button) => button.textContent?.trim() === "Empty topic")
      ?.click();
    await settle();

    // Nothing was sent, so nothing was emptied — and no toast claims otherwise.
    expect(
      host.stub.calls.filter(
        (call) => call === "/api/v1/clusters/{clusterId}/topics/{topicName}/messages/purge",
      ),
    ).toHaveLength(0);
    expect(toasts()).toHaveLength(0);
    dispose();
  });

  test("a ?q= in the address filters the list", async () => {
    /*
     * The drawer's topic-prefix rows link at `…/topics?q=<prefix>`, and this screen used to seed
     * `DEFAULT_TOPIC_QUERY` and read the address only for `?tab=` — so the link was honest and the
     * destination listed the whole cluster. W4-06 owns the link; this is the reading.
     *
     * Asserted on the *request*, because that is where the filter is applied: a case that read the
     * search box would pass on a screen that filled the box and asked for everything.
     */
    const host = topicsHost({
      at: "/clusters/addressed-cluster/topics?q=orders.",
      answers: { "/api/v1/clusters/{clusterId}/topics": threeRows },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    const asked = host.stub.requests.find(
      (request) => request.path === "/api/v1/clusters/{clusterId}/topics",
    );
    expect(asked?.params.query?.["q"]).toBe("orders.");
    // And the box says what was asked for, so the screen and the server agree about the list.
    expect(container.querySelector<HTMLInputElement>(".kui-textfield__input")?.value).toBe("orders.");
    dispose();
  });

  test("a ?showInternal=true in the address lights the Internal chip and asks the server", async () => {
    // The one facet the wire has, so the one a link can carry. The chip and the parameter are the
    // same control (`isServerFacet`), which is why this is the facet the address is allowed to set.
    const host = topicsHost({
      at: "/clusters/internal-cluster/topics?showInternal=true",
      answers: { "/api/v1/clusters/{clusterId}/topics": threeRows },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    const asked = host.stub.requests.find(
      (request) => request.path === "/api/v1/clusters/{clusterId}/topics",
    );
    expect(asked?.params.query?.["showInternal"]).toBe(true);
    const lit = [...container.querySelectorAll("button")].find(
      (button) =>
        button.getAttribute("aria-pressed") === "true" ||
        button.getAttribute("aria-checked") === "true" ||
        button.getAttribute("aria-selected") === "true",
    );
    expect(lit?.textContent).toContain("Internal");
    dispose();
  });

  test("a second address change on a mounted route reaches the server", async () => {
    /*
     * The rule: **the address keeps being read, not read once.**
     *
     * The drawer's topic-prefix rows are ordinary links, so clicking a second one while this screen
     * is already on does not remount the route — it changes the search string underneath a live
     * component. Every other address case in this file mounts fresh at its address, and the seed
     * (`queryFromAddress(listLocation.search)`) answers those on its own; the effect that follows
     * later changes was live code that nothing exercised, and deleting it left this whole package
     * green.
     *
     * Asserted on the *requests*, in order, because that is where the following happens: a screen
     * that repainted its search box and kept asking for `orders.` would look identical.
     */
    const host = topicsHost({
      at: "/clusters/second-address-cluster/topics?q=orders.",
      answers: { "/api/v1/clusters/{clusterId}/topics": threeRows },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    const listCalls = (): readonly StubRequest[] =>
      host.stub.requests.filter(
        (request) => request.path === "/api/v1/clusters/{clusterId}/topics",
      );
    const first = listCalls();
    expect(first.at(-1)?.params.query?.["q"]).toBe("orders.");

    // The second prefix row, followed while this screen is the one on screen.
    host.goTo("/clusters/second-address-cluster/topics?q=analytics.");
    await settle();

    const after = listCalls();
    expect(after.length).toBeGreaterThan(first.length);
    expect(after.at(-1)?.params.query?.["q"]).toBe("analytics.");
    // And the box agrees with what was asked for, so the screen and the server describe one list.
    expect(container.querySelector<HTMLInputElement>(".kui-textfield__input")?.value).toBe(
      "analytics.",
    );
    dispose();
  });

  test("a ?showInternal=1 is not honoured", async () => {
    /*
     * `showInternal` is the one facet the wire has, and it has exactly one spelling. A screen that
     * accepted `1`, `yes` or `false` as truthy would be honouring a parameter the server has never
     * published — invented in the browser, and impossible to keep in step with anything.
     *
     * `?showInternal=false` is the sharper half: read as "present, therefore on", it would turn the
     * Internal chip on for an address that says in words to leave it off.
     */
    for (const spelling of ["1", "false"]) {
      const host = topicsHost({
        at: `/clusters/spelling-${spelling}-cluster/topics?showInternal=${spelling}`,
        answers: { "/api/v1/clusters/{clusterId}/topics": threeRows },
      });
      const { container, dispose } = mount(host.view);
      await settle();

      const asked = host.stub.requests.find(
        (request) => request.path === "/api/v1/clusters/{clusterId}/topics",
      );
      expect(asked?.params.query?.["showInternal"]).toBe(false);
      const lit = [...container.querySelectorAll("button")].find(
        (button) => button.getAttribute("aria-pressed") === "true",
      );
      expect(lit?.textContent).toContain("All");
      dispose();
      forgetQueries();
    }
  });

  test("a ?q= with surrounding space is trimmed before it is sent", async () => {
    /*
     * A prefix copied out of a terminal, or a link wrapped by a mail client, arrives with spaces
     * around it. Kafka topic names cannot contain a space, so ` orders. ` is a search that matches
     * nothing — and the screen would report that as "No topic matches that text", which is the
     * wrong answer given confidently.
     */
    const host = topicsHost({
      at: "/clusters/spaced-cluster/topics?q=%20orders.%20",
      answers: { "/api/v1/clusters/{clusterId}/topics": threeRows },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    const asked = host.stub.requests.find(
      (request) => request.path === "/api/v1/clusters/{clusterId}/topics",
    );
    expect(asked?.params.query?.["q"]).toBe("orders.");
    // And the box shows what was sent, rather than the padded text nobody asked the server for.
    expect(container.querySelector<HTMLInputElement>(".kui-textfield__input")?.value).toBe(
      "orders.",
    );
    dispose();
  });

  test("the statistics region names why a figure is unavailable", async () => {
    /*
     * Two different absences, and the tiles must not read the same for both. A document that
     * arrived with `partitionCount: null` is a cluster that could not add something up; a
     * statistics *request* that failed is KUI not having read the document at all — and the second
     * one owes the operator the sentence that says the list underneath is still trustworthy.
     *
     * The statistics path is deliberately unstubbed, which the harness answers as `unreachable`,
     * so the route's `failed` branch is the one under test. Asserted on the tile's own title, which
     * is where `StatTile` puts a `not-measured` figure's reason.
     */
    const host = topicsHost({
      at: "/clusters/no-statistics-cluster/topics",
      answers: { "/api/v1/clusters/{clusterId}/topics": threeRows },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    const why = container
      .querySelector('[data-testid="topic-stat-partitions"] .kui-tile__absent')
      ?.getAttribute("title");
    expect(why).toContain("could not read this cluster's topic totals");
    // The half the operator acts on: the rows below are the cluster's, so the page is still usable.
    expect(why).toContain("The list below is still this cluster's.");
    // And it is words rather than a zero, on every tile the document would have filled.
    expect(container.querySelector('[data-testid="topic-stat-topics"]')?.textContent).toContain(
      "not measured",
    );
    dispose();
  });

  test("a created topic is waited for until the list can see it", async () => {
    /*
     * `createTopics` returns when the **controller has accepted** the create, not when every broker
     * can list it. A screen that re-read the list once therefore shows the list without the topic
     * the operator just made, and the first thing they do is make it again.
     *
     * The stub answers the listing without the new topic for the first **three** reads and with it
     * afterwards, which is the race as it actually happens. Replacing the settle loop with a no-op
     * left every case in this package green, so the poll — a whole commit's worth of behaviour —
     * was carried by nothing.
     *
     * Three and not one, because one gates only that the loop runs at all: with the topic on the
     * second read, `attempt < 1` — a poll that gives up immediately, which is the bug this whole
     * mechanism exists to prevent — still finds it and every case here stays green. Withholding it
     * until the fourth read means the screen has to come back for it twice more, so the bound is
     * observable rather than merely the first iteration. The upper end of the bound is a different
     * rule and this does not gate it: see the note in `settleAfterCreate` for why it stops at all.
     */
    withMeasuredRows();
    const created = {
      name: "orders.new.v1",
      internal: false,
      partitionCount: 3,
      replicationFactor: 1,
      outOfSyncReplicas: 0,
      offlinePartitions: 0,
    };
    let listReads = 0;
    const host = topicsHost({
      at: "/clusters/created-cluster/topics",
      answers: {
        "/api/v1/clusters/{clusterId}/topics": (request: StubRequest) => {
          // The POST goes to the same templated path; only the reads are counted and answered.
          if (request.body !== undefined) return { name: created.name, partitions: 3 };
          listReads += 1;
          if (listReads <= 3) return threeRows;
          return {
            topics: {
              status: "ok",
              fetchedAt: "2026-09-06T00:00:00Z",
              data: {
                items: [...threeRows.topics.data.items, created],
                page: { page: 1, pageSize: 32, totalItems: 4 },
              },
            },
            incompleteTopics: 0,
          };
        },
      },
    });
    const { container, dispose } = mount(host.view);
    await settle();
    expect(container.textContent).not.toContain("orders.new.v1");

    [...container.querySelectorAll("button")]
      .find((button) => button.textContent?.trim() === "Create topic")
      ?.click();
    await settle();

    const field = document.querySelector<HTMLInputElement>('[role="dialog"] input[type="text"]');
    expect(field).not.toBeNull();
    if (field !== null) {
      field.value = created.name;
      field.dispatchEvent(new Event("input", { bubbles: true }));
    }
    await settle();

    [...document.querySelectorAll<HTMLButtonElement>('[role="dialog"] button')]
      .find((button) => button.textContent?.trim() === "Create topic")
      ?.click();
    await settle();

    /* Three poll intervals, plus room for the fetch the last one starts. The loop's own comment
       sets 500 ms as "long enough for the fetch the reload just started to have landed", and the
       stub withholds the topic for three reads, so the screen makes three passes before it can
       see it. Generous rather than tight: this is a real timer, and the suite's budget is 30 s. */
    await new Promise((resolve) => setTimeout(resolve, 2_200));
    await settle();

    // Four reads: the one the screen makes on mount, and the three the poll had to make.
    expect(listReads).toBeGreaterThan(3);
    expect(container.textContent).toContain("orders.new.v1");
    dispose();
  });

  test("the create poll stops asking rather than spinning", async () => {
    /*
     * The **upper** half of `settleAfterCreate`'s bound, and the half nothing could reach through
     * the screen. "A created topic is waited for until the list can see it" above gates the lower
     * half — that the screen comes back more than once — and it says in its own note that it does
     * not gate this one. Raising the bound from 6 to 100 draws exactly the same page: the only
     * difference is that a browser left on a cluster which never lists the topic keeps asking the
     * gateway twice a second for fifty seconds instead of three. That is a request loop nobody can
     * see, on a screen an operator has already walked away from.
     *
     * So the loop is a function with one caller — `settleAfterCreate`, twelve lines below it — and
     * this drives the function. Two facts, and the second is what makes it a gate rather than a
     * count: it **finishes**, and it made exactly six passes when it did. Raising the bound reddens
     * both, because the race below hands back `"still going"` long before a hundred passes are
     * done.
     *
     * The literal 6 rather than `CREATE_POLL_ATTEMPTS`: a case that imported the constant would
     * move with it, and the bound is the rule.
     */
    let reloads = 0;
    // Never listed, which is the state the bound exists for: a topic the cluster will not show.
    const polled = pollUntilListed(
      () => {
        reloads += 1;
      },
      () => false,
    );
    const outcome = await Promise.race([
      polled.then(() => "finished" as const),
      new Promise<"still going">((resolve) => setTimeout(() => resolve("still going"), 5_000)),
    ]);

    expect(outcome).toBe("finished");
    expect(reloads).toBe(6);
  });

  /**
   * The short-circuit the function is named for, which the case above is structurally blind to.
   *
   * It passes `() => false`, so the loop can only ever run to its bound — deleting
   * `if (listed()) return;` leaves it at six reloads and green. What that ships is a screen that
   * sees the new topic on its first re-read and goes on asking the gateway five more times over the
   * next two and a half seconds, for a row already on screen. `settleAfterCreate` runs after every
   * successful create, so it is every create on the cluster.
   *
   * `listed` answers true on the second pass rather than the first, because a loop that returned
   * after one reload unconditionally would satisfy a first-pass fixture and is a different defect.
   */
  test("the create poll stops at the first read that can see the topic", async () => {
    let reloads = 0;
    await pollUntilListed(
      () => {
        reloads += 1;
      },
      () => reloads >= 2,
    );

    expect(reloads).toBe(2);
  });

  /**
   * The interval, on a clock this case owns.
   *
   * Half a second is "long enough for the fetch the reload just started to have landed", and the
   * figure had nothing holding it: 500 and 50 draw the same page, and the difference is a gateway
   * asked ten times a second by every browser that has just created a topic. It was left ungated on
   * the reasoning that gating it means asserting wall-clock duration, which is sound about a real
   * clock and is why this one is fake — the assertion is that the loop waits for *that many
   * milliseconds*, made in two turns either side of the boundary, and it costs no wall time at all.
   */
  test("the create poll waits half a second between reads", async () => {
    vi.useFakeTimers();
    try {
      let reloads = 0;
      // Never listed, so nothing but the clock can end a pass.
      const polled = pollUntilListed(() => {
        reloads += 1;
      }, () => false);

      expect(reloads).toBe(1);
      await vi.advanceTimersByTimeAsync(499);
      expect(reloads).toBe(1);
      await vi.advanceTimersByTimeAsync(1);
      expect(reloads).toBe(2);

      // And it still finishes, rather than this case leaving a timer running into the next one.
      await vi.advanceTimersByTimeAsync(6 * 500);
      await polled;
      expect(reloads).toBe(6);
    } finally {
      vi.useRealTimers();
    }
  });

  test("the bulk bar's dismiss clears the ticks and not just the bar", async () => {
    /*
     * Dismissing the bar is the operator saying "never mind", and the ticks are the selection: a
     * dismiss that only hid the bar would leave rows ticked with no control over them, and the next
     * bulk action would run against a set nobody could see. `onDismiss` returning nothing left this
     * package green, which is why the ticks themselves are counted here and not the bar's absence.
     */
    withMeasuredRows();
    const host = topicsHost({
      at: "/clusters/dismissed-cluster/topics",
      answers: { "/api/v1/clusters/{clusterId}/topics": threeRows },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    container.querySelector<HTMLInputElement>('tbody input[type="checkbox"]')?.click();
    await settle();
    expect(
      [...container.querySelectorAll<HTMLInputElement>('tbody input[type="checkbox"]')].filter(
        (box) => box.checked,
      ),
    ).toHaveLength(1);

    const bar = container.querySelector('[data-testid="topic-bulk-bar"]');
    const dismiss = [...(bar?.querySelectorAll("button") ?? [])].find(
      (button) => button.textContent?.trim() === "Clear selection",
    );
    expect(dismiss).toBeDefined();
    dismiss?.click();
    await settle();

    expect(
      [...container.querySelectorAll<HTMLInputElement>('tbody input[type="checkbox"]')].filter(
        (box) => box.checked,
      ),
    ).toHaveLength(0);
    dispose();
  });
}, THIRTY_SECONDS); // see the note at the top of this file

describe("the consumers tab's columns", () => {
  test("no column heading is empty, and the dormant column's is Activity", async () => {
    /*
     * The a11y sweep is a whole-tree gate: it says *a* table somewhere has a blank `<th>`, and it
     * says it over 694 stories. This is the case that says which column — and it is the column that
     * shipped `header: \"\"` and produced all fourteen of the sweep's violations. See the long note
     * on the column itself for why the string is visible rather than visually hidden.
     */
    const { container, dispose } = mount(() => (
      <TopicConsumers
        rows={[
          {
            groupId: "order-fulfilment",
            state: "STABLE",
            members: 2,
            topicLag: 4,
            partitions: 6,
            dormant: true,
            totalLag: 4,
            topics: 1,
            coordinator: "kafka:9092",
          },
        ]}
        hrefFor={(groupId) => `/ui/consumer-groups/${groupId}`}
      />
    ));
    await flush();

    const headings = [...container.querySelectorAll("th")].map((cell) => cell.textContent?.trim());
    expect(headings).not.toContain("");
    expect(headings).toContain("Activity");
    expect(headings).toContain("Coordinator");
    dispose();
  });
});

describe("the export, and the sentence above the list", () => {
  afterEach(forgetQueries);

  test("a figure nobody measured is an empty cell and never a zero", () => {
    /*
     * A spreadsheet sums a column without asking. A `0` written for a topic whose size could not be
     * read becomes a cluster total that is quietly short — and unlike the screen, the file carries
     * no dash and no sentence to say so.
     */
    const csv = topicsCsv([rows[2] as TopicRow]);
    const cells = (csv.split("\r\n")[1] ?? "").split(",");
    // name, internal, partitions, RF, health, records, size, rate, policy.
    expect(cells).toEqual(['"shipments.v1"', '"no"', '"6"', '"2"', '"unknown"', '""', '""', '""', '""']);
  });

  test("a policy containing a comma does not shift every column after it", () => {
    // `compact,delete` is a real and common value of `cleanup.policy`, and an unquoted comma there
    // silently moves the rest of the row one column left. RFC 4180 quoting is what stops it.
    const csv = topicsCsv([
      { name: 'odd"name', internal: false, partitions: 1, replicationFactor: 1, health: "in-sync", cleanupPolicy: "compact,delete" },
    ]);
    expect(csv).toContain('"odd""name"');
    expect(csv).toContain('"compact,delete"');
    expect(csv.split("\r\n")[1]?.split('","')).toHaveLength(9);
  });

  test("the Export control hands the browser the rows it is showing", async () => {
    /*
     * The seam, rather than the formatter: the button is wired to the *page's* rows and to
     * `topicsCsv`, and a control that produced an empty file would look identical in a screenshot.
     * `createObjectURL` does not exist in a DOM with no layout engine, so it is supplied here and
     * the blob it is handed is read back.
     */
    let handed: Blob | undefined;
    const original = URL.createObjectURL;
    Object.defineProperty(URL, "createObjectURL", {
      configurable: true,
      value: (blob: Blob) => {
        handed = blob;
        return "blob:test";
      },
    });
    Object.defineProperty(URL, "revokeObjectURL", { configurable: true, value: () => undefined });

    /* The name the file lands under, which nothing asserted: `download("topics.csv", …)` is green
       on every case here, and two clusters exported into one folder then produce `topics.csv` and
       `topics (1).csv` — two files of Kafka topics with nothing on either to say which cluster it
       came from. The anchor is removed immediately after the click, so the name is captured from
       the click rather than read off the document afterwards. */
    let savedAs: string | undefined;
    const realClick = HTMLAnchorElement.prototype.click;
    HTMLAnchorElement.prototype.click = function saved(this: HTMLAnchorElement): void {
      savedAs = this.download;
    };

    const host = topicsHost({
      at: "/clusters/export-cluster/topics",
      answers: {
        "/api/v1/clusters/{clusterId}/topics": {
          topics: {
            status: "ok",
            fetchedAt: "2026-09-06T00:00:00Z",
            data: {
              items: [
                {
                  name: "orders.v1",
                  internal: false,
                  partitionCount: 6,
                  replicationFactor: 1,
                  outOfSyncReplicas: 0,
                  offlinePartitions: 0,
                  messageCount: 16,
                  sizeBytes: 5114,
                  cleanupPolicy: "delete",
                },
              ],
              page: { page: 1, pageSize: 32, totalItems: 1 },
            },
          },
          incompleteTopics: 0,
        },
      },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    [...container.querySelectorAll("button")]
      .find((button) => button.textContent?.trim() === "Export")
      ?.click();

    expect(handed).toBeDefined();
    const text = await (handed as Blob).text();
    expect(text).toContain('"orders.v1"');
    expect(text).toContain('"delete"');
    /* The whole header row, in order, so a file with rows and no columns is not mistaken for a
       working export — and so a column quietly dropped from `topicsCsv` is a red case rather than a
       spreadsheet whose figures have all shifted one place left. */
    expect(text.split("\r\n")[0]).toBe(
      '"topic","internal","partitions","replication factor","health","records","size bytes",' +
        '"messages per second","cleanup policy"',
    );
    /* And the anchor the download was made with is gone again. It is attached because a detached
       one is ignored in some browsers, which makes leaving it a real possibility rather than a
       tidiness point: one stray link per export accumulates in a tab somebody keeps open all day.
       Dropping `anchor.remove()` left every other case in this package green. */
    expect(document.querySelectorAll("a[download]")).toHaveLength(0);
    // Named for the cluster it came from, so two of these in one folder are two different files.
    expect(savedAs).toBe("export-cluster-topics.csv");

    HTMLAnchorElement.prototype.click = realClick;
    Object.defineProperty(URL, "createObjectURL", { configurable: true, value: original });
    dispose();
  });

  test("the voice line drops a clause it cannot measure rather than filling it with zero", () => {
    // Three figures from three documents, and each can be missing. `0 partitions` on a cluster
    // whose sweep was incomplete would be the never-zero rule broken in the most readable place on
    // the screen.
    expect(topicsVoice(3, 128, 1536)).toBe("3 of 128 topics match · 1,536 partitions");
    expect(topicsVoice(128, 128, 1536)).toBe("128 topics · 1,536 partitions");
    expect(topicsVoice(3, 128, undefined)).toBe("3 of 128 topics match");
    expect(topicsVoice(undefined, undefined, undefined)).toBe("");
  });
}, THIRTY_SECONDS); // see the note at the top of this file

describe("a destructive success says so", () => {
  afterEach(() => {
    clearToasts();
    forgetQueries();
  });

  test("deleting a topic raises a toast naming it", async () => {
    /*
     * The rule this package owes every destructive path: an action that worked says so. A screen
     * that navigates away in silence leaves the operator wondering whether the click registered,
     * and the answer they reach for is to do it again — which on a delete is the one repetition
     * that must never be encouraged.
     *
     * Driven through the real dialog: plan, type the name, confirm. A case that called `notify`
     * itself would assert that a toast library works.
     */
    const plan = {
      topic: "orders.v1",
      partitions: 6,
      records: 16,
      autoCreateEnabled: true,
      warnings: [],
      token: "tok-1",
      expiresAt: "2026-09-06T00:05:00Z",
    };
    const host = topicsHost({
      at: "/clusters/toast-cluster/topics/orders.v1",
      answers: {
        "/api/v1/clusters/{clusterId}/topics/{topicName}/overview": {
          topic: {
            status: "ok",
            fetchedAt: "2026-09-06T00:00:00Z",
            data: {
              row: { name: "orders.v1", internal: false, partitionCount: 6, replicationFactor: 1, outOfSyncReplicas: 0, offlinePartitions: 0 },
              partitions: [],
            },
          },
        },
        "/api/v1/clusters/{clusterId}/topics/{topicName}/deletion/plan": plan,
        "/api/v1/clusters/{clusterId}/topics/{topicName}": plan,
      },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    [...container.querySelectorAll("button")]
      .find((button) => button.textContent?.includes("Delete topic"))
      ?.click();
    await settle();

    const confirmation = document.querySelector('[data-testid="planned-action-confirm"], [role="dialog"]');
    const field = confirmation?.querySelector<HTMLInputElement>('input[type="text"]');
    if (field !== null && field !== undefined) {
      field.value = "orders.v1";
      field.dispatchEvent(new Event("input", { bubbles: true }));
    }
    await flush();

    const confirm = [...(confirmation?.querySelectorAll("button") ?? [])].find(
      (button) => button.textContent?.trim() === "Delete topic",
    );
    const invalidation = vi.spyOn(sharedQueries, "invalidateWhere");
    confirm?.click();
    await settle();

    const affected = invalidation.mock.calls.map(([predicate]) => predicate);
    expect(affected.some((matches) => matches('topics|toast-cluster|{"page":1}'))).toBe(true);
    expect(affected.some((matches) => matches('topic-statistics|toast-cluster'))).toBe(true);
    expect(affected.some((matches) => matches('topic-overview|toast-cluster|orders.v1'))).toBe(true);
    expect(affected.some((matches) => matches('topics|other-cluster|{"page":1}'))).toBe(false);
    invalidation.mockRestore();

    expect(toasts().map((toast) => toast.title)).toContain("orders.v1 deleted");
    // The sentence an operator is least likely to have thought of, carried into the confirmation.
    expect(toasts()[0]?.message).toContain("recreate");
    dispose();
  });

  test("a purge that partly refused raises a warning toast", async () => {
    /*
     * The tone is the whole content of this rendering, and it was a constant nothing could fail:
     * hard-coding `tone: "success"` here left all 127 cases in this package green, because the only
     * asserted purge toast was the one where nothing refused.
     *
     * A purge is per partition and the broker answers per partition, so "four emptied, two refused"
     * is an ordinary outcome rather than an error envelope — there is no failure for the dialog to
     * show, and the toast is the only place the operator is told the difference. A green toast over
     * it is the reassuring rendering of the state that needs attention.
     */
    const plan = {
      topic: "orders.v1",
      partitions: [
        { partition: 0, lowWatermark: 0, highWatermark: 8 },
        { partition: 1, lowWatermark: 0, highWatermark: 8 },
      ],
      warnings: [],
      token: "tok-purge",
      expiresAt: "2026-09-06T00:05:00Z",
    };
    const host = topicsHost({
      at: "/clusters/half-purged-cluster/topics/orders.v1",
      answers: {
        "/api/v1/clusters/{clusterId}/topics/{topicName}/overview": {
          topic: {
            status: "ok",
            fetchedAt: "2026-09-06T00:00:00Z",
            data: {
              row: {
                name: "orders.v1",
                internal: false,
                partitionCount: 6,
                replicationFactor: 1,
                outOfSyncReplicas: 0,
                offlinePartitions: 0,
              },
              partitions: [],
            },
          },
        },
        "/api/v1/clusters/{clusterId}/topics/{topicName}/messages/purge/plan": plan,
        "/api/v1/clusters/{clusterId}/topics/{topicName}/messages/purge": {
          result: {
            purged: [{ partition: 0 }, { partition: 1 }, { partition: 2 }, { partition: 3 }],
            failed: [
              { partition: 4, reason: "leader unavailable" },
              { partition: 5, reason: "leader unavailable" },
            ],
          },
        },
      },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    [...container.querySelectorAll("button")]
      .find((button) => button.textContent?.includes("Empty topic"))
      ?.click();
    await settle();

    const confirmation = document.querySelector(
      '[data-testid="planned-action-confirm"], [role="dialog"]',
    );
    const field = confirmation?.querySelector<HTMLInputElement>('input[type="text"]');
    expect(field).toBeDefined();
    if (field !== null && field !== undefined) {
      field.value = "orders.v1";
      field.dispatchEvent(new Event("input", { bubbles: true }));
    }
    await flush();

    [...(confirmation?.querySelectorAll("button") ?? [])]
      .find((button) => button.textContent?.trim() === "Empty topic")
      ?.click();
    await settle();

    const raised = toasts().at(-1);
    expect(raised?.title).toBe("orders.v1 emptied");
    // Both halves, from the server's own answer rather than from the plan.
    expect(raised?.message).toContain("4 emptied");
    expect(raised?.message).toContain("2 refused");
    // The rendering this case exists for.
    expect(raised?.tone).toBe("warning");
    dispose();
  });

  test("a purge that refused nothing raises a success toast", async () => {
    // The other half of the same expression: with both branches asserted the tone cannot be a
    // constant of either value, which is the only shape of assertion that closes a ternary.
    const plan = {
      topic: "orders.v1",
      partitions: [{ partition: 0, lowWatermark: 0, highWatermark: 8 }],
      warnings: [],
      token: "tok-purge",
      expiresAt: "2026-09-06T00:05:00Z",
    };
    const host = topicsHost({
      at: "/clusters/wholly-purged-cluster/topics/orders.v1",
      answers: {
        "/api/v1/clusters/{clusterId}/topics/{topicName}/overview": {
          topic: {
            status: "ok",
            fetchedAt: "2026-09-06T00:00:00Z",
            data: {
              row: {
                name: "orders.v1",
                internal: false,
                partitionCount: 6,
                replicationFactor: 1,
                outOfSyncReplicas: 0,
                offlinePartitions: 0,
              },
              partitions: [],
            },
          },
        },
        "/api/v1/clusters/{clusterId}/topics/{topicName}/messages/purge/plan": plan,
        "/api/v1/clusters/{clusterId}/topics/{topicName}/messages/purge": {
          result: { purged: [{ partition: 0 }], failed: [] },
        },
      },
    });
    const { container, dispose } = mount(host.view);
    await settle();

    [...container.querySelectorAll("button")]
      .find((button) => button.textContent?.includes("Empty topic"))
      ?.click();
    await settle();

    const confirmation = document.querySelector(
      '[data-testid="planned-action-confirm"], [role="dialog"]',
    );
    const field = confirmation?.querySelector<HTMLInputElement>('input[type="text"]');
    if (field !== null && field !== undefined) {
      field.value = "orders.v1";
      field.dispatchEvent(new Event("input", { bubbles: true }));
    }
    await flush();

    [...(confirmation?.querySelectorAll("button") ?? [])]
      .find((button) => button.textContent?.trim() === "Empty topic")
      ?.click();
    await settle();

    const raised = toasts().at(-1);
    expect(raised?.message).toContain("1 partition emptied");
    expect(raised?.tone).toBe("success");
    dispose();
  });

  test("a bulk outcome names both halves, because a set can fail in the middle", () => {
    // "3 topics deleted" over a selection of five leaves the operator to discover the other two.
    expect(bulkSentence("deleted", { done: ["a", "b"], failed: [] })).toBe("2 topics deleted");
    expect(
      bulkSentence("deleted", { done: ["a"], failed: [{ topic: "b", reason: "Not permitted." }] }),
    ).toBe("1 topic deleted. 1 refused: b — Not permitted.");
  });
}, THIRTY_SECONDS); // see the note at the top of this file
