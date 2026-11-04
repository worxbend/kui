/**
 * The consumer screens' tests.
 *
 * Two kinds, deliberately separated. The pure ones exercise the rules — which chip a state gets,
 * when lag turns amber, what sentence the voice picks, whether the form refuses — and need no DOM.
 * The rendered ones exercise the things that are only true once markup exists: that an unreadable
 * lag draws an em dash rather than a zero, that a narrow window drops a column instead of hiding
 * it, and that pressing Preview always changes the screen.
 *
 * Everything a Solid 2 test has to remember is in `testing.ts`: `flush()` before asserting, and
 * dispose at the end.
 */

import { describe, expect, it, vi } from "vitest";
import { flush } from "solid-js";
import { createRouter, memoryHistory } from "@solidjs/router";
import { KuiProvider } from "@kui/kernel";
import type { KuiApiClient } from "@kui/api";
import { describeViolations, findViolations, mount, testContext } from "./testing.js";
import Consumers, { GroupsScreen } from "./ConsumersRoute.jsx";
import { coordinatorAddress } from "./data.js";
import {
  LAG_WARN_ABOVE,
  UNREADABLE_STATE_CHIP,
  groupsVoice,
  healthOf,
  lagLevel,
  stateChip,
} from "./model.js";
import { EMPTY_RESET_FORM, partitionLag, recordsMoved, resetRequestOf, subscriptions, targetOption, type ResetTarget } from "./detail.js";
import { GroupList } from "./GroupList.jsx";
import { GroupDetail } from "./GroupDetail.jsx";
import { ResetWizard, scopeSentence } from "./ResetWizard.jsx";
import { DEGRADED_GROUPS, NO_OP_PLAN, SAMPLE_GROUPS, SAMPLE_GROUP_DETAIL, SAMPLE_PLAN } from "./fixtures.js";

const noop = (): void => {};

describe("group state chips", () => {
  it("says Rebalancing for both rebalancing states", () => {
    expect(stateChip("PREPARING_REBALANCE").label).toBe("Rebalancing");
    expect(stateChip("COMPLETING_REBALANCE").label).toBe("Rebalancing");
  });

  it("keeps Empty neutral, because a batch job with no members is not a problem", () => {
    expect(stateChip("EMPTY").tone).toBe("neutral");
  });

  it("draws a state that could not be read as a dash, never as the word Unknown", () => {
    // Kafka's own UNKNOWN is a fact the coordinator reported; a dash is KUI failing to ask.
    expect(stateChip("UNKNOWN").label).toBe("Unknown");
    expect(UNREADABLE_STATE_CHIP.label).toBe("—");
  });
});

describe("lag levels", () => {
  it("puts the screenshot's own figures either side of the boundary", () => {
    expect(lagLevel(333)).toBe("normal");
    expect(lagLevel(3_861)).toBe("warning");
  });

  it("treats the threshold itself as still ordinary", () => {
    expect(lagLevel(LAG_WARN_ABOVE)).toBe("normal");
    expect(lagLevel(LAG_WARN_ABOVE + 1)).toBe("warning");
  });
});

describe("the voice", () => {
  const total = (count: number) => ({ kind: "total" as const, total: count });

  it("keeps the aside only while everything is healthy", () => {
    expect(groupsVoice({ kind: "healthy", count: total(14), rebalancing: 1 })).toBe(
      "14 groups. One is rebalancing again. We don't judge.",
    );
    expect(groupsVoice({ kind: "lagging", count: total(14), behind: 2 })).not.toContain("judge");
    expect(groupsVoice({ kind: "unavailable" })).toBe("Consumer group data is unavailable.");
  });

  it("describes a lagging page as lagging even while something is also rebalancing", () => {
    // Ordering is the rule: the operator has to act on the lag, not on the rebalance.
    expect(healthOf(SAMPLE_GROUPS, 0, 6).kind).toBe("lagging");
  });

  it("reports missing coordinators ahead of everything else, because the rows are then incomplete", () => {
    expect(healthOf(SAMPLE_GROUPS, 2, 6).kind).toBe("incomplete");
  });

  it("counts the cluster's groups and not the rows in hand", () => {
    // Six rows on the page, ninety on the cluster. The sentence is about the cluster: this is
    // screenshot `04`'s own case, where `14 groups` sits over six drawn rows.
    const health = healthOf(SAMPLE_GROUPS, 0, 90);
    // The two branches that carry no count are excluded by name rather than by a truthiness check,
    // so a third one added later is a type error here instead of a silently skipped assertion.
    expect(
      health.kind !== "unavailable" && health.kind !== "counting" && health.count,
    ).toEqual({ kind: "total", total: 90 });
    expect(groupsVoice(health)).toContain("90 groups");
  });

  it("says the question is still out rather than stating a count nobody has asked for", () => {
    /*
     * `ConsumersRoute` starts its total at `null`, so before this branch existed the first paint of
     * every visit read "0 groups on this page, of an unstated total" — the sentence a server that
     * *answered without a total* earns, printed over a request that had not come back. The two are
     * different facts and the screen now draws them differently.
     */
    expect(groupsVoice(healthOf([], 0, null, true))).toContain("Asking this cluster");
    expect(groupsVoice(healthOf([], 0, null, true))).not.toContain("unstated total");
    expect(groupsVoice(healthOf([], 0, null, true))).not.toMatch(/\b0 groups\b/);
  });

  it("keeps stating the count it already has while a refresh is out", () => {
    // A poll that is in flight must not blank a sentence about figures still on screen: the count
    // the server gave is the best answer anybody has until a better one arrives.
    expect(groupsVoice(healthOf(SAMPLE_GROUPS, 0, 90, true))).toContain("90 groups");
  });

  it("states a counted zero as a sentence, not as an arithmetic aside", () => {
    // `0 groups. Nothing is rebalancing. Rare, and welcome.` is arithmetic over an empty set.
    expect(groupsVoice(healthOf([], 0, 0))).toBe("No consumer groups on this cluster.");
  });

  it("says the total is unstated rather than printing the page's own length as one", () => {
    // A server that carried no `totalItems` leaves the screen knowing only what it can see. It
    // says so: publishing `6` as the cluster's figure would be a measurement nobody made.
    const health = healthOf(SAMPLE_GROUPS, 0, null);
    const line = groupsVoice(health);
    expect(line).toContain("on this page");
    expect(line).toContain("unstated total");
  });
});

describe("the group list", () => {
  it("draws every row from screenshot 04 with its state and its lag", async () => {
    const { container, dispose } = mount(() => <GroupList rows={SAMPLE_GROUPS} hrefFor={(id) => `/g/${id}`} />);
    await flush();
    expect(container.querySelectorAll("tbody tr")).toHaveLength(6);
    expect(container.textContent).toContain("payments-processor");
    expect(container.textContent).toContain("Rebalancing");
    // The cell carries the figure plus the visually-hidden "(high lag)" a screen reader hears.
    expect(container.querySelector('[data-testid="group-clickstream-etl-lag"]')?.textContent).toContain("3,861");
    dispose();
  });

  it("gives a group's name a real href, so copy-link and open-in-new-tab work", async () => {
    const { container, dispose } = mount(() => <GroupList rows={SAMPLE_GROUPS} hrefFor={(id) => `/g/${id}`} />);
    await flush();
    const link = container.querySelector<HTMLAnchorElement>(".kui-cg-name__link");
    expect(link?.getAttribute("href")).toBe("/g/payments-processor");
    dispose();
  });

  it("draws an unreadable lag as a dash and a caught-up group as a zero", async () => {
    const { container, dispose } = mount(() => <GroupList rows={DEGRADED_GROUPS} hrefFor={(id) => `/g/${id}`} />);
    await flush();
    // The row whose lag could not be computed has no figure at all, only the dash and its reason.
    expect(container.querySelector('[data-testid="group-unreadable-lag-lag"]')).toBeNull();
    const text = container.textContent ?? "";
    expect(text).toContain("—");
    // And a zero is still a zero somewhere on the healthy fixture, not a dash.
    const zero = mount(() => <GroupList rows={SAMPLE_GROUPS} hrefFor={(id) => `/g/${id}`} />);
    await flush();
    expect(zero.container.querySelector('[data-testid="group-payments-processor-lag"]')?.textContent).toBe("0");
    zero.dispose();
    dispose();
  });

  it("says something different when a filter matched nothing than when there is nothing", async () => {
    const filtered = mount(() => (
      <GroupList rows={[]} hrefFor={() => "#"} failure={{ kind: "filtered", term: "payments", onClear: noop }} />
    ));
    await flush();
    expect(filtered.container.textContent).toContain("Nothing matched payments.");
    expect(filtered.container.textContent).toContain("Clear filter");
    filtered.dispose();

    const empty = mount(() => <GroupList rows={[]} hrefFor={() => "#"} />);
    await flush();
    expect(empty.container.textContent).toContain("No consumer groups yet.");
    expect(empty.container.textContent).not.toContain("Clear filter");
    empty.dispose();
  });

  it("says a filter matched nothing in the voice, and does not call the cluster empty", async () => {
    /*
     * Both halves of the filtered voice were added together and only the counted-zero half was
     * asserted. Guard the filtered branch off and the fall-through reaches the *other* sentence —
     * `No consumer groups on this cluster.` — over a page whose zero rows are the filter's doing.
     * An operator searching for a name they mistyped is then told the cluster has no consumer
     * groups at all, which is a statement about the cluster made from a statement about the box
     * they are typing in, and it is the one thing this screen's voice is written not to do.
     *
     * `totalItems={0}` because that is what the server answers a search that matched nothing, and
     * it is the value that makes the fall-through say the wrong sentence rather than a vaguer one.
     */
    const { container, dispose } = mount(() => (
      <GroupList
        rows={[]}
        totalItems={0}
        hrefFor={() => "#"}
        failure={{ kind: "filtered", term: "payments", onClear: noop }}
      />
    ));
    await flush();

    const voice = container.querySelector('[data-testid="consumer-groups-head"]')?.textContent ?? "";
    expect(voice).toContain("No consumer group on this cluster is named like payments.");
    expect(voice).not.toContain("No consumer groups on this cluster.");
    dispose();
  });

  it("keeps a frame, a code and a retry when the request failed", async () => {
    const { container, dispose } = mount(() => (
      <GroupList
        rows={[]}
        hrefFor={() => "#"}
        failure={{ kind: "unavailable", message: "Consumer group data is unavailable.", code: "UPSTREAM_UNAVAILABLE", onRetry: noop }}
      />
    ));
    await flush();
    expect(container.textContent).toContain("UPSTREAM_UNAVAILABLE");
    expect(container.textContent).toContain("Retry");
    // The frame survives: the card still names itself.
    expect(container.querySelector('[data-testid="consumer-groups-card"]')).not.toBeNull();
    dispose();
  });

  it("drops COORDINATOR and TOPICS in a narrow window, rather than hiding them", async () => {
    // Dropped from the array and not `display: none`: a hidden column is still in the
    // accessibility tree and still counted in the row, so a screen-reader user would hear a cell
    // nobody can see. jsdom has no `matchMedia`, so the narrow window is stubbed.
    const original = window.matchMedia;
    Object.defineProperty(window, "matchMedia", {
      configurable: true,
      value: (query: string) => ({
        matches: true,
        media: query,
        addEventListener: () => {},
        removeEventListener: () => {},
      }),
    });
    try {
      const { container, dispose } = mount(() => <GroupList rows={SAMPLE_GROUPS} hrefFor={(id) => `/g/${id}`} />);
      await flush();
      const headers = [...container.querySelectorAll("th")].map((th) => th.textContent);
      expect(headers).not.toContain("Coordinator");
      expect(headers).not.toContain("Topics");
      // The three that never go.
      expect(headers).toContain("Group id");
      expect(headers).toContain("State");
      expect(headers).toContain("Lag");
      dispose();
    } finally {
      if (original === undefined) Reflect.deleteProperty(window, "matchMedia");
      else Object.defineProperty(window, "matchMedia", { configurable: true, value: original });
    }
  });

  it("prints the server's total beside the heading, not the number of rows on screen", async () => {
    /*
     * The seam this case exists for. Six rows are drawn; the cluster has ninety. `GroupList` is
     * where the server's figure meets the sentence, and before this the sentence was computed from
     * `rows.length` — so it was always true of the table and always wrong about the cluster the
     * moment there was a second page.
     */
    const { container, dispose } = mount(() => (
      <GroupList
        rows={SAMPLE_GROUPS}
        totalItems={90}
        page={1}
        pageSize={6}
        onPage={noop}
        hrefFor={(id) => `/g/${id}`}
      />
    ));
    await flush();
    expect(container.querySelectorAll("tbody tr")).toHaveLength(6);
    const voice = container.querySelector('[data-testid="consumer-groups-head"]')?.textContent ?? "";
    expect(voice).toContain("90 groups");
    expect(voice).not.toContain("6 groups");
    dispose();
  });

  it("offers a way to the next page, and says which rows of how many are on screen", async () => {
    const asked: number[] = [];
    const { container, dispose } = mount(() => (
      <GroupList
        rows={SAMPLE_GROUPS}
        totalItems={90}
        page={2}
        pageSize={6}
        onPage={(next) => asked.push(next)}
        hrefFor={(id) => `/g/${id}`}
      />
    ));
    await flush();
    const control = container.querySelector('[data-testid="consumer-groups-pagination"]');
    expect(control?.textContent).toContain("of 90");
    control?.querySelector<HTMLButtonElement>('[aria-label="Next page"]')?.click();
    await flush();
    expect(asked).toEqual([3]);
    dispose();
  });

  it("draws no paging control for a caller that cannot answer one", async () => {
    // A story or a detail panel hands this a fixed array. A paginator there would be a control that
    // does nothing, which teaches people that the controls on this screen do nothing.
    const { container, dispose } = mount(() => <GroupList rows={SAMPLE_GROUPS} hrefFor={(id) => `/g/${id}`} />);
    await flush();
    expect(container.querySelector('[data-testid="consumer-groups-pagination"]')).toBeNull();
    dispose();
  });

  it("enables Next on a full page with no total, and disables it on a short one", async () => {
    /*
     * `mayHaveMore` is the only signal there is when the server carried no total, and it was
     * asserted nowhere: mutated to `() => true` the whole package stayed green, and `Pagination`
     * ignores it entirely once a total is known — so every existing paging case ran past it.
     *
     * A page that came back full may have a successor; a short page is the end of the list. The
     * two mounts differ only in how many rows they hold, so the assertion cannot pass on anything
     * except the length comparison the rule is.
     */
    const full = mount(() => (
      <GroupList rows={SAMPLE_GROUPS} page={1} pageSize={6} onPage={noop} hrefFor={(id) => `/g/${id}`} />
    ));
    await flush();
    const forward = (root: HTMLElement): HTMLButtonElement | null =>
      root.querySelector<HTMLButtonElement>('[data-testid="consumer-groups-pagination"] [aria-label="Next page"]');
    expect(forward(full.container)?.disabled).toBe(false);
    full.dispose();

    // Three rows of a page that holds six: there is nowhere forward to go, and offering it would
    // send the operator to an empty page and leave them there.
    const short = mount(() => (
      <GroupList
        rows={SAMPLE_GROUPS.slice(0, 3)}
        page={1}
        pageSize={6}
        onPage={noop}
        hrefFor={(id) => `/g/${id}`}
      />
    ));
    await flush();
    expect(forward(short.container)?.disabled).toBe(true);
    short.dispose();
  });

  it("renders no address at all for a group whose coordinator the wire did not carry", async () => {
    // Not `broker 1`. A broker id is a number dressed as an address: it is nowhere an operator can
    // point `kafka-topics.sh`, and on screen it is indistinguishable from a coordinator that
    // answered. The cell draws the dash and its reason instead.
    const { container, dispose } = mount(() => <GroupList rows={DEGRADED_GROUPS} hrefFor={(id) => `/g/${id}`} />);
    await flush();
    const text = container.textContent ?? "";
    expect(text).not.toContain("broker 1");
    expect(text).not.toMatch(/broker \d/);
    expect(text).toContain("coordinator unavailable");
    dispose();
  });

  it("draws no paginator for a caller that cannot say how big a page is", async () => {
    /*
     * The control used to appear on `onPage` alone and take `props.pageSize ?? props.rows.length`
     * for its page size — the array's own length, which is the one quantity the prop's own doc four
     * lines above it forbids. On the last page of a list that is smaller than a page, so every
     * figure the control drew was arithmetic over how many rows happened to come back.
     *
     * A caller that can answer "go to page 4" knows what it asked for and supplies both. One that
     * cannot gets no control, rather than one whose numbers are made up.
     */
    const { container, dispose } = mount(() => (
      <GroupList rows={SAMPLE_GROUPS} totalItems={90} onPage={noop} hrefFor={(id) => `/g/${id}`} />
    ));
    await flush();
    expect(container.querySelector('[data-testid="consumer-groups-pagination"]')).toBeNull();
    dispose();
  });

  it("counts rows by the page the screen asked for, not by the rows that came back", async () => {
    // A short last page: three rows of a page that holds six, on page three of ninety. Read off
    // `rows.length` the range would say 7–9; read off the request it says 13–15, which is where
    // these rows actually sit in the list.
    const { container, dispose } = mount(() => (
      <GroupList
        rows={SAMPLE_GROUPS.slice(0, 3)}
        totalItems={90}
        page={3}
        pageSize={6}
        onPage={noop}
        hrefFor={(id) => `/g/${id}`}
      />
    ));
    await flush();
    const paging = container.querySelector('[data-testid="consumer-groups-pagination"]');
    expect(paging?.textContent).toContain("Showing 13–15 of 90");
    dispose();
  });

  it("says the count is still being asked for while the first answer is out", async () => {
    // The screen's first paint. Skeleton rows, and a sentence that describes a question rather than
    // a cluster — this is the state `ConsumersRoute` opens in on every visit.
    const { container, dispose } = mount(() => (
      <GroupList rows={[]} loading hrefFor={(id) => `/g/${id}`} />
    ));
    await flush();
    const head = container.querySelector('[data-testid="consumer-groups-head"]');
    const voice = head?.textContent ?? "";
    expect(voice).toContain("Asking this cluster");
    expect(voice).not.toContain("unstated total");
    dispose();
  });

  it("has no axe violations", async () => {
    const { container, dispose } = mount(() => <GroupList rows={SAMPLE_GROUPS} hrefFor={(id) => `/g/${id}`} onOpen={noop} />);
    await flush();
    const violations = await findViolations(container);
    expect(describeViolations(violations)).toBe("");
    dispose();
  });
});

/**
 * The list screen, mounted the way the shell mounts it.
 *
 * These are the cases the component tests above cannot reach. `GroupList` can be handed a total and
 * a page by hand; what nobody was watching is whether the *route* reads them off the server's
 * answer and puts the page number into the request. Wave 2 shipped eight rules whose tests composed
 * the rule inside the test file; this file mounts `GroupsScreen`, gives it a client, and looks at
 * what the client was asked for.
 */
describe("the consumer groups screen", () => {
  /** One page of an eighty-group cluster, in the shape the gateway sends. */
  function listing(page: number, pageSize: number, totalItems: number | null): unknown {
    const items = Array.from({ length: Math.min(pageSize, 3) }, (_, index) => ({
      groupId: `group-${(page - 1) * pageSize + index}`,
      state: "STABLE",
      members: 1,
      topics: 1,
      coordinatorId: 1,
      coordinatorHost: "kafka",
      coordinatorPort: 9092,
      totalLag: 0,
      excludedPartitions: 0,
      incomplete: null,
    }));
    return {
      groups: {
        status: "ok",
        data: {
          items,
          page: { page, pageSize, ...(totalItems === null ? {} : { totalItems }) },
        },
      },
      incompleteCoordinators: 0,
    };
  }

  /** Records every list request and answers it; the lag poll gets a quiet, incremental answer. */
  function stubbed(totalItems: number | null = 80): {
    readonly api: KuiApiClient;
    readonly pages: { page?: number; pageSize?: number }[];
    /** The `group` scope each lag request carried, in order. `[]` for a cluster-wide one. */
    readonly lagScopes: readonly string[][];
  } {
    const pages: { page?: number; pageSize?: number }[] = [];
    const lagScopes: string[][] = [];
    const get = vi.fn(
      async (
        path: string,
        init?: {
          params?: { query?: { page?: number; pageSize?: number; group?: readonly string[] } };
        },
      ) => {
        if (path.endsWith("/lag")) {
          lagScopes.push([...(init?.params?.query?.group ?? [])]);
          const quiet = { changed: [], gone: [], token: "t", nextPollMs: 30_000, full: false };
          return { ok: true, value: quiet };
        }
        const query = init?.params?.query ?? {};
        pages.push(query);
        return { ok: true, value: listing(query.page ?? 1, query.pageSize ?? 16, totalItems) };
      },
    );
    return {
      api: { get, post: get, put: get, delete: get, patch: get, raw: {} } as unknown as KuiApiClient,
      pages,
      lagScopes,
    };
  }

  function open(api: KuiApiClient, cluster: string) {
    return mount(() => (
      <KuiProvider value={testContext(api)}>
        <GroupsScreen clusterId={cluster} />
      </KuiProvider>
    ));
  }

  /**
   * Waits for the screen to stop moving.
   *
   * A single `flush()` is not enough here, and the reason is worth writing down: the answer travels
   * through the query cache, so it crosses two promises and two of Solid's scheduling turns before
   * it reaches the table. A fixed number of flushes chosen by trial is the shape that starts
   * passing for the wrong reason later, so this drives it until the DOM stops changing.
   */
  async function settle(container: HTMLElement): Promise<void> {
    let previous = "";
    for (let turn = 0; turn < 20; turn += 1) {
      await flush();
      const now = container.innerHTML;
      if (now === previous && turn > 1) return;
      previous = now;
    }
  }

  it("prints the count the server gave, over the rows the server sent", async () => {
    const { api } = stubbed(80);
    const { container, dispose } = open(api, "count-cluster");
    await settle(container);
    expect(container.querySelectorAll("tbody tr")).toHaveLength(3);
    expect(container.querySelector('[data-testid="consumer-groups-head"]')?.textContent).toContain(
      "80 groups",
    );
    dispose();
  });

  it("asks the server for page 2 when the operator asks for page 2", async () => {
    const { api, pages } = stubbed(80);
    const { container, dispose } = open(api, "paging-cluster");
    await settle(container);
    expect(pages[0]?.page).toBe(1);

    container
      .querySelector('[data-testid="consumer-groups-pagination"]')
      ?.querySelector<HTMLButtonElement>('[aria-label="Next page"]')
      ?.click();
    await settle(container);
    // A second request, for page 2. Not the first page's rows re-sliced in the browser: this list
    // is one page of the cluster and the browser holds no other page to slice.
    expect(pages.map((one) => one.page)).toEqual([1, 2]);
    expect(container.textContent).toContain("group-16");
    dispose();
  });

  it("says the total is unstated when the server sent none, not the row count", async () => {
    const { api } = stubbed(null);
    const { container, dispose } = open(api, "silent-cluster");
    await settle(container);
    const voice = container.querySelector('[data-testid="consumer-groups-head"]')?.textContent ?? "";
    expect(voice).toContain("on this page");
    expect(voice).not.toBe("3 groups. Nothing is rebalancing. Rare, and welcome.");
    dispose();
  });

  it("does not state a count on its first paint, before anything has answered", async () => {
    /*
     * The route holds `total` at `null` until an answer arrives, and `null` was read by the voice
     * as "the server sent no total" — so the first frame of every visit to this screen carried a
     * confident sentence about a cluster nobody had asked yet. Driven here through the route rather
     * than by handing `GroupList` a `loading` prop, because it is the route that owns the `null`.
     */
    const never = new Promise<never>(() => undefined);
    const get = vi.fn(() => never);
    const api = {
      get,
      post: get,
      put: get,
      delete: get,
      patch: get,
      raw: {},
    } as unknown as KuiApiClient;

    const { container, dispose } = open(api, "unanswered-cluster");
    await flush();

    const head = container.querySelector('[data-testid="consumer-groups-head"]');
    const voice = head?.textContent ?? "";
    expect(voice).toContain("Asking this cluster");
    expect(voice).not.toContain("unstated total");
    expect(voice).not.toMatch(/\d+ groups\./);
    dispose();
  });

  it("scopes the very first lag request to the page, not just the ones after it", async () => {
    /*
     * The ordering only the route has, and the reason this case is here rather than beside
     * `pollLag`'s own tests: the rows are written by one effect and the poll is started by the
     * next, in the same tick, and Solid 2 commits a signal write on a microtask. A first request
     * composed synchronously therefore named an empty page — which the endpoint reads as "every
     * group on the cluster", the exact thing the scoping exists to stop. A unit test that hands
     * `pollLag` an already-assigned variable cannot see it; a browser did.
     */
    const { api, lagScopes } = stubbed(80);
    const { container, dispose } = open(api, "seed-scope-cluster");
    await settle(container);
    // A macrotask, because the seeding request is deliberately scheduled onto one. `flush()` drains
    // microtasks and would read this assertion before the request was made.
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(lagScopes.length).toBeGreaterThan(0);
    const drawn = [...container.querySelectorAll(".kui-cg-name__link")].map(
      (link) => link.textContent ?? "",
    );
    expect(drawn.length).toBeGreaterThan(0);
    expect(lagScopes[0]).toEqual(drawn);
    dispose();
  });

  it("prints the coordinator's address, which is what the wire now carries", async () => {
    const { api } = stubbed(80);
    const { container, dispose } = open(api, "coordinator-cluster");
    await settle(container);
    expect(container.textContent).toContain("kafka:9092");
    dispose();
  });
});

describe("the consumer groups route, moved from one cluster to another", () => {
  /*
   * The shell sends `/consumer-groups` to one route entry, and the router keeps a route's component
   * mounted when only its parameters change. So a history step from one cluster's list to
   * another's lands on the screen that drew the first one, and everything that screen holds about
   * its cluster — the page it is on, the rows it last drew — is still there unless the route
   * starts the screen again for the new cluster.
   */
  const FIRST = "alpha";
  const SECOND = "bravo";

  function listingFor(cluster: string, page: number, pageSize: number, totalItems: number): unknown {
    const first = (page - 1) * pageSize;
    const count = Math.max(0, Math.min(pageSize, totalItems - first));
    return {
      groups: {
        status: "ok",
        data: {
          items: Array.from({ length: count }, (_, index) => ({
            groupId: `${cluster}-group-${String(first + index)}`,
            state: "STABLE",
            members: 1,
            topics: 1,
            coordinatorId: 1,
            coordinatorHost: "kafka",
            coordinatorPort: 9092,
            totalLag: 0,
            excludedPartitions: 0,
            incomplete: null,
          })),
          page: { page, pageSize, totalItems },
        },
      },
      incompleteCoordinators: 0,
    };
  }

  it("starts the second cluster's list on its first page, with none of the first's rows", async () => {
    let release = (): void => {};
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const asked: { cluster: string; page: number | undefined }[] = [];
    const get = async (
      path: string,
      init?: {
        params?: { path?: { clusterId?: string }; query?: { page?: number; pageSize?: number } };
      },
    ) => {
      const cluster = init?.params?.path?.clusterId ?? "";
      if (path.endsWith("/lag")) {
        return { ok: true, value: { changed: [], gone: [], token: "t", nextPollMs: 30_000, full: false } };
      }
      const query = init?.params?.query ?? {};
      asked.push({ cluster, page: query.page });
      // The second cluster's answer is held, so the case can look at the screen while it is out.
      if (cluster === SECOND) await held;
      // Eighty groups on the first cluster; three on the second, fewer than one page.
      const total = cluster === FIRST ? 80 : 3;
      return { ok: true, value: listingFor(cluster, query.page ?? 1, query.pageSize ?? 16, total) };
    };
    const api = { get, post: get, put: get, delete: get, patch: get, raw: {} } as unknown as KuiApiClient;

    const history = memoryHistory(`/ui/clusters/${FIRST}/consumer-groups`);
    const Router = createRouter({
      routes: [{ path: "/clusters/:clusterId/consumer-groups", component: Consumers }],
      base: "/ui",
      history,
      scrollRestoration: false,
    });
    const { container, dispose } = mount(() => (
      <KuiProvider value={testContext(api)}>
        <Router />
      </KuiProvider>
    ));
    const settle = async (): Promise<void> => {
      for (let turn = 0; turn < 20; turn += 1) await flush();
    };
    await settle();

    container
      .querySelector('[data-testid="consumer-groups-pagination"]')
      ?.querySelector<HTMLButtonElement>('[aria-label="Next page"]')
      ?.click();
    await settle();
    expect(asked).toEqual([
      { cluster: FIRST, page: 1 },
      { cluster: FIRST, page: 2 },
    ]);
    expect(container.textContent).toContain(`${FIRST}-group-16`);

    history.set({ value: `/ui/clusters/${SECOND}/consumer-groups` });
    await settle();
    // While the second cluster's answer is out, nothing on the screen is the first cluster's.
    expect(container.textContent).not.toContain(`${FIRST}-group-`);

    release();
    await settle();
    // Page 1 of the cluster now on screen, not the first cluster's page 2 — which on a cluster of
    // three groups is a page that does not exist, drawn as "No consumer groups yet".
    expect(asked.filter((one) => one.cluster === SECOND)).toEqual([{ cluster: SECOND, page: 1 }]);
    expect(container.textContent).toContain(`${SECOND}-group-0`);
    expect(container.textContent).not.toContain(`${FIRST}-group-`);

    dispose();
  });
});

describe("the coordinator's address", () => {
  it("is host and port together, or nothing", () => {
    expect(coordinatorAddress("kafka", 9092)).toBe("kafka:9092");
    // Half an address reads as a value truncated in transit, which is worse than none.
    expect(coordinatorAddress("kafka", null)).toBeNull();
    expect(coordinatorAddress(null, 9092)).toBeNull();
    expect(coordinatorAddress("", 9092)).toBeNull();
  });
});

describe("the group detail page", () => {
  it("links every assignment topic to that topic's page", async () => {
    const { container, dispose } = mount(() => (
      <GroupDetail
        group={SAMPLE_GROUP_DETAIL}
        listHref="/groups"
        topicHref={(topic) => `/topics/${encodeURIComponent(topic)}`}
        reset={{
          plan: async () => ({ ok: false, problem: "no" }),
          apply: async () => ({ ok: false, problem: "no" }),
        }}
      />
    ));
    await flush();

    const link = container.querySelector<HTMLAnchorElement>(
      '[data-testid="group-assignments-table"] tbody a',
    );
    expect(link?.textContent).toBe(SAMPLE_GROUP_DETAIL.offsets[0]?.topic);
    expect(link?.getAttribute("href")).toBe(
      `/topics/${encodeURIComponent(SAMPLE_GROUP_DETAIL.offsets[0]?.topic ?? "")}`,
    );
    dispose();
  });

  it("carries no voice line, because it is a page about one object", async () => {
    const { container, dispose } = mount(() => (
      <GroupDetail group={SAMPLE_GROUP_DETAIL} listHref="/groups" reset={{ plan: async () => ({ ok: false, problem: "no" }), apply: async () => ({ ok: false, problem: "no" }) }} />
    ));
    await flush();
    expect(container.querySelector(".kui-page-head__voice")).toBeNull();
    dispose();
  });

  it("prints a stalled commit rate as a word rather than as a zero beside a large lag", async () => {
    const { container, dispose } = mount(() => (
      <GroupDetail
        group={{ ...SAMPLE_GROUP_DETAIL, pace: 0 }}
        listHref="/groups"
        reset={{ plan: async () => ({ ok: false, problem: "no" }), apply: async () => ({ ok: false, problem: "no" }) }}
      />
    ));
    await flush();
    expect(container.querySelector('[data-testid="group-pace"]')?.textContent).toBe("Stalled");
    dispose();
  });

  it("drops the static-id column when no member has one", async () => {
    const { container, dispose } = mount(() => (
      <GroupDetail group={SAMPLE_GROUP_DETAIL} listHref="/groups" reset={{ plan: async () => ({ ok: false, problem: "no" }), apply: async () => ({ ok: false, problem: "no" }) }} />
    ));
    await flush();
    const headers = [...container.querySelectorAll('[data-testid="group-members-table"] th')].map((th) => th.textContent);
    expect(headers).not.toContain("Static id");
    dispose();
  });

  it("refuses to offer delete while the group still has members, and says why", async () => {
    const { container, dispose } = mount(() => (
      <GroupDetail
        group={SAMPLE_GROUP_DETAIL}
        listHref="/groups"
        onDelete={noop}
        reset={{ plan: async () => ({ ok: false, problem: "no" }), apply: async () => ({ ok: false, problem: "no" }) }}
      />
    ));
    await flush();
    // `aria-disabled`, not `disabled`: the button stays focusable so a keyboard user can reach it
    // and read the reason, which a `disabled` element does not let them do.
    const button = container.querySelector<HTMLButtonElement>(".kui-page-head__actions button");
    expect(button?.getAttribute("aria-disabled")).toBe("true");
    // The reason reaches the operator through the button's tooltip, which opens on focus as well
    // as on hover — a disabled control with no reason is worse than no control. The bubble is
    // portalled to `document.body`, so it is not inside the mounted container.
    button?.dispatchEvent(new FocusEvent("focusin", { bubbles: true }));
    await flush();
    expect(document.body.querySelector('[role="tooltip"]')?.textContent).toContain("Stop its consumers first.");
    dispose();
  });

  it("offers forgetting the offsets once per topic the group holds them on", async () => {
    /*
     * `CG-005`'s control. The endpoint behind it names a *topic* — `DELETE …/offsets?topic=` —
     * so the row is per topic, and the figure beside each one is the number of partitions this
     * group holds a committed position on there. That figure is what the receipt afterwards is
     * read against, which is why it is said before the click as well as after it.
     */
    const asked: string[] = [];
    const { container, dispose } = mount(() => (
      <GroupDetail
        group={SAMPLE_GROUP_DETAIL}
        listHref="/groups"
        onForgetOffsets={(topic) => asked.push(topic)}
        reset={{ plan: async () => ({ ok: false, problem: "no" }), apply: async () => ({ ok: false, problem: "no" }) }}
      />
    ));
    await flush();

    const section = container.querySelector<HTMLElement>('[data-testid="group-forget-offsets"]');
    expect(section).not.toBeNull();
    const rows = [...(section?.querySelectorAll("li") ?? [])];
    // The two topics this group holds offsets on, in the order `subscriptions` sorts them.
    expect(rows.map((row) => row.querySelector(".kui-cg-forget__topic")?.textContent)).toEqual([
      "clickstream",
      "sessions",
    ]);
    // Four partitions of `clickstream` and one of `sessions`, spelled out rather than left as a
    // bare number beside a topic name where it reads as a version or a replica count.
    expect(rows[0]?.textContent).toContain("4 partitions held");
    expect(rows[1]?.textContent).toContain("1 partition held");

    rows[1]?.querySelector("button")?.click();
    await flush();
    // The topic on the row that was pressed, not the first one and not the whole group.
    expect(asked).toEqual(["sessions"]);
    dispose();
  });

  it("draws no forget control at all for a group that holds no offsets", async () => {
    // There is nothing to forget, and a heading over an empty list reads as a control that failed
    // to load rather than as an action that does not apply.
    const { container, dispose } = mount(() => (
      <GroupDetail
        group={{ ...SAMPLE_GROUP_DETAIL, offsets: [] }}
        listHref="/groups"
        onForgetOffsets={noop}
        reset={{ plan: async () => ({ ok: false, problem: "no" }), apply: async () => ({ ok: false, problem: "no" }) }}
      />
    ));
    await flush();
    expect(container.querySelector('[data-testid="group-forget-offsets"]')).toBeNull();
    dispose();
  });

  it("shows the forget control disabled, with the reason, for somebody who may not use it", async () => {
    // Disabled and present rather than absent: a control that vanishes teaches an operator the
    // product cannot do this at all, where the truth is that this account may not.
    const { container, dispose } = mount(() => (
      <GroupDetail
        group={SAMPLE_GROUP_DETAIL}
        listHref="/groups"
        forgetRefusal="You do not have permission to change this group's committed offsets."
        reset={{ plan: async () => ({ ok: false, problem: "no" }), apply: async () => ({ ok: false, problem: "no" }) }}
      />
    ));
    await flush();

    const button = container.querySelector<HTMLButtonElement>(
      '[data-testid="group-forget-offsets"] button',
    );
    // `aria-disabled` and not `disabled`, for the reason the delete action gives: a `disabled`
    // element cannot be focused, so the reason below can never be read from a keyboard.
    expect(button?.getAttribute("aria-disabled")).toBe("true");
    button?.dispatchEvent(new FocusEvent("focusin", { bubbles: true }));
    await flush();
    expect(document.body.querySelector('[role="tooltip"]')?.textContent).toContain(
      "You do not have permission to change this group's committed offsets.",
    );
    dispose();
  });

  it("still says why forgetting is refused when the caller supplied no sentence", async () => {
    /*
     * The `??` fallback beside `forgetRefusal`, which nothing asserted: emptied, this ships a
     * destructive control that is disabled and will not say why — the one class §3.7 rules out and
     * the class this component's own header names. The two sentences differ deliberately and the
     * difference is not stylistic: the caller's names *this group*, and a component that was handed
     * no callback has not been told which permission is missing, so its own can only speak about
     * the cluster. Wording them identically is what made the route's ternary unobservable in
     * wave 5.
     *
     * Read through the button's own `aria-describedby` rather than through the first
     * `[role="tooltip"]` in the document: bubbles are portalled into `body` and an earlier case's
     * can still be there, so a query across the body can answer with somebody else's sentence.
     */
    const { container, dispose } = mount(() => (
      <GroupDetail
        group={SAMPLE_GROUP_DETAIL}
        listHref="/groups"
        reset={{
          plan: async () => ({ ok: false, problem: "no" }),
          apply: async () => ({ ok: false, problem: "no" }),
        }}
      />
    ));
    await flush();

    const button = container.querySelector<HTMLButtonElement>(
      '[data-testid="group-forget-offsets"] button',
    );
    expect(button?.getAttribute("aria-disabled")).toBe("true");
    button?.dispatchEvent(new FocusEvent("focusin", { bubbles: true }));
    await flush();
    const described = button?.getAttribute("aria-describedby") ?? "";
    const bubble = described === "" ? null : document.getElementById(described);
    expect(bubble, "a disabled control must carry a reason a keyboard can reach").not.toBeNull();
    expect(bubble?.textContent).toBe(
      "You do not have permission to change committed offsets on this cluster.",
    );
    dispose();
  });

  it("has no axe violations with the forget rows on the page", async () => {
    // The new section is a list of rows each carrying a destructive control, which is exactly the
    // shape that produces `list`, `button-name` and `aria-*` findings if it is assembled loosely.
    const { container, dispose } = mount(() => (
      <GroupDetail
        group={SAMPLE_GROUP_DETAIL}
        listHref="/groups"
        onDelete={noop}
        onForgetOffsets={noop}
        reset={{ plan: async () => ({ ok: false, problem: "no" }), apply: async () => ({ ok: false, problem: "no" }) }}
      />
    ));
    await flush();
    const violations = await findViolations(container);
    expect(describeViolations(violations)).toBe("");
    dispose();
  });

  it("computes a partition's lag only when both offsets are there", () => {
    expect(partitionLag({ topic: "t", partition: 0, committed: 10, endOffset: 25, memberId: null })).toBe(15);
    expect(partitionLag({ topic: "t", partition: 0, committed: null, endOffset: 25, memberId: null })).toBeNull();
    expect(partitionLag({ topic: "t", partition: 0, committed: 10, endOffset: null, memberId: null })).toBeNull();
    // A commit read a moment before the end offset can cross it; a negative lag reads as a bug.
    expect(partitionLag({ topic: "t", partition: 0, committed: 30, endOffset: 25, memberId: null })).toBe(0);
  });

  it("lists the group's topics with their partitions, sorted", () => {
    expect(subscriptions(SAMPLE_GROUP_DETAIL)).toEqual([
      { topic: "clickstream", partitions: [0, 1, 2, 3] },
      { topic: "sessions", partitions: [0] },
    ]);
  });
});

describe("the reset form's refusals", () => {
  const partitions = [0, 1, 2];

  it("refuses an empty topic with a sentence rather than with silence", () => {
    const answer = resetRequestOf(EMPTY_RESET_FORM, partitions);
    expect(answer.ok).toBe(false);
    expect(answer.ok === false && answer.problem).toContain("Choose a topic");
  });

  it("refuses a shift of zero, which would move nothing while looking like an action", () => {
    const answer = resetRequestOf({ ...EMPTY_RESET_FORM, topic: "t", target: "SHIFT_BY", shiftBy: "0" }, partitions);
    expect(answer.ok).toBe(false);
  });

  it("accepts a negative shift, because rewinding is the common case", () => {
    const answer = resetRequestOf({ ...EMPTY_RESET_FORM, topic: "t", target: "SHIFT_BY", shiftBy: "-4200" }, partitions);
    expect(answer.ok && answer.request.shiftBy).toBe(-4_200);
  });

  it("refuses a topic with no partitions rather than sending a request that resets nothing", () => {
    const answer = resetRequestOf({ ...EMPTY_RESET_FORM, topic: "t" }, []);
    expect(answer.ok).toBe(false);
  });

  it("names the extra field each target needs, so the form cannot show the wrong one", () => {
    expect(targetOption("EARLIEST").parameter).toBeNull();
    expect(targetOption("TIMESTAMP").parameter).toBe("timestamp");
    expect(targetOption("TIMESTAMP").hint).toContain("moves to its end");
  });

  it("refuses loudly for a target no option describes, rather than handing back nothing", () => {
    /*
     * `RESET_TARGETS` is data and `targetOption` reads it, so the union and the array agree by
     * convention and not by the type system. Dropping a member from the array leaves every call
     * here type-correct and answers `undefined` — and the caller reads `.parameter` off it, so a
     * dropped `TIMESTAMP` would put the wizard in front of an operator with the timestamp field
     * missing and the form still willing to send a `TIMESTAMP` request.
     *
     * The cast is the point of the case: it is the shape the array being edited would produce, and
     * it is the only way to reach a branch the compiler otherwise proves unreachable.
     */
    expect(() => targetOption("SOMETHING_ELSE" as ResetTarget)).toThrow(/SOMETHING_ELSE/);

    // And through the form builder, which is where the product reads it: a request is refused
    // rather than composed without the parameter its target needs.
    expect(() =>
      resetRequestOf({ ...EMPTY_RESET_FORM, topic: "t", target: "SOMETHING_ELSE" as ResetTarget }, partitions),
    ).toThrow();
  });

  it("counts the records a plan moves, ignoring direction", () => {
    expect(recordsMoved(SAMPLE_PLAN)).toBe(4_998 + 0 + 11_300);
  });

  it("says how many partitions are in scope, and says so plainly when there are none", () => {
    expect(scopeSentence(0)).toContain("no offsets");
    expect(scopeSentence(1)).toBe("1 partition will be moved.");
    expect(scopeSentence(12)).toBe("12 partitions will be moved.");
  });
});

describe("the reset wizard", () => {
  const topics = [{ topic: "clickstream", partitions: [0, 1, 2, 3] }];

  async function openWizard(overrides: Partial<Parameters<typeof ResetWizard>[0]> = {}) {
    const mounted = mount(() => (
      <ResetWizard
        topics={topics}
        plan={async () => ({ ok: true, plan: SAMPLE_PLAN })}
        apply={async () => ({ ok: true, receipt: SAMPLE_PLAN })}
        formatTime={() => "09:19"}
        {...overrides}
      />
    ));
    await flush();
    click(mounted.container, "button");
    await flush();
    return mounted;
  }

  it("shows the plan when Preview is pressed — the defect this rewrite exists to fix", async () => {
    const { container, dispose } = await openWizard();
    expect(container.querySelector('[data-testid="group-reset-plan"]')).toBeNull();

    clickText(container, "Preview the plan");
    await flush();
    // Two frames: the promise resolves on a microtask, then Solid flushes the write.
    await flush();

    const plan = container.querySelector('[data-testid="group-reset-plan-table"]');
    expect(plan).not.toBeNull();
    expect(container.querySelector('[data-testid="group-reset-summary"]')?.textContent).toContain("4 partitions move");
    expect(container.querySelector('[data-testid="group-reset-warnings"]')?.textContent).toContain("KIP-122");
    dispose();
  });

  it("says why, rather than nothing, when the form cannot be turned into a request", async () => {
    const { container, dispose } = await openWizard({ topics: [] });
    clickText(container, "Preview the plan");
    await flush();
    const problem = container.querySelector('[data-testid="group-reset-problem"]');
    expect(problem?.textContent).toContain("Choose a topic");
    expect(problem?.getAttribute("role")).toBe("alert");
    dispose();
  });

  it("goes back to the form with the reason when the server refuses to plan", async () => {
    const { container, dispose } = await openWizard({ plan: async () => ({ ok: false, problem: "The cluster is read-only." }) });
    clickText(container, "Preview the plan");
    await flush();
    await flush();
    expect(container.querySelector('[data-testid="group-reset-problem"]')?.textContent).toBe("The cluster is read-only.");
    expect(container.querySelector('[data-testid="group-reset-form"]')).not.toBeNull();
    dispose();
  });

  it("offers nothing to apply when the plan changes nothing", async () => {
    const { container, dispose } = await openWizard({ plan: async () => ({ ok: true, plan: NO_OP_PLAN }) });
    clickText(container, "Preview the plan");
    await flush();
    await flush();
    expect(container.textContent).toContain("Every partition is already where this reset would put it.");
    expect(hasText(container, "Apply this plan")).toBe(false);
    dispose();
  });

  it("stays on the plan, and does not re-plan, when the token has expired", async () => {
    const { container, dispose } = await openWizard({ apply: async () => ({ ok: false, problem: "That plan has expired." }) });
    clickText(container, "Preview the plan");
    await flush();
    await flush();
    clickText(container, "Apply this plan");
    await flush();
    await flush();
    expect(container.querySelector('[data-testid="group-reset-problem"]')?.textContent).toBe("That plan has expired.");
    expect(container.querySelector('[data-testid="group-reset-plan-table"]')).not.toBeNull();
    dispose();
  });

  it("shows what the broker wrote, not what the browser asked for", async () => {
    const { container, dispose } = await openWizard();
    clickText(container, "Preview the plan");
    await flush();
    await flush();
    clickText(container, "Apply this plan");
    await flush();
    await flush();
    expect(container.querySelector('[data-testid="group-reset-receipt-table"]')).not.toBeNull();
    dispose();
  });

  it("does not open at all for somebody who may not reset offsets, and says why", async () => {
    const mounted = mount(() => (
      <ResetWizard
        topics={topics}
        permitted={false}
        refusal="You do not have permission to reset offsets on this cluster."
        plan={async () => ({ ok: true, plan: SAMPLE_PLAN })}
        apply={async () => ({ ok: true, receipt: SAMPLE_PLAN })}
      />
    ));
    await flush();
    const button = mounted.container.querySelector<HTMLButtonElement>("button");
    expect(button?.getAttribute("aria-disabled")).toBe("true");
    button?.dispatchEvent(new FocusEvent("focusin", { bubbles: true }));
    await flush();
    expect(document.body.querySelector('[role="tooltip"]')?.textContent).toContain("do not have permission");
    mounted.dispose();
  });

  it("still says why a reset is refused when the caller supplied no sentence", async () => {
    /*
     * `ResetWizard`'s own `??` fallback, the twin of `GroupDetail`'s forget one and ungated for the
     * same reason: every case that reached the refusal branch passed a `refusal` in, so the default
     * behind it could be emptied with this package green. What that ships is a disabled destructive
     * control with nothing to say, which reads as a broken product rather than as a permission the
     * account does not hold.
     *
     * Cluster-scoped rather than group-scoped, deliberately: a component handed `permitted={false}`
     * and no sentence has not been told which group's permission is missing, and two identical
     * sentences would make the caller's gate — the `mayReset()` ternary in `GroupRoute` — invisible
     * again.
     */
    const mounted = mount(() => (
      <ResetWizard
        topics={topics}
        permitted={false}
        plan={async () => ({ ok: true, plan: SAMPLE_PLAN })}
        apply={async () => ({ ok: true, receipt: SAMPLE_PLAN })}
      />
    ));
    await flush();
    const button = mounted.container.querySelector<HTMLButtonElement>("button");
    expect(button?.getAttribute("aria-disabled")).toBe("true");
    button?.dispatchEvent(new FocusEvent("focusin", { bubbles: true }));
    await flush();
    const described = button?.getAttribute("aria-describedby") ?? "";
    const bubble = described === "" ? null : document.getElementById(described);
    expect(bubble, "a disabled control must carry a reason a keyboard can reach").not.toBeNull();
    expect(bubble?.textContent).toBe(
      "You do not have permission to reset offsets on this cluster.",
    );
    mounted.dispose();
  });

  it("has no axe violations with a plan on screen", async () => {
    const { container, dispose } = await openWizard();
    clickText(container, "Preview the plan");
    await flush();
    await flush();
    const violations = await findViolations(container);
    expect(describeViolations(violations)).toBe("");
    dispose();
  });
});

function click(root: ParentNode, selector: string): void {
  root.querySelector<HTMLElement>(selector)?.click();
}

function buttons(root: ParentNode): HTMLButtonElement[] {
  return [...root.querySelectorAll<HTMLButtonElement>("button")];
}

function clickText(root: ParentNode, text: string): void {
  const button = buttons(root).find((one) => (one.textContent ?? "").includes(text));
  if (button === undefined) throw new Error(`No button reading "${text}". Buttons: ${buttons(root).map((b) => b.textContent).join(" | ")}`);
  button.click();
}

function hasText(root: ParentNode, text: string): boolean {
  return buttons(root).some((one) => (one.textContent ?? "").includes(text));
}
