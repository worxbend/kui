/**
 * The topic write paths, driven through the interface against a real broker.
 *
 * These are the tests worth having. Every one of them exercises a control that changes somebody's
 * cluster, and each asserts the *sentence* the screen shows as well as the outcome — because on a
 * destructive confirmation the words are the feature. A dialog that deletes the right records while
 * saying the wrong number is a dialog nobody should trust.
 */
import { test, expect, CLUSTER, removeTopic, scratchTopic, type KuiApi } from "./fixtures";

// One shared cluster, and these write to it. Serial, so a create and a delete cannot interleave.
test.describe.configure({ mode: "serial" });

test.describe("creating a topic", () => {
  const name = scratchTopic("create");

  test.afterAll(async ({ playwright }) => {
    const request = await playwright.request.newContext();
    await request.dispose();
  });

  test("says what cannot be undone before it asks for a name", async ({ page }) => {
    await page.goto(`/ui/clusters/${CLUSTER}/topics`);
    await page.getByRole("button", { name: /create topic/i }).first().click();

    const dialog = page.getByRole("dialog");
    await expect(dialog).toBeVisible();

    /*
     * The two sentences that are the point of this dialog. Partitions can be added and never
     * removed, and adding them changes which partition a key hashes to — which breaks per-key
     * ordering for anything relying on it. Replication factor cannot be changed from KUI at all.
     * Both are permanent decisions taken in ten seconds by somebody who came here to do something
     * else, and a dialog that does not say so is three boxes.
     */
    await expect(dialog).toContainText(/added later but never removed/i);
    await expect(dialog).toContainText(/cannot be changed/i);
  });

  test("refuses a name Kafka reserves, before a round trip", async ({ page }) => {
    await page.goto(`/ui/clusters/${CLUSTER}/topics`);
    await page.getByRole("button", { name: /create topic/i }).first().click();
    const dialog = page.getByRole("dialog");
    await dialog.getByLabel(/^name/i).fill("..");
    await expect(dialog).toContainText(/reserves/i);
  });

  test("creates it, and the list shows it", async ({ page, api }) => {
    await page.goto(`/ui/clusters/${CLUSTER}/topics`);
    await page.getByRole("button", { name: /create topic/i }).first().click();
    const dialog = page.getByRole("dialog");
    await dialog.getByLabel(/^name/i).fill(name);
    await dialog.getByRole("button", { name: /create topic/i }).click();

    /*
     * The dialog closing is asserted first, and separately, because it is what distinguishes the two
     * failures. A create the server refused leaves the dialog open with the reason in it — and
     * asserting only on the list gave "the list does not contain this name" and a screenshot of a
     * perfectly ordinary topic list, with the actual explanation sitting in a dialog the assertion
     * never looked at.
     */
    await expect(dialog, "the create dialog should close on success; if it is open it carries the reason it did not")
      .toBeHidden({ timeout: 20_000 });

    await expect(page.locator("body")).toContainText(name, { timeout: 20_000 });
    await removeTopic(api, name);
  });
});

test.describe("emptying a topic", () => {
  /*
   * One record, produced over HTTP before the dialog is opened.
   *
   * The sentence this case is about is the server's *measured* warning, and the server has nothing
   * to measure on an empty topic — it answers `warnings: []` and the dialog falls back to
   * `describePurge`, which is the correct rendering of a different state. So the case used to depend
   * on whatever the shared quickstart happened to hold, and it failed on a cluster whose seed had
   * been consumed: 14 later cases in this serial file never ran, for a reason that had nothing to do
   * with any of them. Arranging over HTTP rather than through the screen is this suite's own rule —
   * see `fixtures.ts`.
   */
  test("quotes the server's measured warning, and only once", async ({ page, api }) => {
    await api.post(`/api/v1/clusters/${CLUSTER}/topics/orders.v1/messages`, {
      count: 1,
      value: "kui-e2e: a record, so the purge plan has something to measure",
    });
    await page.goto(`/ui/clusters/${CLUSTER}/topics/orders.v1`);
    await page.getByRole("button", { name: /empty topic/i }).first().click();

    const dialog = page.getByRole("dialog");
    await expect(dialog).toContainText(/records? across \d+ partitions? (are|is) deleted/i, {
      timeout: 20_000,
    });

    /*
     * The screen used to compose its own measurement and append the server's warnings, so the
     * operator was told the size of the deletion twice, in two phrasings with two different
     * partition counts — the server says "4 partitions" because two hold nothing, and the browser
     * can only say "6". On a dialog whose whole purpose is that its numbers get read, that is the
     * worst possible place to disagree with yourself.
     */
    const text = (await dialog.innerText()).toLowerCase();
    expect(text.match(/cannot be recovered/g)?.length ?? 0).toBe(1);

    // And it says what survives, which is the reason somebody chooses empty over delete.
    await expect(dialog).toContainText(/only the records go/i);
  });

  test("asks for the topic's name before the button will work", async ({ page }) => {
    // Asked for here because the action destroys data and cannot be undone — and deliberately not
    // asked for on things that can be undone, because a product that demands it everywhere teaches
    // operators to type names without reading.
    await page.goto(`/ui/clusters/${CLUSTER}/topics/orders.v1`);
    await page.getByRole("button", { name: /empty topic/i }).first().click();
    const dialog = page.getByRole("dialog");
    await expect(dialog).toContainText("orders.v1", { timeout: 20_000 });
    await expect(dialog.getByRole("button", { name: /^empty topic$/i })).toBeDisabled();
  });
});

test.describe("deleting a topic", () => {
  test("warns that auto-create will bring the name straight back", async ({ page, api }) => {
    const name = scratchTopic("delete");
    await api.post(`/api/v1/clusters/${CLUSTER}/topics`, { name, config: {} });

    await page.goto(`/ui/clusters/${CLUSTER}/topics/${name}`);
    await page.getByRole("button", { name: /delete topic/i }).first().click();

    const dialog = page.getByRole("dialog");
    /*
     * The sentence an operator is least likely to have thought of. With auto.create.topics.enable
     * on — which the quickstart has — deleting a topic something is still producing to does not
     * remove it: it removes the configuration and the data and leaves a fresh topic with the
     * broker's defaults, which is usually the opposite of the intent.
     */
    await expect(dialog).toContainText(/auto\.create\.topics\.enable|recreate/i, { timeout: 20_000 });

    await dialog.getByRole("textbox").first().fill(name);
    await dialog.getByRole("button", { name: /^delete topic$/i }).click();

    await expect(page).toHaveURL(new RegExp(`/clusters/${CLUSTER}/topics$`), { timeout: 20_000 });
    await removeTopic(api, name);
  });
});

test.describe("the topic list", () => {
  test("searches on the server, not the page it happens to hold", async ({ page }) => {
    /*
     * It used to filter the rows it had been given. That is honest for a cluster of ten topics and
     * wrong for one of four thousand: a search that only looks at the rows it was handed is a search
     * that lies, and it lies by finding nothing and saying so.
     */
    await page.goto(`/ui/clusters/${CLUSTER}/topics`);
    await expect(page.getByText("orders.v1").first()).toBeVisible();

    /*
     * Waiting for the request rather than collecting them and asserting afterwards.
     *
     * The first version asserted on a list of seen URLs immediately after the row appeared — and
     * `analytics.pageviews` is already on screen before any search, so the assertion ran during the
     * search box's 300ms debounce and saw nothing. Waiting for the request is both correct and the
     * thing the test is actually about.
     */
    const search = page.waitForRequest((request) => request.url().includes("q=analytics"));
    await page.getByPlaceholder("Search topics…").fill("analytics");
    await search;

    await expect(page.getByText("analytics.pageviews").first()).toBeVisible();
    // And the server really narrowed it: a topic that does not match is gone from the page, which a
    // client-side filter over one page could not guarantee for a cluster of any size.
    await expect(page.locator("body")).not.toContainText("orders.v1");
  });

  test("a ?q= in the address filters the list", async ({ page }) => {
    /*
     * The drawer's topic-prefix rows link here (`…/topics?q=<prefix>`), and this screen used to
     * seed its query from a default and read the address only for `?tab=` — so the link was honest
     * and the destination listed the whole cluster. The shell owns the link; this asserts arrival.
     *
     * The request is waited for rather than the rows, because that is where the filter is applied:
     * a screen that filled the search box and asked for everything would look the same on arrival.
     */
    const asked = page.waitForRequest((request) => request.url().includes("q=orders"));
    await page.goto(`/ui/clusters/${CLUSTER}/topics?q=orders`);
    await asked;

    await expect(page.getByPlaceholder("Search topics…")).toHaveValue("orders");
    await expect(page.getByText("orders.v1").first()).toBeVisible();
    // And the server really narrowed it: a topic that does not match is not on the page.
    await expect(page.locator("body")).not.toContainText("analytics.pageviews");
  });

  test("reads the cluster's read-only flag, and the write controls obey it", async ({ page }) => {
    /*
     * The edge, driven through the browser's own path — and the assertion moved to where only this
     * feature's own read can satisfy it.
     *
     * `TopicsRoute` gates all seven of its write controls on ADR-047's read-only flag, which lives
     * on `GET /api/v1/clusters/{clusterId}`: a different service from the one that answers for
     * topics. Every unit case in `feature-topics` stubs that path, so all of them stay green if the
     * request 404s, 502s or never reaches the gateway at all — the accessor answers "not read-only"
     * when it cannot ask, deliberately, which makes a broken edge completely silent.
     *
     * This case used to wait for that *response* and stop there, and that gated nothing:
     * `shell/src/App.tsx` fetches the same endpoint on **every** route, so the wait was
     * satisfied by the shell's own request. Measured at wave 7's verification, two such requests
     * fire on `/clusters/<id>/consumer-groups`, where `feature-topics` is not mounted at all — and
     * deleting `useClusterReadOnly` outright left the case passing.
     *
     * So the flag is flipped in flight on the real answer, and what is asserted is the **screen**:
     * only a read this feature made itself can close the control and print the deployment's
     * sentence. Both directions, in one case, because a screen that refused every cluster would
     * satisfy the second half alone.
     */
    const clusterDocument = (url: URL): boolean => url.pathname === `/api/v1/clusters/${CLUSTER}`;

    /*
     * First, the request itself, counted rather than waited for.
     *
     * The shell asks for this document on every route, so "a response arrived" is not evidence that
     * this feature asked. What is evidence is the *difference* between a route where the feature is
     * mounted and one where it is not — and it is a difference rather than a number so that the
     * shell changing its own behaviour moves both sides together instead of reddening this case for
     * somebody else's reason. Measured here, on this stack: two on the group list, three on the
     * topic list.
     */
    let asked = 0;
    page.on("response", (response) => {
      if (clusterDocument(new URL(response.url()))) asked += 1;
    });

    await page.goto(`/ui/clusters/${CLUSTER}/consumer-groups`);
    await expect(page.getByRole("heading", { name: /consumer groups/i }).first()).toBeVisible();
    const withoutThisFeature = asked;

    const answered = page.waitForResponse(
      (response) =>
        clusterDocument(new URL(response.url())) && response.request().method() === "GET",
    );
    await page.goto(`/ui/clusters/${CLUSTER}/topics`);
    const response = await answered;
    expect(response.status()).toBe(200);
    const body = (await response.json()) as { cluster?: { readOnly?: boolean } };
    // The quickstart really is writable, so the control is live — and that is what makes the
    // second half below a measurement rather than a screen that says no to everything.
    expect(body.cluster?.readOnly).toBe(false);

    const create = page.getByRole("button", { name: /create topic/i }).first();
    await expect(create).toBeVisible();
    await expect(create).not.toHaveAttribute("aria-disabled", "true");

    // The read this feature makes for itself, on top of whatever the shell asked for.
    expect(asked).toBeGreaterThan(withoutThisFeature);

    /*
     * Now the same document with one field changed, fetched from the real gateway and patched on
     * the way through rather than invented here: everything else on it is the deployment's own.
     */
    await page.route(
      (url) => clusterDocument(url),
      async (route) => {
        const original = await route.fetch();
        const document = (await original.json()) as { cluster?: Record<string, unknown> };
        await route.fulfill({
          response: original,
          json: { ...document, cluster: { ...(document.cluster ?? {}), readOnly: true } },
        });
      },
    );

    await page.goto(`/ui/clusters/${CLUSTER}/topics`);
    const frozen = page.getByRole("button", { name: /create topic/i }).first();
    await expect(frozen).toBeVisible();
    await expect(frozen).toHaveAttribute("aria-disabled", "true");

    /*
     * And the deployment's sentence, not the permission one. `Button` renders its reason into a
     * tooltip reached by focus, which is also how a keyboard user reaches it; the two sentences
     * send an operator to different places, and the wrong one costs them an afternoon asking an
     * administrator for a permission they already hold.
     *
     * The sentence is the shell's write policy (`shell/src/data/writePolicy.ts`), which every
     * feature asks first since bf1393c; `writeBlockedReason`'s longer "configured read-only in
     * KUI" wording is only the fallback when no shell policy is mounted, which is never the case in
     * this browser.
     */
    await frozen.focus();
    const reason = page.locator(`#${(await frozen.getAttribute("aria-describedby")) ?? "none"}`);
    await expect(reason).toContainText("This cluster is read-only.");
    await expect(reason).not.toContainText("You do not have permission");
  });

  test("remembers whether the reader prefers cards", async ({ page }) => {
    // The design is explicit that the choice persists per user: an operator who prefers cards and
    // gets a table on every navigation concludes the control does not work.
    await page.goto(`/ui/clusters/${CLUSTER}/topics`);
    await page.getByRole("radio", { name: /^cards$/i }).click({ force: true });
    await expect(page.locator(".kui-topic-card").first()).toBeVisible();

    await page.goto(`/ui/clusters/${CLUSTER}/consumer-groups`);
    await page.goto(`/ui/clusters/${CLUSTER}/topics`);
    await expect(page.locator(".kui-topic-card").first()).toBeVisible();

    await page.getByRole("radio", { name: /^table$/i }).click({ force: true });
    await expect(page.locator("table").first()).toBeVisible();
  });
});

test.describe("the topics list's statistics region", () => {
  test("reads the cluster's totals and does not follow the search box", async ({ page }) => {
    /*
     * `SCREENS-V4.md` §4.6 calls this the load-bearing fact of the screen: the capture shows the
     * cluster's topic count above a table narrowed to three rows. Against the quickstart the
     * cluster holds ten topics and the list excludes Kafka's bookkeeping ones, so the tile and the
     * table already disagree before anything is typed — and typing must not move the tile.
     */
    await page.goto(`/ui/clusters/${CLUSTER}/topics`);

    const total = page.getByTestId("topic-stat-topics");
    await expect(total).toBeVisible();
    /* The figure alone, out of the label and the chip around it. A real number, not the sentence:
       the quickstart measures this one, and a tile reading "not measured" here would mean the
       assertion below was comparing two absences. */
    const figureOf = async (): Promise<string> =>
      (await total.innerText()).replace(/[^\d]/g, "");
    const before = await figureOf();
    expect(before).not.toBe("");
    await expect(total).not.toContainText("not measured");

    const search = page.waitForRequest((request) => request.url().includes("q=orders"));
    await page.getByPlaceholder("Search topics…").fill("orders");
    await search;
    await expect(page.getByText("analytics.pageviews")).toHaveCount(0);

    // The table moved; the cluster did not.
    expect(await figureOf()).toBe(before);
  });

  test("the statistics tile's partition total is read and compared, not merely non-empty", async ({
    page,
    api,
  }) => {
    /*
     * The case said "a page of eight topics cannot sum to the cluster's 86" and then asserted only
     * that the tile did not read "not measured" — so a tile that summed the page would have passed
     * it, which is the one failure the name describes. The number is now read off the screen and
     * compared twice: against the cluster's own total, and against the page's sum, which is a
     * different number and is what a fold over the rows would have drawn.
     */
    const statistics = (await api.get(`/api/v1/clusters/${CLUSTER}/topics/statistics`)) as {
      readonly statistics?: { readonly data?: { readonly partitionCount?: number | null } };
    };
    const clusterTotal = statistics.statistics?.data?.partitionCount ?? null;
    expect(
      clusterTotal,
      "the quickstart measures this; a null here is a broken fixture rather than a passing test",
    ).not.toBeNull();

    const listing = (await api.get(`/api/v1/clusters/${CLUSTER}/topics?page=1&pageSize=32`)) as {
      readonly topics?: {
        readonly data?: { readonly items?: readonly { readonly partitionCount?: number }[] };
      };
    };
    const pageSum = (listing.topics?.data?.items ?? []).reduce(
      (sum, topic) => sum + (topic.partitionCount ?? 0),
      0,
    );
    expect(
      pageSum,
      "the list excludes Kafka's own bookkeeping topics, so its sum must differ from the cluster's",
    ).not.toBe(clusterTotal);

    await page.goto(`/ui/clusters/${CLUSTER}/topics`);
    const partitions = page.getByTestId("topic-stat-partitions");
    await expect(partitions).toBeVisible();
    await expect(partitions).not.toContainText("not measured");

    /* The figure alone, out of the label and the chip around it — the same reading the case above
       uses, and the reason the tile carries a testId at all. */
    const drawn = Number((await partitions.innerText()).replace(/[^\d]/g, ""));
    expect(drawn).toBe(clusterTotal);
    expect(drawn).not.toBe(pageSum);
  });

  test("the switch closes the region and the list stays", async ({ page }) => {
    await page.goto(`/ui/clusters/${CLUSTER}/topics`);
    await expect(page.getByTestId("topic-stat-topics")).toBeVisible();
    await page.getByTestId("topic-statistics-switch").click({ force: true });
    await expect(page.getByTestId("topic-stat-topics")).toHaveCount(0);
    await expect(page.getByText("orders.v1").first()).toBeVisible();
  });
});

test.describe("the topics list's chips and selection", () => {
  test("a chip the cluster cannot apply says that it narrowed the page", async ({ page }) => {
    await page.goto(`/ui/clusters/${CLUSTER}/topics`);
    await page.getByRole("button", { name: /^compacted$/i }).click();
    await expect(page.getByText(/narrows the \d+ topics on this page/i)).toBeVisible();
  });

  test("selecting rows raises the bulk bar, and Delete keeps its place", async ({ page }) => {
    /*
     * §3.7: an action the principal may not take is disabled with its reason, never hidden — if
     * `Delete` disappeared, `Empty` would move into its place and the same gesture would do two
     * different irreversible things to two different people. Here the principal may do both, so the
     * assertion is that the bar appears with both of them on it and counts what was ticked.
     */
    await page.goto(`/ui/clusters/${CLUSTER}/topics`);
    await expect(page.getByText("orders.v1").first()).toBeVisible();

    const ticks = page.locator("tbody input[type=checkbox]");
    const drawn = await ticks.count();
    expect(
      drawn,
      "this list draws fewer than three rows, so a bar counting the page and a bar counting the " +
        "selection would print the same figure and the assertions below would distinguish nothing",
    ).toBeGreaterThan(2);

    /*
     * W10-02, closed by W10-A2: absent at zero selection, asserted before anything is ticked.
     *
     * `TopicListPage` states the rule in its own comment — "a bar that is always in the document
     * is a strip of the window nobody can use" — and this file referenced the bar only *after* a
     * tick, so the `await expect(bar).toBeVisible()` below is satisfied by a bar that was never
     * away. A destructive strip carrying Export, Empty and Delete floating over an untouched list,
     * reading "0 topics selected", is what that leaves room for. `toHaveCount(0)` rather than
     * `not.toBeVisible()`: a bar that is merely invisible still swallows the clicks meant for the
     * row underneath it.
     *
     * **And it has now been watched failing, which W10-A2 said plainly it had not done.** W11-02
     * substituted `BulkActionBar`'s guard in the bundle on its way to the browser —
     * `<Show when={props.count > 0}>` compiled to `get when(){return e.count>0}`, served as
     * `e.count>=0` — and this line reported `Expected: 0, Received: 1` after resolving to one
     * element 34 times. The unit case beside it (`surfaces.test.tsx`, "renders nothing at all at
     * zero selection") holds the same rule one layer down and is the cheaper of the two; what it
     * cannot see is `TopicListPage` handing the bar a count, which is the half that made the strip
     * appear over a real list here.
     */
    const bulkBar = page.getByTestId("topic-bulk-bar");
    await expect(bulkBar).toHaveCount(0);

    await ticks.first().click({ force: true });

    /*
     * One tick, and the figure read before the second one.
     *
     * The bar counts the **selection**, and the mutation that matters here counts the rows on the
     * page instead — which is the misreading that walks an operator into a destructive dialog with
     * the wrong number in front of them. Ticking two rows of a list that draws two makes those two
     * counts the same number, which is how `docs/plan/verification/W9-03.md` F4 found that switch
     * green under both of this file's bulk cases. This list is unfiltered, so the page count is the
     * cluster's topic count and the singular reading below is the cheapest thing that separates
     * them. It is also the only place in this suite that drives `BulkActionBar`'s singular.
     *
     * There is a second reason that switch survived here, found while re-applying the mutation and
     * worth writing down because it defeats every count assertion in this file: `toContainText` is
     * a **substring** match, and `12 topics selected` — which is what the mutated bar printed over
     * this unfiltered list — contains `2 topics selected`. So the plural assertion below could not
     * have reddened even with the two figures far apart. The singular has no such overlap: no count
     * this list can print ends in ` 1 topic selected`.
     */
    const bar = page.getByTestId("topic-bulk-bar");
    await expect(bar).toBeVisible();
    await expect(bar).toContainText("1 topic selected");

    await ticks.nth(1).click({ force: true });
    await expect(bar).toContainText("2 topics selected");
    await expect(bar.getByRole("button", { name: /^delete$/i })).toBeVisible();
    await expect(bar.getByRole("button", { name: /^empty$/i })).toBeVisible();
  });

  /**
   * `M14` — the plural receipt, and the last partially-covered screen of the twenty-three.
   *
   * Single deletion is covered above and `messages.spec.ts` covers two other toasts, so the toast
   * *machinery* was proved; **the sentence after a bulk delete was asserted nowhere.** It is not
   * the same sentence and it is not produced by the same code: `bulkSentence` counts what a set of
   * plan→token→confirm rounds actually did, pluralises on that count, and appends the refusals when
   * part of the set failed — so "2 topics deleted" over a selection of two is the one reading here
   * that establishes both halves of it at once, and a receipt that quietly said "1 topic deleted"
   * over a pair is precisely the failure mode this suite's header is about.
   *
   * Two scratch topics with a shared, timestamped prefix, and the list filtered to them **on the
   * server**. Ticking `tbody input[type=checkbox]` on an unfiltered list would select whatever the
   * quickstart's first two rows happen to be, and this case deletes what it selects.
   */
  test("a bulk delete says how many it deleted, in the plural", async ({ page, api }) => {
    /*
     * Longer than the file's default because of the arrangement below, and *only* because of it:
     * the assertions are as quick as any other case here. `kui.topics.refreshInterval` is 60s and a
     * scrape of this cluster takes ten to fifteen seconds under the load this machine runs at, so
     * the worst case for "both topics are in the service's snapshot" is one in-flight scrape plus
     * one of this case's own — measured at under a second on a quiet stack and at just over thirty
     * in a full-file run, which is where the 30s budget it shipped with went red.
     */
    test.setTimeout(180_000);

    const prefix = scratchTopic("bulk");
    const names = [`${prefix}-a`, `${prefix}-b`];

    try {
      /*
       * Created over HTTP, and then the list is **made** to catch up rather than waited on.
       *
       * `services/topic` serves the list from a `SnapshotCell` on `kui.topics.refreshInterval` (60
       * seconds by default) and `MutationGuard` asks the cell for an out-of-band refresh after a
       * write. `SnapshotCell.refresh` is idempotent under concurrency — five asks are one scrape —
       * so two creates a few milliseconds apart produce **one** scrape, and it is the scrape the
       * first create started, which ran before the second topic existed. Measured on this stack:
       * create `-a` and `-b` back to back, then poll `?q=<prefix>`, and the answer is `[-a]` and
       * only `[-a]` for the whole of the next thirty seconds. The same coalescing swallows a
       * create made while any earlier case's scrape is in flight, which is why sequencing the two
       * creates is not enough either — the full-file run reddened where the single-case run passed.
       *
       * So each attempt asks `POST …/topics/refresh` — the product's own "read this cluster now"
       * endpoint, 202 and asynchronous — and then reads. Some attempt's refresh starts after both
       * topics exist, and that one lands them. On a quiet stack the first attempt is enough; the
       * budget above is sized for the case where it has to wait out somebody else's scrape first.
       *
       * The wait is here rather than on a DOM assertion because the page does not re-fetch on its
       * own: a table that arrives empty stays empty however long `toHaveCount` retries.
       */
      for (const name of names) {
        await api.post(`/api/v1/clusters/${CLUSTER}/topics`, { name, config: {} });
      }
      await expect.poll(() => listedAfterRefresh(api, prefix), SETTLE).toBe(names.length);

      const filtered = page.waitForRequest((request) => request.url().includes(`q=${prefix}`));
      await page.goto(`/ui/clusters/${CLUSTER}/topics?q=${prefix}`);
      await filtered;

      /* The filter did what this case depends on, asserted before anything is ticked: two rows and
         no others. A search that quietly returned the whole cluster would otherwise be discovered
         by the confirmation dialog listing somebody's production topics. */
      const ticks = page.locator("tbody input[type=checkbox]");
      await expect(ticks).toHaveCount(names.length, { timeout: 20_000 });

      /*
       * Ticked one at a time, and the bar read after each.
       *
       * The arrangement above is what makes this necessary: the list is filtered on the server to
       * exactly the rows this case is about to tick, so once both are ticked "rows drawn" and "rows
       * selected" are the same number and a bar counting either one prints `2`. W9-03's verifier
       * measured that — switching the bar's count from the selected set to the rows on the page
       * left this case and the one above it green. Reading the figure while **one** of the two is
       * ticked is the assertion that tells them apart, and it drives the singular on the way past.
       */
      const bar = page.getByTestId("topic-bulk-bar");
      await ticks.nth(0).click({ force: true });
      await expect(bar).toContainText("1 topic selected");
      for (let row = 1; row < names.length; row += 1) {
        await ticks.nth(row).click({ force: true });
      }
      await expect(bar).toContainText(`${names.length} topics selected`);
      await bar.getByRole("button", { name: /^delete$/i }).click();

      /* Named rather than counted, which is `TopicsRoute`'s own rule for this dialog and the whole
         reason ADR-045 plans before it confirms: a confirmation reading "2 topics" is one the
         operator cannot check. */
      const dialog = page.getByTestId("topic-bulk-confirm");
      await expect(dialog).toBeVisible();
      for (const name of names) await expect(dialog).toContainText(name);

      await dialog.getByRole("textbox").first().fill("delete");
      await dialog.getByRole("button", { name: /^delete topics$/i }).click();

      /*
       * The receipt. Asserted before anything else, because a toast is on a six-second timer.
       *
       * The count is `names.length` rather than a literal `2`: what is under test is that the
       * sentence counts the set, and a literal on both sides of that is a test of nothing. The
       * plural is in the same assertion — `bulkSentence` picks "topic" or "topics" off the number
       * it is about to print, so `2 topics deleted` covers both halves of it at once.
       *
       * The second reading is taken from the text the first one found rather than as a second
       * retrying assertion, because a *negative* that retries is satisfied by the toast having
       * auto-dismissed: six seconds after it appears, `.kui-notice-stack` contains nothing and
       * every `not.toContainText` in the world passes over it.
       */
      const notices = page.locator(".kui-notice-stack");
      await expect(notices).toContainText(`${names.length} topics deleted`, { timeout: 20_000 });
      const receipt = await notices.innerText();
      expect(
        receipt,
        "the receipt names a refusal, so the set failed in the middle and the count above is the " +
          "half that worked",
      ).not.toContain("refused");

      /* And it deleted them, which is the other half of a receipt being true. The API is asked
         rather than the screen: the list reloads on its own timing and "the row is gone" is
         satisfied by a filter that stopped matching. */
      await expect.poll(() => listedAfterRefresh(api, prefix), SETTLE).toBe(0);
    } finally {
      for (const name of names) await removeTopic(api, name);
    }
  });
});

test.describe("the topic's Overview tab", () => {
  test("draws a body: four figures and the partition table", async ({ page }) => {
    /*
     * The tab the strip opens by default, and the one that rendered nothing at all before this
     * wave: `TopicsRoute` declared `id: "overview"` and had no body for it anywhere in the file.
     */
    await page.goto(`/ui/clusters/${CLUSTER}/topics/orders.v1`);

    await expect(page.getByTestId("topic-overview-partitions")).toContainText("6");
    await expect(page.getByTestId("topic-overview-groups")).toBeVisible();
    await expect(page.getByTestId("topic-partitions-table")).toBeVisible();

    /*
     * The quickstart's single broker reports no per-topic log-directory size and no produce rate.
     * Both must say so in words — `0 B` and `0 /s` are what a topic legitimately measures at, and
     * the whole promise of this product is that the two never look alike.
     */
    await expect(page.getByTestId("topic-overview-size")).toContainText("not measured");

    // And the trail inside the content, which is how somebody gets back to the list.
    await expect(page.getByRole("navigation", { name: "Breadcrumb" }).first()).toContainText("Topics");
  });

  test("Produce message goes to the browser that can produce", async ({ page }) => {
    await page.goto(`/ui/clusters/${CLUSTER}/topics/orders.v1`);
    await page.getByRole("button", { name: /produce message/i }).first().click();
    await expect(page).toHaveURL(new RegExp(`/topics/orders.v1/messages$`), { timeout: 20_000 });
  });
});

test.describe("the topic's Consumers tab", () => {
  test("prints the coordinator as an address and not as a broker id", async ({ page }) => {
    /*
     * `coordinatorHost` and `coordinatorPort` are on the wire beside `coordinatorId`. Against the
     * quickstart the group's coordinator is `kafka:9092`; `broker 1` is not something an operator
     * can connect to, ping, or find in a log, which is the whole reason the column is an address.
     */
    await page.goto(`/ui/clusters/${CLUSTER}/topics/orders.v1?tab=consumers`);
    const table = page.getByTestId("topic-consumers-table");
    await expect(table).toBeVisible();
    await expect(table).toContainText("order-fulfilment", { timeout: 20_000 });
    await expect(table).toContainText(/[a-z0-9.-]+:\d+/);
    await expect(table).not.toContainText(/broker \d/i);
  });

  test("no column heading is blank", async ({ page }) => {
    // The heading that shipped as `""` and produced all fourteen of the a11y sweep's violations.
    // A screen reader announces a cell by its column, and this column had nothing to announce.
    await page.goto(`/ui/clusters/${CLUSTER}/topics/orders.v1?tab=consumers`);
    const headings = page.getByTestId("topic-consumers-table").locator("th");
    await expect(headings.first()).toBeVisible();
    for (const text of await headings.allInnerTexts()) {
      expect(text.trim()).not.toBe("");
    }
  });
});

/**
 * How long a change of the cluster is given to reach the topic service's own snapshot.
 *
 * Sized from the two figures that decide it, both of which are in this repository:
 * `kui.topics.refreshInterval` is 60 seconds (`libs/config`'s `TopicsConfig`) and one scrape of this
 * cluster takes ten to fifteen under the load this machine runs at. A read that arrives while
 * somebody else's scrape is in flight waits that scrape out and then its own, which is where a
 * 30-second budget went red in a full-file run and passed when the same case ran alone.
 */
const SETTLE = { timeout: 90_000, intervals: [500, 500, 1_000, 1_000, 2_000] };

/**
 * Asks the service to read the cluster now, then counts what it holds under a prefix.
 *
 * The refresh is inside the poll rather than before it because `SnapshotCell.refresh` is idempotent
 * under concurrency — a caller arriving during a scrape joins *that* scrape, which may have started
 * before the change this is waiting for. Asking again on the next attempt is what eventually starts
 * one that did not.
 */
async function listedAfterRefresh(api: KuiApi, prefix: string): Promise<number> {
  await api.post(`/api/v1/clusters/${CLUSTER}/topics/refresh`);
  return (await topicsMatching(api, prefix)).length;
}

/**
 * The names the cluster still holds under a prefix, asked of the gateway rather than of the screen.
 *
 * A row leaving the table is satisfied by a filter that stopped matching, by a page that moved, and
 * by a list that failed to reload — three states a receipt saying "deleted" must not be confirmed
 * by. This asks the same question the operator would ask afterwards.
 */
async function topicsMatching(api: KuiApi, prefix: string): Promise<readonly string[]> {
  const listing = (await api.get(
    `/api/v1/clusters/${CLUSTER}/topics?q=${encodeURIComponent(prefix)}&page=1&pageSize=32`,
  )) as {
    readonly topics?: {
      readonly data?: { readonly items?: readonly { readonly name?: string }[] };
    };
  };
  return (listing.topics?.data?.items ?? [])
    .map((topic) => topic.name)
    .filter((name): name is string => typeof name === "string" && name.startsWith(prefix));
}
