/**
 * The message browser, driven against a real broker.
 *
 * Three of the four things this screen was rebuilt for are only true end to end, and each of them
 * was false in the shipped product for a reason no component test could see:
 *
 *   - **The partition count is the topic's.** The route held `createSignal(0)` and handed the zero
 *     to three children, so the copy dialog told every operator the source topic has 0 partitions.
 *     The assertion below reads the figure from the API and then reads it off the screen, so it
 *     cannot pass against a hard-coded anything.
 *   - **A typed predicate reaches the request.** The key and value predicates have no query
 *     parameter — they compile to one CEL expression that the screen registers with the message
 *     service, and the browse quotes the id it was given. Two hops, both over the network.
 *   - **The address is the browse.** It was written with `window.history.replaceState`, which the
 *     router never hears about, so every control on the bar changed the URL and left the browse
 *     reading whatever the page was opened with.
 *
 * Everything here reads except the last case, which publishes one record into a topic it created.
 */
import type { Page, Request } from "@playwright/test";
import { test, expect, CLUSTER, removeTopic, scratchTopic } from "./fixtures";

// It shares the cluster with the other specs, and the last case writes to it.
test.describe.configure({ mode: "serial" });

const TOPIC = "orders.v1";

async function applyFieldFilter(
  page: Page,
  topic: string,
  path: string,
  value: string,
  serdes = "",
): Promise<void> {
  await page.goto(
    `/ui/clusters/${CLUSTER}/topics/${encodeURIComponent(topic)}/messages?seekTo=beginning${serdes}`,
  );
  await page.getByRole("button", { name: "Filter with an expression" }).click();

  const dialog = page.getByRole("dialog");
  await expect(dialog).toBeVisible();
  await expect(dialog.getByRole("button", { name: "Field filter" })).toHaveAttribute(
    "aria-pressed",
    "true",
  );
  await dialog.getByLabel("Field path").fill(path);
  await dialog.getByLabel("Compare with").fill(value);

  const registration = page.waitForResponse(
    (response) =>
      response.url().includes("/messages/filters") && response.request().method() === "POST",
  );
  await dialog.getByRole("button", { name: "Use this filter" }).click();
  expect((await registration).ok()).toBe(true);
  await expect(dialog).toBeHidden();

  const browse = page.waitForRequest((request) => request.url().includes("/messages/stream"));
  await page.getByRole("button", { name: /^read$/i }).first().click();
  await browse;
  await expect(page.locator(".kui-browse__phase")).toContainText("Finished", {
    timeout: 30_000,
  });
}

/**
 * Every request the page stopped reading, and how: `null` when the body ran to its end, the
 * browser's error text when it did not.
 *
 * This replaces `response.finished()`, which hung these cases until the test timeout. The message
 * stream's client aborts its own request the moment it parses the terminal `done` frame (every
 * terminal path in `kernel/src/data/sse/stream.ts` releases the request), so whether the browser
 * records a page as finished or as `net::ERR_ABORTED` is a race between the gateway closing the body
 * and the client reading `done` — and Playwright's `finished()` settles on the first outcome only,
 * never on the second. Both mean the page was a bounded request that ended; anything else (a reset,
 * a refusal) is a real failure and is still one here.
 */
function trackEndedRequests(page: Page): Map<Request, string | null> {
  const ended = new Map<Request, string | null>();
  page.on("requestfinished", (request) => ended.set(request, null));
  page.on("requestfailed", (request) => ended.set(request, request.failure()?.errorText ?? "failed"));
  return ended;
}

async function expectStreamEnded(
  ended: Map<Request, string | null>,
  request: Request,
): Promise<void> {
  await expect.poll(() => ended.has(request), { timeout: 30_000 }).toBe(true);
  expect([null, "net::ERR_ABORTED"]).toContain(ended.get(request));
}

/** The topic's own partition count, straight from the gateway. The screen has to agree with this. */
async function partitionCount(api: { get: (path: string) => Promise<unknown> }): Promise<number> {
  const answer = (await api.get(
    `/api/v1/clusters/${CLUSTER}/topics/${encodeURIComponent(TOPIC)}`,
  )) as { topic?: { data?: { row?: { partitionCount?: number } } } };
  const count = answer.topic?.data?.row?.partitionCount;
  if (typeof count !== "number") {
    throw new Error(
      `the gateway did not describe ${TOPIC}; there is nothing for the screen to agree with`,
    );
  }
  return count;
}

test.describe("the partition count", () => {
  test("the selector names the topic's real one, and never `all 0`", async ({ page, api }) => {
    const count = await partitionCount(api);

    await page.goto(`/ui/clusters/${CLUSTER}/topics/${TOPIC}/messages`);
    const picker = page.locator(".kui-partition-picker__trigger");
    await expect(picker).toBeVisible();

    /*
     * `all N` with the broker's own N. The zero this replaced was not a rounding error: it made the
     * menu empty as well, so the control offered no partition to choose and read as a topic that
     * has none.
     */
    await expect(picker).toContainText(`all ${count}`, { timeout: 20_000 });
    await expect(picker).not.toContainText("all 0");
  });

  test("the copy dialog quotes it, in a sentence", async ({ page, api }) => {
    const count = await partitionCount(api);

    await page.goto(`/ui/clusters/${CLUSTER}/topics/${TOPIC}/messages`);
    await page.getByRole("button", { name: /copy records out/i }).click();

    const dialog = page.getByRole("dialog");
    await expect(dialog).toBeVisible();
    /*
     * The sentence is the feature. A dialog that copies the right records while saying the wrong
     * number is a dialog nobody should trust — and this one said "has 0 partitions" beside a
     * control that copies records between topics.
     */
    const clause = count === 1 ? "has 1 partition." : `has ${count} partitions.`;
    await expect(dialog).toContainText(`${TOPIC} ${clause}`, { timeout: 20_000 });
    await expect(dialog).not.toContainText("has 0 partitions");
    // And it counts. The sentence read "has 1 partitions" on every single-partition topic, which is
    // most of what a scratch cluster holds; the clause above is the screen's own, not the spec's.
    await expect(dialog).not.toContainText("has 1 partitions");
    // The class says which of the two sentences this is. They were sharing `__unknown`, so the one
    // line on this dialog that states a measured figure was marked up as the absence of one.
    await expect(dialog.locator(".kui-resend__known")).toBeVisible();
  });
});

test.describe("the topic context", () => {
  test("keeps the topic identity and all sibling sections around Messages", async ({ page }) => {
    await page.goto(`/ui/clusters/${CLUSTER}/topics/${TOPIC}/messages`);

    const header = page.getByTestId("topic-message-head");
    await expect(header.getByRole("heading", { name: TOPIC, exact: true })).toBeVisible();
    await expect(header.getByText("in sync", { exact: true })).toBeVisible();
    await expect(header.getByRole("navigation", { name: "Breadcrumb" })).toContainText("Topics");

    const sections = page.getByRole("navigation", { name: "Topic sections" });
    await expect(sections.getByRole("link", { name: /messages/i })).toHaveAttribute(
      "aria-current",
      "page",
    );
    for (const name of ["Overview", "Partitions", "Messages", "Consumers", "Settings"]) {
      await expect(sections.getByRole("link", { name: new RegExp(name, "i") })).toBeVisible();
    }

    await sections.getByRole("link", { name: /partitions/i }).click();
    await expect(page).toHaveURL(new RegExp(`/topics/${TOPIC.replace(".", "\\.")}\\?tab=partitions`));
    await expect(page.getByRole("heading", { name: TOPIC, exact: true })).toBeVisible();
  });
});

test.describe("the typed predicates", () => {
  test("a key predicate and a value predicate reach the request separately", async ({ page }) => {
    await page.goto(`/ui/clusters/${CLUSTER}/topics/${TOPIC}/messages`);

    await page.getByLabel(/key predicate/i).fill("ord_");
    await page.getByLabel(/value predicate/i).fill("UAH");

    /*
     * The address first, because it is the cheap half and it is the half that was broken: the
     * previous writer used `window.history.replaceState`, which the router does not hear, so the
     * URL and the browse disagreed from the first keystroke.
     */
    await expect(page).toHaveURL(/key=ord_/, { timeout: 10_000 });
    await expect(page).toHaveURL(/value=UAH/);

    /*
     * Then the two requests Read makes. Waiting for them rather than collecting afterwards, for the
     * reason `topics.spec.ts` gives about the search box: an assertion that runs between the click
     * and the request sees nothing and blames the feature.
     */
    const registration = page.waitForRequest(
      (request) => request.url().includes("/messages/filters") && request.method() === "POST",
    );
    const browse = page.waitForRequest((request) => request.url().includes("/messages/stream"));

    await page.getByRole("button", { name: /^read$/i }).first().click();

    const registered = await registration;
    const source = String((registered.postDataJSON() as { source?: unknown }).source ?? "");
    // Two terms over two variables. `q`, the one substring parameter the endpoint has, is matched
    // against the whole decoded record and could not tell a key predicate from a value one.
    expect(source).toContain('record.keyAsText.contains("ord_")');
    expect(source).toContain('record.valueAsText.contains("UAH")');

    const url = new URL((await browse).url());
    // The id and the source travel together: a replica that never saw the registration compiles the
    // source beside it, and a browse carrying only the source is dropped in silence.
    expect(url.searchParams.get("filterId")).toBeTruthy();
    expect(url.searchParams.get("filterSource")).toContain("record.keyAsText");
    expect(url.searchParams.get("filterSource")).toContain("record.valueAsText");
  });

  test("a browse with no predicate registers nothing", async ({ page }) => {
    // The common browse must not have acquired a round trip. A filter endpoint call here would be
    // the screen registering the empty expression, which is a compile nobody asked for.
    await page.goto(`/ui/clusters/${CLUSTER}/topics/${TOPIC}/messages`);

    const filterCalls: string[] = [];
    page.on("request", (request) => {
      if (request.url().includes("/messages/filters")) filterCalls.push(request.url());
    });

    const browse = page.waitForRequest((request) => request.url().includes("/messages/stream"));
    await page.getByRole("button", { name: /^read$/i }).first().click();
    await browse;

    expect(filterCalls).toEqual([]);
  });
});

test.describe("offset cursor pagination", () => {
  test("paints a useful partial page before committing a 100-record burst", async ({ page }) => {
    await page.goto(
      `/ui/clusters/${CLUSTER}/topics/connect.file.lines/messages?seekTo=latest&limit=100`,
    );
    /* `goto` resolves on `load`, which is before the shell has bootstrapped and the lazily loaded
     * messages feature has mounted; reaching straight for `.kui-browse` raced that and, on a slow
     * runner, found the shell still empty. The Read button is inside the browser section, so once it
     * is on screen the section the observer attaches to is the one the burst will render into. */
    const read = page.getByRole("button", { name: /^read$/i }).first();
    await expect(read).toBeVisible();
    await page.evaluate(() => {
      const browse = document.querySelector(".kui-browse");
      if (browse === null) throw new Error("the message browser did not render");
      const evidence: { mutations: number[]; firstFrame?: number } = { mutations: [] };
      (window as Window & { __kuiProgressiveRender?: typeof evidence }).__kuiProgressiveRender =
        evidence;
      let frameRequested = false;
      new MutationObserver(() => {
        const count = document.querySelectorAll(".kui-record").length;
        if (count === 0 || evidence.mutations.at(-1) === count) return;
        evidence.mutations.push(count);
        if (frameRequested) return;
        frameRequested = true;
        requestAnimationFrame(() => {
          evidence.firstFrame = document.querySelectorAll(".kui-record").length;
        });
      }).observe(browse, { childList: true, subtree: true });
    });

    await read.click();
    await expect(page.locator(".kui-record")).toHaveCount(100, { timeout: 30_000 });
    await expect
      .poll(() =>
        page.evaluate(
          () =>
            (window as Window & { __kuiProgressiveRender?: { firstFrame?: number } })
              .__kuiProgressiveRender?.firstFrame,
        ),
      )
      .toBeDefined();

    const evidence = await page.evaluate(
      () =>
        (window as Window & {
          __kuiProgressiveRender?: { mutations: number[]; firstFrame?: number };
        }).__kuiProgressiveRender,
    );
    expect(evidence?.firstFrame).toBeGreaterThan(0);
    expect(evidence?.firstFrame).toBeLessThan(100);
    expect(evidence?.mutations.length).toBeGreaterThan(1);
  });

  test("a full message page scrolls inside the frame without creating a blank document tail", async ({
    page,
  }) => {
    await page.setViewportSize({ width: 1760, height: 900 });
    await page.goto(
      `/ui/clusters/${CLUSTER}/topics/connect.file.lines/messages?seekTo=beginning&limit=100`,
    );

    const browse = page.waitForResponse((response) =>
      response.url().includes("/messages/stream"),
    );
    await page.getByRole("button", { name: /^read$/i }).first().click();
    expect((await browse).ok()).toBe(true);
    await expect(page.locator(".kui-record")).toHaveCount(100, { timeout: 30_000 });

    const geometry = await page.evaluate(() => {
      const content = document.querySelector<HTMLElement>(".kui-frame__content");
      if (content === null) throw new Error("the application frame has no content scroller");
      content.scrollTop = content.scrollHeight;
      window.scrollTo({ top: document.documentElement.scrollHeight, behavior: "instant" });
      return {
        rootScrollTop: window.scrollY,
        rootHeight: document.documentElement.scrollHeight,
        viewportHeight: window.innerHeight,
        contentScrollTop: content.scrollTop,
        contentMaximum: content.scrollHeight - content.clientHeight,
      };
    });

    expect(geometry.rootScrollTop).toBe(0);
    expect(geometry.rootHeight).toBe(geometry.viewportHeight);
    expect(geometry.contentScrollTop).toBe(geometry.contentMaximum);
    await expect(page.getByRole("navigation", { name: "Message offset pages" })).toBeInViewport();
    await expect(page.getByRole("button", { name: "Produce message" })).toBeInViewport();
  });

  test("pages cache backwards navigation and infinite scroll preloads one continuation at a time", async ({
    page,
  }) => {
    const streamUrls: string[] = [];
    let releasePreload = (): void => {};
    const preloadGate = new Promise<void>((resolve) => {
      releasePreload = resolve;
    });

    /* Keep the first infinite-scroll continuation at the browser boundary for a few frames. If
     * two observer callbacks can spend the same cursor concurrently, both requests reach this
     * route and the assertion below sees four calls before either one has a response. The response
     * still comes from the real message service once the gate opens. */
    const ended = trackEndedRequests(page);
    await page.route(/\/messages\/stream(?:\?|$)/, async (route) => {
      streamUrls.push(route.request().url());
      if (streamUrls.length === 3) await preloadGate;
      await route.continue();
    });

    await page.goto(
      `/ui/clusters/${CLUSTER}/topics/${TOPIC}/messages?seekTo=beginning&limit=2`,
    );

    const mode = page.getByRole("radiogroup", { name: "Message loading mode" });
    await expect(mode.getByRole("radio", { name: "Pages" })).toBeChecked();

    const firstResponse = page.waitForResponse((response) =>
      response.url().includes("/messages/stream"),
    );
    await page.getByRole("button", { name: /^read$/i }).first().click();
    const first = await firstResponse;
    expect(first.ok()).toBe(true);
    await expectStreamEnded(ended, first.request());

    const records = page.locator(".kui-record");
    const pager = page.getByRole("navigation", { name: "Message offset pages" });
    const pageSummary = pager.locator(".kui-browse__offset-range");
    await expect(records).toHaveCount(2);
    await expect(pageSummary).toContainText("Page 1");

    /* Kafka has no global row number across partitions. The page names the partition-local
     * offsets actually represented, regardless of which two partitions the keyed seed selected. */
    expect((await pageSummary.innerText()).trim()).toMatch(
      /^Page 1 · p\d+ offsets \d+–\d+(?: · p\d+ offsets \d+–\d+)* · 2 records$/,
    );

    const firstPagePositions = await records.evaluateAll((rows) =>
      rows.map((row) => {
        const partition = row.querySelector(".kui-record__partition")?.textContent ?? "";
        const offset = row.querySelector(".kui-record__offset-value")?.textContent ?? "";
        return `${partition.replace(/\s+/g, "")}:${offset.replace(/\s+/g, "")}`;
      }),
    );

    expect(streamUrls).toHaveLength(1);
    const firstUrl = new URL(streamUrls[0] ?? "");
    expect(firstUrl.searchParams.get("limit")).toBe("2");
    expect(firstUrl.searchParams.get("seekTo")).toBe("beginning");
    expect(firstUrl.searchParams.get("cursor")).toBeNull();

    const secondResponse = page.waitForResponse((response) => {
      const url = new URL(response.url());
      return url.pathname.endsWith("/messages/stream") && url.searchParams.has("cursor");
    });
    await pager.getByRole("button", { name: "Next offset page" }).click();
    const second = await secondResponse;
    expect(second.ok()).toBe(true);
    await expectStreamEnded(ended, second.request());
    await expect(records).toHaveCount(2);
    await expect(pageSummary).toContainText("Page 2");

    expect(streamUrls).toHaveLength(2);
    const nextUrl = new URL(streamUrls[1] ?? "");
    expect(nextUrl.searchParams.get("cursor")).toBeTruthy();
    expect(nextUrl.searchParams.getAll("seekTo")).toEqual([]);
    expect(nextUrl.searchParams.get("limit")).toBe("2");

    await pager.getByRole("button", { name: "Previous offset page" }).click();
    await expect(pageSummary).toContainText("Page 1");
    expect(
      await records.evaluateAll((rows) =>
        rows.map((row) => {
          const partition = row.querySelector(".kui-record__partition")?.textContent ?? "";
          const offset = row.querySelector(".kui-record__offset-value")?.textContent ?? "";
          return `${partition.replace(/\s+/g, "")}:${offset.replace(/\s+/g, "")}`;
        }),
      ),
    ).toEqual(firstPagePositions);

    /* Cached Previous is entirely local. Two animation frames cover the reactive update and the
     * following paint; a transport call caused by it would already have crossed the route above. */
    await page.evaluate(
      () =>
        new Promise<void>((resolve) => {
          requestAnimationFrame(() => requestAnimationFrame(() => resolve()));
        }),
    );
    expect(streamUrls).toHaveLength(2);

    const preloadResponse = page.waitForResponse((response) => {
      const url = new URL(response.url());
      return url.pathname.endsWith("/messages/stream") && url.searchParams.has("cursor");
    });
    await mode.getByRole("radio", { name: "Infinite scroll" }).check();

    /* The two cached pages are visible immediately; the observer then asks for their real cursor
     * continuation before the sentinel enters the viewport. */
    await expect(records).toHaveCount(4);
    await expect.poll(() => streamUrls.length).toBe(3);
    const preloadUrl = new URL(streamUrls[2] ?? "");
    expect(preloadUrl.searchParams.get("cursor")).toBeTruthy();
    expect(preloadUrl.searchParams.getAll("seekTo")).toEqual([]);

    await page.evaluate(
      () =>
        new Promise<void>((resolve) => {
          requestAnimationFrame(() =>
            requestAnimationFrame(() => requestAnimationFrame(() => resolve())),
          );
        }),
    );
    expect(streamUrls).toHaveLength(3);

    releasePreload();
    const preload = await preloadResponse;
    expect(preload.ok()).toBe(true);
    await expectStreamEnded(ended, preload.request());
    await expect.poll(() => records.count(), { timeout: 30_000 }).toBeGreaterThan(4);
  });
});

test.describe("field filtering over decoded records", () => {
  test("a JSONPath-style nested field filter returns only matching JSON records", async ({ page }) => {
    await applyFieldFilter(page, "payments.transactions", "$.method.type", "card");

    const rows = page.locator(".kui-record");
    expect(await rows.count()).toBeGreaterThan(0);
    for (const row of await rows.all()) {
      await row.locator(".kui-record__summary").click();
      await expect(row).toContainText(/"type"\s*:\s*"card"/);
    }
    await expect(page).toHaveURL(/filterSource=record\.value\.method\.type/);
  });

  test("the same field builder filters registry-backed Avro values", async ({ page }) => {
    await applyFieldFilter(
      page,
      "orders.avro",
      "$.address.city",
      "Krakow",
      "&keySerde=String&valueSerde=SchemaRegistry",
    );

    const rows = page.locator(".kui-record");
    expect(await rows.count()).toBeGreaterThan(0);
    for (const row of await rows.all()) {
      await expect(row).toContainText("Krakow");
      await row.locator(".kui-record__summary").click();
      await expect(row).toContainText(/schema/i);
    }
  });

  for (const topic of ["orders.jsonschema", "orders.protobuf"] as const) {
    test(`the same field builder filters registry-backed ${topic.split(".")[1]} values`, async ({
      page,
    }) => {
      await applyFieldFilter(
        page,
        topic,
        "$.shipping.city",
        "Krakow",
        "&keySerde=String&valueSerde=SchemaRegistry",
      );

      const rows = page.locator(".kui-record");
      expect(await rows.count()).toBeGreaterThan(0);
      for (const row of await rows.all()) {
        await expect(row).toContainText("Krakow");
        await row.locator(".kui-record__summary").click();
        await expect(row).toContainText(/schema/i);
        await expect(row).toContainText(/"city"\s*:\s*"Krakow"/);
      }
    });
  }

  test("plain string topics use the fast value-text predicate", async ({ page }) => {
    await page.goto(
      `/ui/clusters/${CLUSTER}/topics/audit.log.raw/messages?seekTo=beginning&value=result%3Dsuccess`,
    );
    const browse = page.waitForRequest((request) => request.url().includes("/messages/stream"));
    await page.getByRole("button", { name: /^read$/i }).first().click();
    await browse;
    await expect(page.locator(".kui-browse__phase")).toContainText("Finished", {
      timeout: 30_000,
    });

    const rows = page.locator(".kui-record");
    expect(await rows.count()).toBeGreaterThan(0);
    for (const row of await rows.all()) await expect(row).toContainText("result=success");
  });
});

test.describe("record copy actions and JSON tree", () => {
  test("copies a collapsed message, then exposes value, key, headers, and JSON nodes", async ({ page }) => {
    await page.context().grantPermissions(["clipboard-read", "clipboard-write"]);
    await page.goto(
      `/ui/clusters/${CLUSTER}/topics/inventory.stock-levels/messages?seekTo=beginning`,
    );
    const browse = page.waitForRequest((request) => request.url().includes("/messages/stream"));
    await page.getByRole("button", { name: /^read$/i }).first().click();
    await browse;
    await expect(page.locator(".kui-browse__phase")).toContainText("Finished", {
      timeout: 30_000,
    });

    const row = page.locator(".kui-record").first();
    const summary = row.locator(".kui-record__summary");
    await expect(summary).toHaveAttribute("aria-expanded", "false");

    await row.getByRole("button", { name: "Copy message", exact: true }).click();
    const message = JSON.parse(await page.evaluate(() => navigator.clipboard.readText())) as {
      headers?: unknown[];
      key?: unknown;
      value?: { sku?: unknown };
    };
    expect(message.headers?.length).toBeGreaterThan(0);
    expect(typeof message.key).toBe("string");
    expect(typeof message.value?.sku).toBe("string");
    await expect(summary).toHaveAttribute("aria-expanded", "false");
    await expect(row.getByRole("status")).toContainText("Message copied");

    await row.locator(".kui-record__summary").click();

    await row.getByRole("button", { name: "Copy value", exact: true }).click();
    const value = JSON.parse(await page.evaluate(() => navigator.clipboard.readText())) as {
      sku?: unknown;
    };
    expect(typeof value.sku).toBe("string");

    await row.getByRole("button", { name: "Copy key", exact: true }).click();
    expect(await page.evaluate(() => navigator.clipboard.readText())).toBe(message.key);

    await row.getByRole("button", { name: "Copy headers", exact: true }).click();
    const headers = JSON.parse(await page.evaluate(() => navigator.clipboard.readText())) as unknown[];
    expect(headers).toEqual(message.headers);
    await expect(row.getByRole("status")).toContainText("Headers copied");

    const root = row.locator('details[data-json-path="$"]');
    await expect(root).toHaveAttribute("open", "");
    await root.locator(":scope > summary").click();
    await expect(root).not.toHaveAttribute("open", "");
  });

  test("keeps the copy actions and JSON tree inside a narrow record", async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto(
      `/ui/clusters/${CLUSTER}/topics/inventory.stock-levels/messages?seekTo=beginning`,
    );
    const browse = page.waitForRequest((request) => request.url().includes("/messages/stream"));
    await page.getByRole("button", { name: /^read$/i }).first().click();
    await browse;
    await expect(page.locator(".kui-browse__phase")).toContainText("Finished", {
      timeout: 30_000,
    });

    const row = page.locator(".kui-record").first();
    const copyMessage = row.getByRole("button", { name: "Copy message", exact: true });
    await expect(copyMessage).toBeVisible();
    expect(await copyMessage.evaluate((element) => element.getBoundingClientRect().width)).toBeLessThan(
      48,
    );
    await expect(row.locator(".kui-record__value")).toBeHidden();
    await expect(row.locator(".kui-record__time")).toBeHidden();
    expect(
      await row.locator(".kui-record__head").evaluate((element) =>
        element.scrollWidth <= element.clientWidth,
      ),
    ).toBe(true);

    await row.locator(".kui-record__summary").click();
    await expect(row.locator(".kui-json-tree")).toBeVisible();
    expect(
      await row.locator(".kui-record__body").evaluate((element) =>
        element.scrollWidth <= element.clientWidth,
      ),
    ).toBe(true);
  });
});

test.describe("presets", () => {
  test("naming a preset says where it was saved, which no chip can", async ({ page }) => {
    /*
     * A preset is a name this browser has given to a set of predicates. It does not reach the
     * cluster: `POST …/messages/filters` compiles an expression and answers with `sha256(source)`,
     * there is no `GET`, and nothing on the server holds a name for one. So the chip that appears
     * says a preset exists and cannot say that it exists here only — the sentence this raises is
     * the one place the product tells anybody, and until this wave nothing asserted it in either
     * suite.
     *
     * `page.on("dialog")` because `savePreset` asks with `window.prompt`, and Playwright dismisses
     * an unhandled dialog — which is the cancel path, and saves nothing.
     */
    page.on("dialog", (dialog) => void dialog.accept("Big tickets"));

    await page.goto(`/ui/clusters/${CLUSTER}/topics/${TOPIC}/messages?key=ord_`);

    // Offered only when there is something to save, so the predicate above is what puts it there.
    const save = page.getByRole("button", { name: /save as preset/i });
    await expect(save).toBeVisible({ timeout: 20_000 });
    await save.click();

    const toast = page.locator(".kui-notice").first();
    await expect(toast).toBeVisible({ timeout: 20_000 });
    await expect(toast).toContainText("Filter saved");
    await expect(toast).toContainText("Big tickets");
    // The half a chip cannot say, and the reason the toast exists at all.
    await expect(toast).toContainText("this browser only");

    // And the chip is on the bar, so the sentence is a confirmation of something that happened.
    // `exact`, because the chip's own remove control is named `Forget the preset Big tickets`.
    await expect(page.getByRole("button", { name: "Big tickets", exact: true })).toBeVisible();
  });
});

test.describe("writing", () => {
  test("a produce that succeeds raises a toast naming where the record landed", async ({
    page,
    api,
  }) => {
    const name = scratchTopic("produce");
    await api.post(`/api/v1/clusters/${CLUSTER}/topics`, { name, config: {} });

    await page.goto(`/ui/clusters/${CLUSTER}/topics/${name}/messages`);
    await page.getByRole("button", { name: /produce message/i }).click();

    const drawer = page.getByRole("dialog");
    await expect(drawer).toBeVisible();
    const editor = drawer.getByLabel("Value", { exact: true }).first();
    await editor.fill('{"from":"the e2e suite","metadata":{"attempt":1}}');
    await expect(drawer).toContainText("Valid JSON");
    await drawer.getByRole("button", { name: "Format JSON" }).click();
    await expect(editor).toHaveValue(
      '{\n  "from": "the e2e suite",\n  "metadata": {\n    "attempt": 1\n  }\n}',
    );
    await drawer.getByRole("checkbox", { name: "Minify before sending" }).check();

    const request = page.waitForRequest(
      (candidate) => candidate.url().includes(`/topics/${name}/messages`) && candidate.method() === "POST",
    );
    await drawer.getByRole("button", { name: /^produce record$/i }).click();
    const body = (await request).postDataJSON() as { value?: string };
    expect(body.value).toBe('{"from":"the e2e suite","metadata":{"attempt":1}}');

    /*
     * The toast, and not the drawer's own receipt. A produce cannot be undone and the drawer is
     * dismissible; the confirmation that survives closing it is the thing an operator has afterwards
     * to say the write happened. It quotes a position rather than saying "sent", because a position
     * is something they can go and look at.
     */
    const toast = page.locator(".kui-notice").first();
    await expect(toast).toBeVisible({ timeout: 20_000 });
    await expect(toast).toContainText(/published/i);
    await expect(toast).toContainText(/partition \d+, offset \d+/i);

    await removeTopic(api, name);
  });

  test("a copy that moved nothing raises a warning toast, not a green tick", async ({
    page,
    api,
  }) => {
    /*
     * The state this screen is shaped around, produced against a real broker rather than described:
     * a range naming offsets that are not in the log answers **200** with `{"read":0,"written":0}`.
     * No error, no warning, nothing. A source topic that has never been written to is the cheapest
     * honest way to get there — offset 0 exists in no log — and it is what an operator meets when
     * retention has eaten the range they chose.
     *
     * The tone is the assertion. The wording is what a reader skims; the colour is what they see
     * from across the room, and a green tick here sends them to look at a destination they believe
     * now holds their records.
     */
    const source = scratchTopic("copy-src");
    const destination = scratchTopic("copy-dst");
    await api.post(`/api/v1/clusters/${CLUSTER}/topics`, {
      name: source,
      partitions: 1,
      config: {},
    });
    await api.post(`/api/v1/clusters/${CLUSTER}/topics`, {
      name: destination,
      partitions: 1,
      config: {},
    });

    await page.goto(`/ui/clusters/${CLUSTER}/topics/${source}/messages`);
    await page.getByRole("button", { name: /copy records out/i }).click();

    const dialog = page.getByRole("dialog");
    await expect(dialog).toBeVisible();
    await dialog.getByLabel("Copy into topic").fill(destination);
    await dialog.getByLabel("From offset").fill("0");
    await dialog.getByLabel("Until offset").fill("1");
    await dialog.getByLabel(`Type ${destination} to confirm`).fill(destination);
    await dialog.getByRole("button", { name: /^copy records$/i }).click();

    // The dialog stays open and states it; the toast is what survives the dialog being dismissed.
    await expect(dialog).toContainText("Nothing was copied", { timeout: 20_000 });

    const toast = page.locator(".kui-notice").first();
    await expect(toast).toBeVisible({ timeout: 20_000 });
    await expect(toast).toContainText("Nothing was copied");
    await expect(toast).not.toContainText("Records copied");
    // `warning`, and the class is the whole difference between "it worked" and "read this".
    await expect(toast).toHaveClass(/kui-notice--warning/);

    await removeTopic(api, source);
    await removeTopic(api, destination);
  });
});
