/**
 * The smart-filter and resend mappings, against documents a real gateway produced.
 *
 * The reason these exist is the one `feature-topics/src/recorded.test.ts` gives: the server
 * documents section payloads with `Schema.any`, so a misspelled field is a type-correct `undefined`
 * that every screen renders as an em dash — and a wholly broken mapping looks exactly like a cluster
 * that is not answering. Neither `tsc` nor a hand-written fixture can catch that, because a
 * hand-written fixture is written from the same misunderstanding as the code.
 *
 * These two endpoints have a sharper version of the problem, and it is the reason the documents in
 * `./recorded/` were captured one at a time rather than assumed:
 *
 * - `filter test` sends **`"error": null`**, while the generated type says `error?: string`. Every
 *   `undefined` comparison against that field is wrong in one direction or the other, and both
 *   readings type-check.
 * - `resend` answers **200 with `read: 0, written: 0`** for a range retention has removed. There is
 *   no error to notice; a mapping that only looks at `answer.ok` reports it as a success.
 * - `resend`'s request field names are `from`/`until`, not `fromOffset`/`toOffset`, and the wrong
 *   ones are refused as an undecodable body rather than as a named field.
 * - `ResendRequestDto.ranges` is **optional in the schema and required by the decoder**.
 * - The 10 000-record cap is in no schema at all; it exists only in the refusal.
 *
 * Re-record them against a gateway with a real cluster (`development` on the demo stack):
 *
 *   TOK=$(curl -s -c /tmp/kui.jar localhost:18080/api/v1/auth/me |
 *     python3 -c 'import sys,json;print(json.load(sys.stdin)["csrfToken"])')
 *   curl -s -b /tmp/kui.jar -H "X-Csrf-Token: $TOK" -H 'Content-Type: application/json' -X POST \
 *     localhost:18080/api/v1/clusters/development/messages/filters \
 *     -d '{"source":"record.value.status == \"CAPTURED\"","name":"captured orders"}' |
 *     python3 -m json.tool > src/recorded/filter-registered.json
 *
 * A diff in any of them is a contract change and should be read as one.
 */
import { describe, expect, it, vi } from "vitest";
import type { KuiApiClient } from "@kui/api";

import registeredDocument from "./recorded/filter-registered.json" with { type: "json" };
import compileErrorDocument from "./recorded/filter-compile-error.json" with { type: "json" };
import matchedDocument from "./recorded/filter-test-matched.json" with { type: "json" };
import noMatchDocument from "./recorded/filter-test-no-match.json" with { type: "json" };
import failedDocument from "./recorded/filter-test-failed.json" with { type: "json" };
import notBooleanDocument from "./recorded/filter-test-not-boolean.json" with { type: "json" };
import copiedDocument from "./recorded/resend-copied.json" with { type: "json" };
import nothingLeftDocument from "./recorded/resend-nothing-left.json" with { type: "json" };
import noRangesDocument from "./recorded/resend-no-ranges.json" with { type: "json" };
import tooManyDocument from "./recorded/resend-too-many.json" with { type: "json" };

import { registerFilter, testFilter, verdictOf } from "./filters.js";
import { MAX_RESEND_RECORDS, readingOf, resend, resendDraftProblem } from "./resend.js";

/** A client whose every verb answers with one document, as the real one would on success. */
function client(document: unknown): KuiApiClient {
  const post = vi.fn(async () => ({ ok: true, value: document }));
  return { get: post, post, put: post, delete: post, patch: post, raw: {} } as unknown as KuiApiClient;
}

/**
 * A client that fails the way the gateway does, from a recorded error envelope.
 *
 * The envelope's own shape is what `userMessage` reads, so building it from the recorded document
 * keeps the code and the message an operator sees tied to the same capture.
 */
function failing(document: { readonly code: string; readonly message: string }): KuiApiClient {
  const post = vi.fn(async () => ({
    ok: false,
    error: { kind: "envelope", code: document.code, message: document.message, details: [] },
  }));
  return { get: post, post, put: post, delete: post, patch: post, raw: {} } as unknown as KuiApiClient;
}

const RECORD = {
  partition: 3,
  offset: 18_442_901,
  timestamp: "2026-09-05T10:00:08Z",
  timestampType: "CreateTime",
  key: { kind: "string", text: "ord_9f21ac", serde: "String", properties: {} },
  value: { kind: "json", text: '{"orderId":"ord_9f21ac","status":"CAPTURED"}', serde: "String", properties: {} },
  headers: { "content-type": "application/json" },
  keySize: 10,
  valueSize: 43,
  headersSize: 30,
  deserializeErrors: [],
};

describe("the recorded filter registration", () => {
  it("reads the id out of the document and keeps the source beside it", async () => {
    const answer = await registerFilter(
      client(registeredDocument),
      "development",
      'record.value.status == "CAPTURED"',
      "captured orders",
    );
    expect(answer.ok).toBe(true);
    if (!answer.ok) return;

    // The id is the server's, verbatim — sixteen hex characters of a sha256 of the source.
    expect(answer.value.id).toBe(registeredDocument.id);
    expect(answer.value.id).toMatch(/^[0-9a-f]{16}$/);

    /* The source travels back with it. A browse sends `filterId` and `filterSource` together so
     * that a replica which never saw the registration compiles it instead of refusing the filter,
     * and dropping the source here would make that impossible at the call site. */
    expect(answer.value.source).toBe('record.value.status == "CAPTURED"');
    expect(answer.value.name).toBe("captured orders");
  });

  it("does not decode the document to an undefined id", async () => {
    // The property, stated separately: an id read from a field that does not exist is `undefined`,
    // which would travel into a browse URL as the string "undefined" and be refused there instead.
    const answer = await registerFilter(client(registeredDocument), "development", "true");
    if (!answer.ok) throw new Error("expected the registration to succeed");
    expect(answer.value.id).not.toBeUndefined();
    expect(answer.value.id.length).toBeGreaterThan(0);
  });

  it("reports a compile failure with the line and column the server named", async () => {
    const answer = await registerFilter(
      failing(compileErrorDocument),
      "development",
      "record.value.status ==",
    );
    expect(answer.ok).toBe(false);
    if (answer.ok) return;
    expect(answer.error).toMatchObject({ code: "KUI-FILTER-COMPILE" });

    /* The position is in `details[0].restrictions[0]`, not in the message, and it is what an editor
     * would underline. Pinned here so that a server which moves it is noticed. */
    expect(compileErrorDocument.details[0]?.field).toBe("filterSource");
    expect(compileErrorDocument.details[0]?.restrictions[0]).toContain("line 1, column 22");
  });
});

describe("the recorded filter preview", () => {
  it("reads a match", async () => {
    const answer = await testFilter(client(matchedDocument), "development", "true", RECORD);
    if (!answer.ok) throw new Error("expected the preview to answer");
    expect(answer.value).toEqual({ kind: "matched" });
  });

  it("reads a record the filter simply did not match", async () => {
    const answer = await testFilter(client(noMatchDocument), "development", "false", RECORD);
    if (!answer.ok) throw new Error("expected the preview to answer");
    expect(answer.value).toEqual({ kind: "no-match" });
  });

  it("keeps an expression that threw apart from one that did not match", async () => {
    /* The whole reason this endpoint has three outcomes. Both documents carry `matched: false`; one
     * of them is a filter that is broken on every record and the other is a filter that is working.
     * A mapping that read only `matched` would return the same verdict for both, and the browse
     * they lead to would show an empty list either way. */
    const answer = await testFilter(client(failedDocument), "development", "x", RECORD);
    if (!answer.ok) throw new Error("expected the preview to answer");
    expect(answer.value.kind).toBe("failed");
    if (answer.value.kind !== "failed") return;
    expect(answer.value.reason).toBe("evaluation error at <input>:12: key 'nosuch' is not present in map.");

    const plain = await testFilter(client(noMatchDocument), "development", "x", RECORD);
    if (!plain.ok) throw new Error("expected the preview to answer");
    expect(plain.value.kind).toBe("no-match");
  });

  it("reads a legal expression that is not a predicate as a failure", async () => {
    /* `record.offset` registers successfully — CEL has no opinion about its type — and only fails
     * when it is run. This is the state that justifies the preview existing at all: without it the
     * discovery happens partway through a browse over a production topic. */
    const answer = await testFilter(client(notBooleanDocument), "development", "record.offset", RECORD);
    if (!answer.ok) throw new Error("expected the preview to answer");
    expect(answer.value).toEqual({
      kind: "failed",
      reason: "the filter returned Long rather than true or false",
    });
  });

  it("does not read the server's explicit null error as a failure", async () => {
    /* The drift this test exists for. The document says `"error": null` while the generated type
     * says `error?: string`, so `error !== undefined` is true for every result — a reading that
     * turns every match into a failure, and type-checks. */
    expect(matchedDocument.error).toBeNull();
    expect(verdictOf(matchedDocument)).toEqual({ kind: "matched" });
    expect(verdictOf(noMatchDocument)).toEqual({ kind: "no-match" });
  });
});

describe("the recorded resend", () => {
  const range = (from: string, until: string) => ({ toTopic: "scratch.jm-test", ranges: [{ partition: 0, from, until }] });

  it("reads the tally and calls a complete copy complete", async () => {
    const answer = await resend(client(copiedDocument), "development", "audit.log.raw", range("0", "3"));
    if (!answer.ok) throw new Error("expected the resend to answer");
    expect(answer.value).toEqual({ toTopic: "scratch.jm-test", read: 3, written: 3, requested: 3, failures: [], rangeFailures: [] });
    expect(readingOf(answer.value)).toEqual({ kind: "complete" });
  });

  it("does not let a copy that moved nothing pass for a success", async () => {
    /* The state this whole file is here for. The range named offsets retention had removed, and the
     * server answered 200 with two zeroes and no warning of any kind. `answer.ok` is true. */
    const answer = await resend(
      client(nothingLeftDocument),
      "development",
      "audit.log.raw",
      range("50", "60"),
    );
    expect(answer.ok).toBe(true);
    if (!answer.ok) return;

    // The zeroes are facts and are carried as the figures they are, never as absent values.
    expect(answer.value.read).toBe(0);
    expect(answer.value.written).toBe(0);
    expect(readingOf(answer.value)).toEqual({ kind: "nothing", requested: 10 });
  });

  it("separates a source that had fewer records from a copy that lost some", async () => {
    // Read fewer than asked for: retention took part of the range before the copy ran.
    expect(readingOf({ toTopic: "t", read: 6, written: 6, requested: 10 })).toEqual({
      kind: "short",
      missing: 4,
    });
    // Wrote fewer than were read: the copy itself failed part-way, and what landed stayed landed.
    expect(readingOf({ toTopic: "t", read: 10, written: 4, requested: 10 })).toEqual({
      kind: "partial",
      lost: 6,
    });
  });

  it("refuses an empty range list the way the server does", async () => {
    expect(noRangesDocument.code).toBe("KUI-VALIDATION");
    expect(noRangesDocument.message).toBe("a resend names no offsets, so there is nothing to copy");
    // The client says the same thing before the request, so the operator is still holding the form.
    expect(resendDraftProblem({ toTopic: "scratch.jm-test", ranges: [] })).toContain("nothing to copy");
  });

  it("knows the record cap that appears in no schema", async () => {
    /* `docs/api/openapi.browser.json` documents no limit on a resend. The number below exists only
     * in the refusal the server sends, which is why it is pinned against a recorded one. */
    expect(tooManyDocument.message).toContain(`at most ${String(MAX_RESEND_RECORDS)} records`);
    expect(tooManyDocument.details[0]?.field).toBe("ranges");

    const problem = resendDraftProblem(range("0", "900000000"));
    expect(problem).toContain("10,000");
  });

  it("sends the field names the decoder requires, not the ones the schema suggests are enough", async () => {
    /* Two drifts in one request. The offsets are `from`/`until` — `fromOffset`/`toOffset` come back
     * as an undecodable body rather than as a named field error — and `ranges` is optional in the
     * schema and required by the decoder, so it is always sent. */
    const post = vi.fn((_path: string, _options: unknown) => Promise.resolve({ ok: true, value: copiedDocument }));
    const api = { get: post, post, put: post, delete: post, patch: post, raw: {} } as unknown as KuiApiClient;

    await resend(api, "development", "audit.log.raw", range("0", "3"));

    const sent = post.mock.calls[0]?.[1] as {
      readonly body: { readonly ranges: readonly Record<string, number>[] };
    };
    expect(sent.body.ranges).toEqual([{ partition: 0, from: 0, until: 3 }]);
    expect(Object.keys(sent.body.ranges[0] ?? {}).sort()).toEqual(["from", "partition", "until"]);
  });
});
