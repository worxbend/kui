import { describe, expect, it, vi } from "vitest";
import { flush } from "solid-js";
import { createApiClient, createCsrfTokens, FallbackBootstrap, type components } from "@kui/api";
import { ResendDialog } from "./ResendDialog.jsx";
import { readingOf, resend } from "./resend.js";
import { mount } from "./testing.js";

type Receipt = components["schemas"]["ResendResultDto"];
const recordFailure = { partition: 2, offset: 17, code: "KUI-UPSTREAM", error: "Broker rejected the write" };
const rangeFailure = { range: { partition: 3, from: 20, until: 30 }, code: "KUI-VALIDATION", error: "Source scan stopped at its byte budget" };

describe("resend HTTP-200 failures reach the receipt", () => {
  it.each([
    { label: "mixed record and range failures", read: 2, written: 1, failures: [recordFailure], rangeFailures: [rangeFailure] },
    { label: "incomplete range after successful writes", read: 2, written: 2, failures: [], rangeFailures: [rangeFailure] },
    { label: "total range failure", read: 0, written: 0, failures: [], rangeFailures: [rangeFailure] },
    { label: "total record failure", read: 1, written: 0, failures: [recordFailure], rangeFailures: [] },
  ])("reports $label without offering a whole-batch retry", async ({ label: _label, ...counts }) => {
    const receipt = { toTopic: "replay", ...counts } satisfies Receipt;
    const csrf = createCsrfTokens();
    csrf.settle("token");
    const fetch = vi.fn(async () => Response.json(receipt));
    const api = createApiClient({ bootstrap: FallbackBootstrap, csrf, origin: "https://example.test", fetch });
    const answer = await resend(api, "prod", "orders", {
      toTopic: "replay", ranges: [{ partition: 2, from: "16", until: "18" }, { partition: 3, from: "20", until: "30" }],
    });
    expect(answer.ok).toBe(true);
    if (!answer.ok) throw new Error("Expected an HTTP-200 receipt");
    expect(answer.value.failures).toEqual(receipt.failures);
    expect(answer.value.rangeFailures).toEqual(receipt.rangeFailures);
    expect(readingOf(answer.value).kind).toBe("incomplete");
    const onSend = vi.fn();
    const ui = mount(() => <ResendDialog open topic="orders" onClose={() => undefined} onSend={onSend} state={{ kind: "done", value: answer.value }} />);
    try {
      await flush();
      const panels = document.body.querySelectorAll<HTMLElement>("[role='dialog']");
      const panel = panels[panels.length - 1];
      expect(panel).toBeDefined();
      const text = panel?.textContent ?? "";
      expect(text).toContain("Copy incomplete");
      expect(text).toContain("Do not repeat the whole batch");
      expect(text).not.toContain("retention removed");
      expect(text).not.toContain("reported no error");
      if (receipt.failures.length > 0) {
        expect(text).toContain("Partition 2, offset 17");
        expect(text).toContain(recordFailure.code);
        expect(text).toContain(recordFailure.error);
      }
      if (receipt.rangeFailures.length > 0) {
        expect(text).toContain("Partition 3, from 20 until 30 (exclusive)");
        expect(text).toContain(rangeFailure.code);
        expect(text).toContain(rangeFailure.error);
      }
      expect(panel?.querySelector(".kui-resend__receipt--complete")).toBeNull();
      expect([...panel?.querySelectorAll("button") ?? []].some((b) => /copy records|retry/i.test(b.textContent ?? ""))).toBe(false);
      expect(fetch).toHaveBeenCalledTimes(1);
      expect(onSend).not.toHaveBeenCalled();
    } finally { ui.dispose(); }
  });
});
