/**
 * The browse grammar.
 *
 * This is the file worth testing hardest, because it is the one that is wrong *silently*. A
 * component that renders the wrong thing is visible in a story; a query string that spells `seekTo`
 * with the wrong separator produces a 400 nothing in this repository notices, and a browse that
 * quietly drops a partition filter produces a page of records that look entirely plausible and are
 * from the wrong place.
 *
 * The exact strings asserted below are the ones the server's own `BrowseParamsSuite` accepts. That
 * agreement is the only check this seam has — see the header of `browse.ts` for why the names
 * cannot be generated the way the error codes are.
 */

import { describe, expect, test } from "vitest";
import {
  DEFAULT_BROWSE,
  decodeSeek,
  encodeSeek,
  fromParams,
  offeredSerde,
  offsetOf,
  partitionSummary,
  queryString,
  seekFor,
  seekKind,
  type BrowseQuery,
} from "./browse.js";

describe("the seek grammar", () => {
  test("spells the four simple forms the way the server reads them", () => {
    expect(encodeSeek({ kind: "beginning" })).toEqual(["beginning"]);
    expect(encodeSeek({ kind: "latest" })).toEqual(["latest"]);
    expect(encodeSeek({ kind: "offset", offset: "41284" })).toEqual(["offset::41284"]);
    expect(encodeSeek({ kind: "timestamp", epochMillis: 1767225600000 })).toEqual([
      "timestamp::1767225600000",
    ]);
  });

  test("spells a per-partition seek as one value per partition, in partition order", () => {
    const seek = {
      kind: "atOffsets",
      offsets: new Map([
        [3, "250"],
        [0, "100"],
      ]),
    } as const;
    // Sorted, so that the same set sent in any order produces the same URL — which is what makes
    // two links to the same place compare equal.
    expect(encodeSeek(seek)).toEqual(["0::100", "3::250"]);
  });

  test("reads back everything it writes", () => {
    for (const seek of [
      { kind: "beginning" },
      { kind: "latest" },
      { kind: "offset", offset: "41284" },
      { kind: "timestamp", epochMillis: 1767225600000 },
    ] as const) {
      expect(decodeSeek(encodeSeek(seek))).toEqual(seek);
    }
  });

  test("keeps an offset past 2^53 exactly", () => {
    // 9007199254740993 is the first integer a JavaScript double cannot hold. Parsing it as a
    // number and printing it again gives 9007199254740992 — a different record.
    const huge = "9007199254740993";
    const decoded = decodeSeek([`offset::${huge}`]);
    expect(decoded).toEqual({ kind: "offset", offset: huge });
    expect(encodeSeek(decoded as never)).toEqual([`offset::${huge}`]);
  });

  test("refuses a mixture of forms rather than picking one", () => {
    // The server refuses this rather than resolving it by a precedence rule. A browser that
    // quietly took one half would show a range nobody asked for.
    expect(decodeSeek(["beginning", "0::100"])).toBeUndefined();
  });

  test("refuses a repeated partition", () => {
    expect(decodeSeek(["0::100", "0::200"])).toBeUndefined();
  });

  test("is undefined rather than throwing on nonsense", () => {
    expect(decodeSeek([])).toBeUndefined();
    expect(decodeSeek(["sideways"])).toBeUndefined();
    expect(decodeSeek(["offset::not-a-number"])).toBeUndefined();
  });
});

describe("the query string", () => {
  test("spells a default browse as nothing but its seek", () => {
    // Everything else is a default the server already holds. A URL that spelled out every default
    // is one a person cannot read, and cannot tell from one that was deliberately configured.
    expect(queryString(DEFAULT_BROWSE)).toBe("seekTo=latest");
  });

  test("repeats the partition parameter, in order", () => {
    const query: BrowseQuery = { ...DEFAULT_BROWSE, partitions: [11, 0, 3] };
    expect(queryString(query)).toBe("seekTo=latest&partition=0&partition=3&partition=11");
  });

  test("escapes the filter text", () => {
    const query: BrowseQuery = { ...DEFAULT_BROWSE, contains: "status=CAPTURED & amount>10" };
    expect(queryString(query)).toBe(
      "seekTo=latest&q=status%3DCAPTURED%20%26%20amount%3E10",
    );
  });

  test("omits live when it is off", () => {
    expect(queryString({ ...DEFAULT_BROWSE, live: false })).not.toContain("live");
    expect(queryString({ ...DEFAULT_BROWSE, live: true })).toContain("live=true");
  });

  test("sends a cursor instead of a seek, and never both", () => {
    const query: BrowseQuery = {
      ...DEFAULT_BROWSE,
      live: true,
      seek: { kind: "offset", offset: "10" },
      cursor: "abc.def",
    };
    const written = queryString(query);
    // The server refuses the pair rather than picking one, so the client must send one or the
    // other; and a continuation is not a tail, so `live` goes too.
    expect(written).not.toContain("seekTo");
    expect(written).not.toContain("live");
    expect(written).toContain("cursor=abc.def");
  });

  test("carries the smart filter alongside a cursor", () => {
    // Unlike the seek: a cursor names a position, and which records at that position are worth
    // delivering is still the filter's decision.
    const written = queryString({
      ...DEFAULT_BROWSE,
      cursor: "abc",
      filterId: "f1",
      filterSource: "value.amount > 10",
    });
    expect(written).toContain("filterId=f1");
    expect(written).toContain("filterSource=value.amount%20%3E%2010");
  });

  test("omits an Automatic serde", () => {
    expect(queryString({ ...DEFAULT_BROWSE, keySerde: "", valueSerde: "JSON" })).toBe(
      "seekTo=latest&valueSerde=JSON",
    );
  });
});

describe("reading a browse back out of a link", () => {
  test("round-trips through a URL", () => {
    const query: BrowseQuery = {
      seek: { kind: "offset", offset: "41284" },
      partitions: [0, 3],
      limit: 200,
      contains: "CAPTURED",
      keySerde: "INT64",
      valueSerde: "JSON",
      live: false,
      filterId: "f1",
      filterSource: "value.amount > 10",
    };
    expect(fromParams(new URLSearchParams(queryString(query)))).toEqual(query);
  });

  test("accepts the comma-separated spelling of the partition list", () => {
    // The server's own codec accepts both spellings, so a link written either way must read back
    // the same here or the two halves disagree about what a link means.
    expect(fromParams(new URLSearchParams("partition=0,3,11")).partitions).toEqual([0, 3, 11]);
  });

  test("costs the reader one setting, not the whole screen, when a value no longer parses", () => {
    const query = fromParams(new URLSearchParams("seekTo=sideways&partition=x&keySerde=MADE_UP"));
    expect(query.seek).toEqual(DEFAULT_BROWSE.seek);
    expect(query.partitions).toEqual([]);
    expect(query.keySerde).toBeUndefined();
  });

  test("never reads a cursor out of a link", () => {
    // A cursor is a five-minute-old statement about offsets. A link carrying one would take the
    // recipient to a page that no longer exists, or quietly to a different one.
    expect(fromParams(new URLSearchParams("cursor=abc")).cursor).toBeUndefined();
  });
});

describe("what the controls show", () => {
  test("a per-partition seek reports as an offset seek", () => {
    const seek = { kind: "atOffsets", offsets: new Map([[0, "100"]]) } as const;
    expect(seekKind(seek)).toBe("offset");
  });

  test("the offset box shows a per-partition seek only when the partitions agree", () => {
    const agreeing = {
      kind: "atOffsets",
      offsets: new Map([
        [0, "100"],
        [1, "100"],
      ]),
    } as const;
    const differing = {
      kind: "atOffsets",
      offsets: new Map([
        [0, "100"],
        [1, "250"],
      ]),
    } as const;
    expect(offsetOf(agreeing)).toBe("100");
    // Showing one of them would silently discard the others the moment somebody pressed Read.
    expect(offsetOf(differing)).toBeUndefined();
  });

  test("an offset seek with an empty box means the start of the range, not no seek", () => {
    expect(seekFor("offset", "", undefined)).toEqual({ kind: "offset", offset: "0" });
  });

  test("the partition summary never reads zero", () => {
    // Empty means every partition — that is what the server means by the parameter being absent —
    // and a control reading "0 selected" for "all" says the opposite of what it does.
    expect(partitionSummary([], 12)).toBe("all 12");
    expect(partitionSummary([0, 1, 2], 3)).toBe("all 3");
    expect(partitionSummary([3], 12)).toBe("p 3");
    expect(partitionSummary([0, 3], 12)).toBe("2 of 12");
  });

  test("a serde the menu does not list falls back to Automatic", () => {
    expect(offeredSerde("JSON")).toBe("JSON");
    // A link naming a serde this build does not list is read as Automatic, not sent on.
    expect(offeredSerde("Avro_from_a_newer_kui")).toBe("");
  });
});
