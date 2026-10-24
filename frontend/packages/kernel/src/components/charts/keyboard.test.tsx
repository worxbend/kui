import { Errored, createSignal, flush } from "solid-js";
import { describe, expect, it } from "vitest";

import { mount } from "../testing.js";
import { BarChart } from "./BarChart.jsx";
import { Histogram } from "./Histogram.jsx";
import { LineChart } from "./LineChart.jsx";

const categories = ["first", "middle", "last"];
const series = (size: number) => [
  { label: "Count", tone: "series-1" as const, points: [1, 2, 3].slice(0, size) },
];

const charts = [
  {
    name: "BarChart",
    render: (size: () => number) => (
      <BarChart label="Counts" categories={categories.slice(0, size())} series={series(size())} />
    ),
    announcements: ["first: Count 1", "middle: Count 2", "last: Count 3"],
    emptyAnnouncement: ": Count —",
  },
  {
    name: "LineChart",
    render: (size: () => number) => (
      <LineChart label="Counts" categories={categories.slice(0, size())} series={series(size())} />
    ),
    announcements: ["first: Count 1", "middle: Count 2", "last: Count 3"],
    emptyAnnouncement: ": Count —",
  },
  {
    name: "Histogram",
    render: (size: () => number) => (
      <Histogram
        label="Counts"
        buckets={[
          { from: 0, to: 1, count: 1 },
          { from: 1, to: 2, count: 2 },
          { from: 2, to: 3, count: 3 },
        ].slice(0, size())}
      />
    ),
    announcements: ["0 – 1: 1", "1 – 2: 2", "2 – 3: 3"],
    emptyAnnouncement: "",
  },
];

function press(container: HTMLElement, key: string): boolean {
  const event = new KeyboardEvent("keydown", { key, bubbles: true, cancelable: true });
  container.querySelector<HTMLElement>(".kui-plot__surface")!.dispatchEvent(event);
  flush();
  return event.defaultPrevented;
}

function announcement(container: HTMLElement): string | null {
  return container.querySelector(".kui-visually-hidden[role='status']")!.textContent;
}

describe.each(charts)("$name keyboard navigation", ({ render, announcements, emptyAnnouncement }) => {
  it("announces movement and clamps both ends, preventing only navigation defaults", () => {
    const { container, dispose } = mount(() => render(() => 3));
    try {
      flush();
      expect(announcement(container)).toBe("");
      const steps = [
        ["Enter", false, ""],
        ["ArrowLeft", true, announcements[2]],
        ["ArrowRight", true, announcements[2]],
        ["Home", true, announcements[0]],
        ["ArrowLeft", true, announcements[0]],
        ["ArrowRight", true, announcements[1]],
        ["End", true, announcements[2]],
        ["Escape", false, ""],
        ["ArrowRight", true, announcements[0]],
        ["Enter", false, announcements[0]],
      ] as const;
      for (const [key, prevented, expected] of steps) {
        expect(press(container, key), key).toBe(prevented);
        expect(announcement(container), key).toBe(expected);
      }
    } finally {
      dispose();
    }
  });

  it("uses the current data length as buckets grow and shrink", () => {
    const [size, setSize] = createSignal(2);
    const { container, dispose } = mount(() => render(size));
    try {
      press(container, "End");
      expect(announcement(container)).toBe(announcements[1]);
      setSize(3);
      flush();
      press(container, "End");
      expect(announcement(container)).toBe(announcements[2]);
      press(container, "Home");
      setSize(2);
      flush();
      press(container, "End");
      expect(announcement(container)).toBe(announcements[1]);
    } finally {
      dispose();
    }
  });

  it("preserves empty-data announcements and default prevention", () => {
    const { container, dispose } = mount(() => render(() => 0));
    try {
      flush();
      expect(announcement(container)).toBe("");
      for (const key of ["ArrowLeft", "ArrowRight"]) {
        expect(press(container, key), key).toBe(true);
        expect(announcement(container), key).toBe("");
      }
      for (const key of ["Home", "End"]) {
        expect(press(container, key), key).toBe(true);
        expect(announcement(container), key).toBe(emptyAnnouncement);
      }
      expect(press(container, "Escape")).toBe(false);
      expect(announcement(container)).toBe("");
    } finally {
      dispose();
    }
  });
});

describe.each(charts)("$name with data shrinking below the cursor", ({ render }) => {
  it("stays rendered without reaching an error boundary", () => {
    const [size, setSize] = createSignal(2);
    const { container, dispose } = mount(() => (
      <Errored fallback={error => <p role="alert">{String(error())}</p>}>{render(size)}</Errored>
    ));
    try {
      press(container, "End");
      setSize(1);
      flush();
      expect(container.querySelector("[role='alert']")).toBeNull();
      expect(container.querySelector(".kui-plot__surface")).not.toBeNull();
    } finally {
      dispose();
    }
  });
});

describe("Histogram selection when its bucket is removed", () => {
  const two = [
    { from: 0, to: 1, count: 1 },
    { from: 1, to: 2, count: 2 },
  ];
  const one = [{ from: 0, to: 1, count: 1 }];

  it("clears the selection and hides the tooltip", () => {
    const [buckets, setBuckets] = createSignal(two);
    const { container, dispose } = mount(() => (
      <Errored fallback={error => <p role="alert">{String(error())}</p>}>
        <Histogram label="Counts" buckets={buckets()} />
      </Errored>
    ));
    try {
      press(container, "End");
      expect(announcement(container)).toBe("1 – 2: 2");
      expect(container.querySelector(".kui-plot__tooltip")).not.toBeNull();
      setBuckets(one);
      flush();
      expect(container.querySelector("[role='alert']")).toBeNull();
      expect(container.querySelector(".kui-plot__surface")).not.toBeNull();
      expect(container.querySelector(".kui-plot__tooltip")).toBeNull();
      expect(container.querySelector(".kui-plot__group--active")).toBeNull();
      expect(announcement(container)).toBe("");
      // Cleared, not hidden: the bucket coming back does not bring the old selection with it.
      setBuckets(two);
      flush();
      expect(container.querySelector(".kui-plot__tooltip")).toBeNull();
      expect(announcement(container)).toBe("");
      // And the keyboard starts afresh rather than from the vanished index.
      press(container, "ArrowRight");
      expect(announcement(container)).toBe("0 – 1: 1");
    } finally {
      dispose();
    }
  });

  it("keeps a selection that is still in range", () => {
    const [buckets, setBuckets] = createSignal(two);
    const { container, dispose } = mount(() => <Histogram label="Counts" buckets={buckets()} />);
    try {
      press(container, "Home");
      setBuckets(one);
      flush();
      expect(announcement(container)).toBe("0 – 1: 1");
      expect(container.querySelector(".kui-plot__tooltip-title")?.textContent).toBe("0 – 1");
    } finally {
      dispose();
    }
  });
});
