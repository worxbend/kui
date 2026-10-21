/**
 * A distribution over server-supplied buckets — the **Message size distribution** card of
 * SCREENS-V4.md §3.5.
 *
 * ## Why this is not `BarChart`
 *
 * Two differences, and each of them is structural rather than cosmetic.
 *
 * 1. **Tone is per bar, not per series.** In `BarChart` colour is a property of a `Series`, so
 *    every bar of a series is painted alike — which is right for throughput, where the ink means
 *    "produce" or "consume" and nothing else. A histogram is one series with three inks in it: the
 *    modal bucket solid, its neighbours muted, the oversize tail amber. There is no way to say
 *    that in a `Series`, and `BarChart`'s own header already disclaims being a histogram.
 * 2. **The axis comes from boundaries, not from categories.** `BarChart` prints the category
 *    string it was handed. A histogram's ticks are the *edges between* buckets — `256 B`, `2 KB`,
 *    `16 KB` — and those edges come from the server. The axis must not invent them, so this
 *    component takes numbers and a formatter rather than pre-rendered strings, and a caller that
 *    cannot say where its buckets end cannot accidentally have labels made up for it.
 *
 * Everything else is deliberately the same as `BarChart`, because a family that behaves
 * differently in each member is not a family: an observed container, real-pixel geometry, a
 * keyboard cursor with a live region, a hidden `ChartDataTable`, and top-rounded bars.
 */
import { For, Show, createSignal, createUniqueId, type Component } from "solid-js";
import { ChartDataTable } from "./ChartDataTable.jsx";
import { useElementSize } from "./elementSize.js";
import { ABSENT, formatCount } from "./format.js";
import { createChartKeyDown } from "./keyboard.js";
import { topRoundedRect } from "./plot.js";
import { toneColor, type ChartTone } from "./tone.js";

export interface HistogramBucket {
  /** The inclusive lower edge, in the quantity's own units. */
  readonly from: number;
  /** The exclusive upper edge. Absent means the open-ended top bucket — `64 KB+`. */
  readonly to?: number | undefined;
  readonly count: number;
  /**
   * Overrides this bar's ink. Used for the oversize tail, which the *server* identifies: an
   * oversize threshold nobody served draws no amber at all rather than guessing at 16 KB.
   */
  readonly tone?: ChartTone | undefined;
}

/** One of the chips beneath the plot: `p50 · 1.1 KB`. */
export interface HistogramReadout {
  readonly label: string;
  readonly value: string;
  readonly tone?: ChartTone | undefined;
}

export interface HistogramProps {
  readonly buckets: readonly HistogramBucket[];
  /** Names the chart for a screen reader and captions the hidden table. */
  readonly label: string;
  readonly height?: number | undefined;
  /** Renders a bucket edge for the axis and the table: `1024` -> `1 KB`. */
  readonly formatBoundary?: ((value: number) => string) | undefined;
  /** Renders a count for the tooltip and the table. Defaults to a grouped integer. */
  readonly formatCount?: ((value: number) => string) | undefined;
  /** A tick on every Nth boundary. The design prints five over twelve buckets, so: three. */
  readonly tickEvery?: number | undefined;
  readonly readouts?: readonly HistogramReadout[] | undefined;
  readonly emptyMessage?: string | undefined;
}

const BAR_RADIUS = 3;
const AXIS_HEIGHT = 18;

export const Histogram: Component<HistogramProps> = props => {
  const box = useElementSize({ width: 640, height: 160 });
  const [cursor, setCursor] = createSignal<number | undefined>(undefined);
  const tableId = createUniqueId();

  const height = (): number => props.height ?? 160;
  const plotHeight = (): number => Math.max(20, height() - AXIS_HEIGHT);
  const width = (): number => box.size().width;
  const count = (): number => props.buckets.length;

  const boundary = (value: number): string => props.formatBoundary?.(value) ?? String(value);
  const amount = (value: number): string => props.formatCount?.(value) ?? formatCount(value);

  /** `256 B – 512 B`, or `64 KB+` for the open-ended tail. The row label in the hidden table. */
  const bucketLabel = (bucket: HistogramBucket): string =>
    bucket.to === undefined ? `${boundary(bucket.from)}+` : `${boundary(bucket.from)} – ${boundary(bucket.to)}`;

  const max = (): number =>
    props.buckets.reduce((m, b) => (Number.isFinite(b.count) && b.count > m ? b.count : m), 0);

  /** An empty histogram is one with no buckets, or one whose every bucket counted nothing. */
  const empty = (): boolean => count() === 0 || max() <= 0;

  /**
   * The modal bucket, found here rather than asked for.
   *
   * "Which bucket is tallest" is arithmetic over data the component already holds, and making
   * every call site recompute it is how two cards end up disagreeing about the same distribution.
   * An explicit `bucket.tone` always wins, so the oversize tail still overrides it.
   *
   * A tie has no mode worth pointing at — two equal peaks with one of them singled out would be a
   * claim about the data that is not true — so a tie highlights nothing.
   */
  const modal = (): number | undefined => {
    let best: number | undefined;
    let ties = 0;
    props.buckets.forEach((bucket, index) => {
      if (!Number.isFinite(bucket.count) || bucket.count <= 0) return;
      if (best === undefined || bucket.count > (props.buckets[best] as HistogramBucket).count) {
        best = index;
        ties = 1;
      } else if (bucket.count === (props.buckets[best] as HistogramBucket).count) {
        ties += 1;
      }
    });
    return ties === 1 ? best : undefined;
  };

  const toneOf = (bucket: HistogramBucket, index: number): ChartTone =>
    bucket.tone ?? (index === modal() ? "accent" : "neutral");

  const slotWidth = (): number => (count() === 0 ? 0 : width() / count());
  /** A ceiling rather than a target: four buckets draw four bars, not four slabs. */
  const barWidth = (): number => Math.max(1, Math.min(18, slotWidth() * 0.72));
  const barX = (index: number): number => slotWidth() * (index + 0.5) - barWidth() / 2;

  const barHeight = (value: number): number => {
    const m = max();
    // The guarded denominator again: `n / 0` is Infinity and an SVG handed one draws a full column.
    if (m <= 0 || !Number.isFinite(value) || value <= 0) return 0;
    return Math.max(0, Math.min(1, value / m)) * plotHeight();
  };

  /**
   * Tick positions in *boundary* space, which has one more member than bucket space: N buckets
   * have N+1 edges, and the last tick is the top of the last bucket.
   */
  const ticks = (): readonly number[] => {
    const every = Math.max(1, props.tickEvery ?? 3);
    const out: number[] = [];
    for (let i = 0; i <= count(); i += every) out.push(i);
    if (out[out.length - 1] !== count() && count() > 0) out.push(count());
    return out;
  };

  const tickLabel = (edge: number): string => {
    const bucket = props.buckets[edge];
    if (bucket !== undefined) return boundary(bucket.from);
    const last = props.buckets[count() - 1];
    if (last === undefined) return "";
    // The top edge of an open-ended tail is not a number anybody served, so it is not printed.
    return last.to === undefined ? "" : boundary(last.to);
  };

  const onKeyDown = createChartKeyDown(count, cursor, setCursor);

  const announcement = (): string => {
    const index = cursor();
    if (index === undefined) return "";
    const bucket = props.buckets[index];
    if (bucket === undefined) return "";
    return `${bucketLabel(bucket)}: ${Number.isFinite(bucket.count) ? amount(bucket.count) : ABSENT}`;
  };

  return (
    <div class="kui-histogram">
      <div class="kui-plot" ref={box.ref}>
        <div
          class="kui-plot__surface"
          style={{ height: `${height()}px` }}
          tabindex="0"
          role="img"
          aria-label={props.label}
          aria-describedby={tableId}
          onKeyDown={onKeyDown}
          onMouseLeave={() => setCursor(undefined)}
          onBlur={() => setCursor(undefined)}
        >
          <Show
            when={!empty()}
            fallback={
              <p class="kui-plot__empty" role="status">
                {props.emptyMessage ?? "No messages in this range."}
              </p>
            }
          >
            <svg class="kui-plot__svg" width={width()} height={plotHeight()} aria-hidden="true">
              <For each={props.buckets}>
                {(bucket, index) => {
                  const h = (): number => barHeight(bucket.count);
                  return (
                    <g>
                      {/* The hit target is the bucket's whole column, because a 4px bar is a
                          target nobody can land on with a mouse. */}
                      <rect
                        class={["kui-plot__group", { "kui-plot__group--active": cursor() === index() }]}
                        x={slotWidth() * index()}
                        y={0}
                        width={slotWidth()}
                        height={plotHeight()}
                        onMouseEnter={() => setCursor(index())}
                      />
                      {/* A bucket that counted nothing draws no bar. Not a one-pixel stub, which
                          would say "a few messages were this size" — the axis label is still
                          there, and an empty column under it is the honest picture. */}
                      <Show when={h() > 0}>
                        <path
                          class="kui-plot__bar"
                          d={topRoundedRect(barX(index()), plotHeight() - h(), barWidth(), h(), BAR_RADIUS)}
                          fill={toneColor(toneOf(bucket, index()))}
                        />
                      </Show>
                    </g>
                  );
                }}
              </For>
            </svg>
          </Show>

          {/* Ticks sit on the *edges* between columns, not under their centres: a boundary label
              centred on a bar would read as that bar's name rather than as where it starts. */}
          <div class="kui-plot__axis" aria-hidden="true">
            <For each={ticks()}>
              {edge => (
                <span
                  class="kui-plot__tick"
                  style={{ left: `${count() === 0 ? 0 : (edge / count()) * 100}%` }}
                >
                  {tickLabel(edge)}
                </span>
              )}
            </For>
          </div>

          <Show when={cursor() !== undefined && !empty()}>
            <div
              class="kui-plot__tooltip"
              aria-hidden="true"
              style={{ left: `${((slotWidth() * ((cursor() ?? 0) + 0.5)) / Math.max(1, width())) * 100}%` }}
            >
              <p class="kui-plot__tooltip-title">{bucketLabel(props.buckets[cursor() ?? 0] as HistogramBucket)}</p>
              <p class="kui-plot__tooltip-row">
                <span class="kui-plot__tooltip-label">messages</span>
                <span class="kui-plot__tooltip-value">
                  {amount((props.buckets[cursor() ?? 0] as HistogramBucket).count)}
                </span>
              </p>
            </div>
          </Show>
        </div>

        <p class="kui-visually-hidden" role="status">
          {announcement()}
        </p>
        <ChartDataTable
          id={tableId}
          caption={props.label}
          categories={props.buckets.map(bucketLabel)}
          series={[{ label: "messages", tone: "neutral", points: props.buckets.map(b => b.count) }]}
          format={amount}
        />
      </div>

      <Show when={props.readouts && props.readouts.length > 0}>
        {/* Percentiles are the reading a histogram is usually consulted for, and they are text —
            so they are printed rather than pointed at, and everybody gets them. */}
        <ul class="kui-histogram__readouts">
          <For each={props.readouts}>
            {readout => (
              <li class="kui-histogram__readout" data-tone={readout.tone ?? "neutral"}>
                <span class="kui-histogram__readout-label">{readout.label}</span>
                <span class="kui-histogram__readout-value">{readout.value}</span>
              </li>
            )}
          </For>
        </ul>
      </Show>
    </div>
  );
};
