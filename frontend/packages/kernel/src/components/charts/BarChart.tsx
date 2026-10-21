/**
 * The grouped column chart of SPEC §4.21 — throughput, two series per time bucket.
 *
 * ## Geometry, measured rather than invented
 *
 * From `01-dashboard.png`, scanning the row at y=470: bars are 10–11px wide, the two bars of a
 * pair are 5–6px apart and the groups are about 10px apart, drawn from a shared baseline with no
 * gridlines and no y-axis. The tops are rounded and the feet are square, which is why the bars
 * are paths and not `<rect rx>` (see `topRoundedRect`).
 *
 * The plot is drawn in real pixels from an observed container size rather than in a fixed
 * `viewBox` scaled to fit, because a scaled viewBox scales the *stroke and the corner radius too*
 * — the same chart in a narrow card would draw fatter corners than in a wide one.
 *
 * ## Keyboard
 *
 * A chart that only answers a mouse is unreadable to a keyboard user, so the plot is one tab stop
 * and `←`/`→` move a highlighted bucket. The highlighted bucket's values are announced through a
 * live region, and the whole series is also available as a hidden table (`ChartDataTable`) for a
 * reader who wants to go through it at their own pace rather than one arrow press at a time.
 */
import { For, Show, createSignal, createUniqueId, type Component } from "solid-js";
import { ChartDataTable } from "./ChartDataTable.jsx";
import { useElementSize } from "./elementSize.js";
import { ABSENT } from "./format.js";
import { createChartKeyDown } from "./keyboard.js";
import { defaultTicks, isPlotEmpty, seriesMax, topRoundedRect, type PlotProps } from "./plot.js";
import { toneColor } from "./tone.js";

export interface BarChartProps extends PlotProps {
  readonly height?: number;
  /** Names the chart for a screen reader. */
  readonly label: string;
  /** Shown, centred, when the range holds no data — with the axis still drawn beneath it. */
  readonly emptyMessage?: string;
}

const BAR_RADIUS = 3;
const AXIS_HEIGHT = 18;

export const BarChart: Component<BarChartProps> = props => {
  const box = useElementSize({ width: 640, height: 180 });
  const [cursor, setCursor] = createSignal<number | undefined>(undefined);
  const tableId = createUniqueId();

  const height = (): number => props.height ?? 180;
  const plotHeight = (): number => Math.max(20, height() - AXIS_HEIGHT);
  const width = (): number => box.size().width;
  const count = (): number => props.categories.length;
  const max = (): number => seriesMax(props.series);
  const empty = (): boolean => isPlotEmpty(props.series) || count() === 0;
  const format = (value: number): string => props.format?.(value) ?? String(value);
  const ticks = (): readonly number[] => props.ticks ?? defaultTicks(count());

  /** Where a group starts and how wide it is, in pixels. */
  const groupWidth = (): number => (count() === 0 ? 0 : width() / count());
  /**
   * Bars and their gaps are both derived from the width one bar's *slot* gets, so that the group
   * can never be wider than the space it has. Deriving them independently is how a chart with 180
   * buckets ends up drawing bars that overlap their neighbours: at that density the design's 11px
   * bar and 5px gap add up to more than the 2.5px a bucket actually owns, and the picture becomes
   * a moiré pattern rather than a histogram.
   *
   * 11px is the design's bar width, measured off `01-dashboard.png`; it is a ceiling, not a
   * target, so a chart with four buckets draws four bars rather than four slabs.
   */
  const slotWidth = (): number => groupWidth() / Math.max(1, props.series.length);
  const barWidth = (): number => Math.max(1, Math.min(11, slotWidth() * 0.7));
  const barGap = (): number => Math.min(5, slotWidth() * 0.2);

  const groupCentre = (index: number): number => groupWidth() * (index + 0.5);

  const barX = (groupIndex: number, seriesIndex: number): number => {
    const total = props.series.length * barWidth() + (props.series.length - 1) * barGap();
    return groupCentre(groupIndex) - total / 2 + seriesIndex * (barWidth() + barGap());
  };

  const barHeight = (value: number | null | undefined): number => {
    if (value === null || value === undefined || !Number.isFinite(value)) return 0;
    const m = max();
    // Guarded denominator, same rule as the magnitude bar: a max of zero must not become a full
    // column, and `value / 0` is Infinity, which the browser silently clamps.
    if (m <= 0) return 0;
    return Math.max(0, Math.min(1, value / m)) * plotHeight();
  };

  const onKeyDown = createChartKeyDown(count, cursor, setCursor);

  const announcement = (): string => {
    const index = cursor();
    if (index === undefined) return "";
    const category = props.categories[index] ?? "";
    const parts = props.series.map(s => {
      const point = s.points[index];
      return `${s.label} ${point === null || point === undefined ? ABSENT : format(point)}`;
    });
    return `${category}: ${parts.join(", ")}`;
  };

  return (
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
              {props.emptyMessage ?? "No data in this range."}
            </p>
          }
        >
          <svg class="kui-plot__svg" width={width()} height={plotHeight()} aria-hidden="true">
            <For each={props.categories}>
              {(_category, groupIndex) => (
                <g>
                  {/* The hover/focus target is the whole column of the group, not the bars: a
                      2px-wide bar is a hit target nobody can land on with a mouse. */}
                  <rect
                    class={["kui-plot__group", { "kui-plot__group--active": cursor() === groupIndex() }]}
                    x={groupWidth() * groupIndex()}
                    y={0}
                    width={groupWidth()}
                    height={plotHeight()}
                    onMouseEnter={() => setCursor(groupIndex())}
                  />
                  <For each={props.series}>
                    {(s, seriesIndex) => {
                      const value = (): number | null | undefined => s.points[groupIndex()];
                      const h = (): number => barHeight(value());
                      return (
                        <Show when={h() > 0}>
                          <path
                            class="kui-plot__bar"
                            d={topRoundedRect(
                              barX(groupIndex(), seriesIndex()),
                              plotHeight() - h(),
                              barWidth(),
                              h(),
                              BAR_RADIUS
                            )}
                            fill={toneColor(s.tone)}
                          />
                        </Show>
                      );
                    }}
                  </For>
                </g>
              )}
            </For>
          </svg>
        </Show>

        {/* Axis ticks are HTML, not `<text>`: they are ordinary 11px type in a token colour, and
            SVG text would need the font stack restated and would not inherit the theme. */}
        <div class="kui-plot__axis" aria-hidden="true">
          <For each={ticks()}>
            {tickIndex => (
              <span
                class="kui-plot__tick"
                style={{ left: `${count() === 0 ? 0 : ((tickIndex + 0.5) / count()) * 100}%` }}
              >
                {props.categories[tickIndex] ?? ""}
              </span>
            )}
          </For>
        </div>

        <Show when={cursor() !== undefined && !empty()}>
          <div
            class="kui-plot__tooltip"
            aria-hidden="true"
            style={{ left: `${(groupCentre(cursor() ?? 0) / Math.max(1, width())) * 100}%` }}
          >
            <p class="kui-plot__tooltip-title">{props.categories[cursor() ?? 0]}</p>
            <For each={props.series}>
              {s => {
                const point = (): number | null | undefined => s.points[cursor() ?? 0];
                return (
                  <p class="kui-plot__tooltip-row">
                    <span class="kui-plot__tooltip-swatch" style={{ background: toneColor(s.tone) }} />
                    <span class="kui-plot__tooltip-label">{s.label}</span>
                    <span class="kui-plot__tooltip-value">
                      {point() === null || point() === undefined ? ABSENT : format(point() as number)}
                    </span>
                  </p>
                );
              }}
            </For>
          </div>
        </Show>
      </div>

      <p class="kui-visually-hidden" role="status">
        {announcement()}
      </p>
      <ChartDataTable
        id={tableId}
        caption={props.label}
        categories={props.categories}
        series={props.series}
        format={props.format ?? ((v: number) => String(v))}
      />
    </div>
  );
};
