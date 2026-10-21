/**
 * The latency chart of SPEC §4.22 — 2px lines with a faint area beneath each.
 *
 * ## The rule that shapes the drawing code
 *
 * **A gap breaks the line.** A `null` point is not a zero and it is not a straight run between
 * its neighbours: interpolating across it invents a measurement that was never taken and hides
 * the outage that caused the hole. So the points are split into contiguous runs and each run is
 * drawn as its own polyline and its own area — which is why this file has a `runs()` function and
 * not a single `points` string.
 *
 * ## Everything else worth knowing
 *
 * - The area is 12% of the series colour, measured: the fill under the produce line samples
 *   #2E3642, and #A8C7FA over #1B1F25 at 12% computes to #2C333F.
 * - The last point of each series carries a dot, as the design draws, so the current value has a
 *   position as well as a number in the legend.
 * - The y domain starts at zero and is padded above the maximum. A latency chart whose axis
 *   starts at the minimum makes a 1ms wobble look like an incident; starting at zero keeps the
 *   shape honest, and since there is no y-axis to read, honesty is all the shape has.
 */
import { For, Show, createMemo, createSignal, createUniqueId, type Component } from "solid-js";
import { ChartDataTable } from "./ChartDataTable.jsx";
import { useElementSize } from "./elementSize.js";
import { ABSENT } from "./format.js";
import { createChartKeyDown } from "./keyboard.js";
import { defaultTicks, isPlotEmpty, seriesMax, type PlotProps, type Series } from "./plot.js";
import { toneAreaFill, toneColor } from "./tone.js";

export interface LineChartProps extends PlotProps {
  readonly height?: number;
  readonly label: string;
  readonly emptyMessage?: string;
  /** The top of the y domain. Defaults to 1.5× the largest value, so lines sit clear of the edge. */
  readonly yMax?: number;
}

const AXIS_HEIGHT = 18;

interface Point {
  readonly x: number;
  readonly y: number;
}

export const LineChart: Component<LineChartProps> = props => {
  const box = useElementSize({ width: 640, height: 140 });
  const [cursor, setCursor] = createSignal<number | undefined>(undefined);
  const tableId = createUniqueId();

  const height = (): number => props.height ?? 140;
  const plotHeight = (): number => Math.max(20, height() - AXIS_HEIGHT);
  const width = (): number => box.size().width;
  const count = (): number => props.categories.length;
  const empty = (): boolean => isPlotEmpty(props.series) || count() === 0;
  const format = (value: number): string => props.format?.(value) ?? String(value);
  const ticks = (): readonly number[] => props.ticks ?? defaultTicks(count());

  const yMax = (): number => {
    if (props.yMax !== undefined && props.yMax > 0) return props.yMax;
    const max = seriesMax(props.series);
    return max <= 0 ? 1 : max * 1.5;
  };

  const xAt = (index: number): number => (count() <= 1 ? width() / 2 : (index / (count() - 1)) * width());
  const yAt = (value: number): number => plotHeight() - Math.max(0, Math.min(1, value / yMax())) * plotHeight();

  /** Contiguous runs of real numbers. A `null` ends a run; the next number starts a new one. */
  const runs = (series: Series): Point[][] => {
    const out: Point[][] = [];
    let current: Point[] = [];
    series.points.forEach((point, index) => {
      if (point === null || point === undefined || !Number.isFinite(point)) {
        if (current.length > 0) out.push(current);
        current = [];
        return;
      }
      current.push({ x: xAt(index), y: yAt(point) });
    });
    if (current.length > 0) out.push(current);
    return out;
  };

  const linePath = (run: readonly Point[]): string =>
    run.map((p, i) => `${i === 0 ? "M" : "L"} ${p.x} ${p.y}`).join(" ");

  const areaPath = (run: readonly Point[]): string => {
    const first = run[0];
    const last = run[run.length - 1];
    if (!first || !last) return "";
    return `${linePath(run)} L ${last.x} ${plotHeight()} L ${first.x} ${plotHeight()} Z`;
  };

  const lastPoint = (seriesRuns: readonly Point[][]): Point | undefined => {
    const lastRun = seriesRuns[seriesRuns.length - 1];
    return lastRun?.[lastRun.length - 1];
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

  const nearest = (event: MouseEvent): void => {
    const target = event.currentTarget as HTMLElement;
    const rect = target.getBoundingClientRect();
    if (rect.width <= 0 || count() === 0) return;
    const ratio = (event.clientX - rect.left) / rect.width;
    setCursor(Math.max(0, Math.min(count() - 1, Math.round(ratio * (count() - 1)))));
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
        onMouseMove={nearest}
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
            <For each={props.series}>
              {series => {
                const seriesRuns = createMemo(() => runs(series));
                return (
                  <g>
                    <For each={seriesRuns()}>
                      {run => (
                        <>
                          {/* A single point has no line to draw, so it gets a dot instead of
                              vanishing — one measurement between two outages is still a fact. */}
                          <Show when={run.length > 1}>
                            <path d={areaPath(run)} fill={toneAreaFill(series.tone)} />
                            <path
                              class="kui-plot__line"
                              d={linePath(run)}
                              fill="none"
                              stroke={toneColor(series.tone)}
                              stroke-width="2"
                              stroke-linecap="round"
                              stroke-linejoin="round"
                            />
                          </Show>
                          <Show when={run.length === 1 && run[0]}>
                            {point => <circle cx={point().x} cy={point().y} r="2.5" fill={toneColor(series.tone)} />}
                          </Show>
                        </>
                      )}
                    </For>
                    <Show when={lastPoint(seriesRuns())}>
                      {point => <circle cx={point().x} cy={point().y} r="3.5" fill={toneColor(series.tone)} />}
                    </Show>
                  </g>
                );
              }}
            </For>

            <Show when={cursor() !== undefined}>
              <line
                class="kui-plot__guide"
                x1={xAt(cursor() ?? 0)}
                x2={xAt(cursor() ?? 0)}
                y1={0}
                y2={plotHeight()}
              />
            </Show>
          </svg>
        </Show>

        <div class="kui-plot__axis" aria-hidden="true">
          <For each={ticks()}>
            {tickIndex => (
              <span
                class="kui-plot__tick"
                style={{ left: `${count() <= 1 ? 50 : (tickIndex / (count() - 1)) * 100}%` }}
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
            style={{ left: `${(xAt(cursor() ?? 0) / Math.max(1, width())) * 100}%` }}
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
