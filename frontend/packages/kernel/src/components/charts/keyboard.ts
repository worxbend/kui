export function createChartKeyDown(
  count: () => number,
  cursor: () => number | undefined,
  setCursor: (value: number | undefined) => void,
): (event: KeyboardEvent) => void {
  const move = (delta: number): void => {
    if (count() === 0) return;
    const current = cursor();
    const next = current === undefined ? (delta > 0 ? 0 : count() - 1) : current + delta;
    setCursor(Math.max(0, Math.min(count() - 1, next)));
  };

  return (event: KeyboardEvent): void => {
    if (event.key === "ArrowRight") {
      event.preventDefault();
      move(1);
    } else if (event.key === "ArrowLeft") {
      event.preventDefault();
      move(-1);
    } else if (event.key === "Home") {
      event.preventDefault();
      setCursor(0);
    } else if (event.key === "End") {
      event.preventDefault();
      setCursor(count() - 1);
    } else if (event.key === "Escape") {
      setCursor(undefined);
    }
  };
}
