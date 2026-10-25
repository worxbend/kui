import { createSignal } from "solid-js";

import { userMessage, type ApiResult, type KuiApiClient } from "@kui/api";

import {
  SEARCH_DEBOUNCE_MS,
  SEARCH_LIMIT,
  fetchSearch,
  type SearchAnswer,
  type SearchState,
} from "./search.js";

export interface SearchController {
  /** What is in the box, as typed. */
  readonly text: () => string;
  /** What the last search answered, or that one is under way. */
  readonly state: () => SearchState;
  readonly input: (next: string) => void;
  /** Asks again for what is in the box now; an empty box asks nothing. */
  readonly retry: () => void;
  readonly dispose: () => void;
}

export interface SearchControllerOptions {
  readonly api: KuiApiClient;
  /**
   * Every answer that is still current, before it is drawn. The caller decides what an answer is
   * evidence of; this module only decides whether it is still wanted.
   */
  readonly answered: (answer: ApiResult<SearchAnswer>) => void;
}

/**
 * What is in the search box, and what the last search answered.
 *
 * Two signals rather than one, because the text and the answer are out of step for most of the
 * time the box is in use: a debounce is precisely the interval during which the field shows what
 * was typed and the overlay shows the previous answer, and collapsing them would either delay the
 * characters appearing or throw the results away on every keystroke.
 */
export function createSearchController(options: SearchControllerOptions): SearchController {
  const [text, setText] = createSignal("");
  const [state, setState] = createSignal<SearchState>({ kind: "idle" });

  let timer: ReturnType<typeof setTimeout> | undefined;
  let request: AbortController | undefined;
  /**
   * Which search is current.
   *
   * `GET /api/v1/search` is a fold over three services, so two searches in flight can finish in
   * either order — and the loser landing last would put the results for `ord` under a box that says
   * `orders`. Every answer carries the number of the search that asked for it and is dropped when
   * that number has moved on, which is the same rule the overview's fetch effect applies to a
   * cluster switch and for the same reason: a wrong answer that looks right is the worst kind.
   */
  let episode = 0;

  const invalidate = (): number => {
    const current = (episode += 1);
    request?.abort();
    request = undefined;
    return current;
  };

  const run = (query: string, asked: number): void => {
    const controller = new AbortController();
    request = controller;
    setState({ kind: "searching" });
    void fetchSearch(options.api, query, SEARCH_LIMIT, controller.signal).then((answer) => {
      if (request === controller) request = undefined;
      if (asked !== episode) return;
      options.answered(answer);
      setState(
        answer.ok
          ? { kind: "ready", answer: answer.value }
          : { kind: "failed", reason: userMessage(answer.error) },
      );
    });
  };

  const input = (next: string): void => {
    setText(next);
    if (timer !== undefined) clearTimeout(timer);
    timer = undefined;

    /* Invalidate at the keystroke, not when its debounce expires. Otherwise an older request can
       land during this 200 ms window and replace "Searching" with rows for text the field no
       longer contains. Cancelling also stops a gateway fan-out whose answer cannot be used. */
    const asked = invalidate();

    const query = next.trim();
    if (query.length === 0) {
      // Emptying the box is not a search for nothing; it is the end of searching. The episode is
      // stepped so that an answer already in flight cannot reopen the overlay behind the caret.
      setState({ kind: "idle" });
      return;
    }

    /* `searching` immediately, and the request after the debounce. Waiting to say anything until
       the request goes out leaves the overlay showing the *previous* query's results for a fifth of
       a second under new text, which reads as a search that answered wrongly. */
    setState({ kind: "searching" });
    timer = setTimeout(() => {
      timer = undefined;
      run(query, asked);
    }, SEARCH_DEBOUNCE_MS);
  };

  const retry = (): void => {
    const query = text().trim();
    if (query.length > 0) run(query, invalidate());
  };

  const dispose = (): void => {
    if (timer !== undefined) clearTimeout(timer);
    episode += 1;
    request?.abort();
  };

  return { text, state, input, retry, dispose };
}
