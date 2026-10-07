/**
 * How a feature is registered and how its chunk is fetched — the two halves of ADR-012.
 *
 * The static half ({@link FeatureRegistration}) is data the shell links against normally. The dynamic
 * half is a bare `import()` behind {@link createLazyModule}, whose whole job is that the wait for it
 * is bounded and its failure is recoverable.
 */
export {
  featureModule,
  type FeatureComponent,
  type FeatureId,
  type FeatureModule,
  type FeatureRegistration,
  type ServiceId,
} from "./registration.js";

export {
  type LazyModule,
  type LazyModuleOptions,
  type LoadState,
  DefaultLoadTimeoutMs,
  createLazyModule,
  timedOutMessage,
} from "./lazyFeature.js";

/**
 * The seam through which a feature reaches the product: the API client, the selected cluster, the
 * permission answer and the links. A feature's root takes no props, so without this there is no way
 * for one to fetch anything — which is why every feature in this workspace rendered fixtures until
 * it existed.
 */
export {
  KuiProvider,
  useKui,
  type CallScope,
  type KuiContextValue,
  type KnownAction,
  type KuiPaths,
  type MessageBrowserDefaults,
  type MessageViewMode,
} from "./context.jsx";
