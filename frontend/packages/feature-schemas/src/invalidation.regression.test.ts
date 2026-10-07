import { expect, it, vi } from "vitest";
import { sharedQueries } from "@kui/kernel";
import type { KuiApiClient } from "@kui/api";
import { registerSchema, setCompatibility } from "./data.js";

it("76: registration invalidates all subject lists, versions and latest, not unrelated subjects/clusters", async () => {
  const invalidate = vi.spyOn(sharedQueries, "invalidateWhere");
  const api = { post: async () => ({ ok: true, value: { version: 2, id: 42 } }) } as unknown as KuiApiClient;
  try {
    await registerSchema(api, "prod", "orders", { schemaType: "AVRO", definition: '"string"' });
    expect(invalidate).toHaveBeenCalled();
    const matches = (key: string) => invalidate.mock.calls.some(([predicate]) => predicate(key));
    for (const key of ["schemas:subjects:prod::asc:1:50", "schemas:subjects:prod:order:desc:2:50", "schemas:versions:prod:orders", "schemas:schema:prod:orders:latest", "schemas:compat:prod:orders"]) expect(matches(key), key).toBe(true);
    for (const key of ["schemas:schema:prod:others:latest", "schemas:subjects:production::asc:1:50", "schemas:schema:prod:orders:1"]) expect(matches(key), key).toBe(false);
  } finally { invalidate.mockRestore(); }
});

it("76: global compatibility invalidates inherited views and subject summaries", async () => {
  const invalidate = vi.spyOn(sharedQueries, "invalidateWhere");
  const api = { put: async () => ({ ok: true, value: {} }) } as unknown as KuiApiClient;
  try {
    await setCompatibility(api, "prod", "FULL");
    expect(invalidate).toHaveBeenCalled();
    const matches = (key: string) => invalidate.mock.calls.some(([predicate]) => predicate(key));
    for (const key of ["schemas:global:prod", "schemas:subjects:prod::asc:1:50", "schemas:compat:prod:orders", "schemas:compat:prod:other"]) expect(matches(key), key).toBe(true);
    expect(matches("schemas:compat:production:orders")).toBe(false);
  } finally { invalidate.mockRestore(); }
});

it("76: a refused mutation invalidates nothing", async () => {
  const invalidate = vi.spyOn(sharedQueries, "invalidateWhere");
  const refuse = async () => ({ ok: false, error: { kind: "unreachable", cause: "offline" } });
  const api = { post: refuse, put: refuse } as unknown as KuiApiClient;
  try {
    await registerSchema(api, "prod", "orders", { schemaType: "AVRO", definition: '"string"' });
    await setCompatibility(api, "prod", "FULL");
    expect(invalidate).not.toHaveBeenCalled();
  } finally { invalidate.mockRestore(); }
});
