// Run with: node --experimental-strip-types --test .storybook/docgen.test.mjs
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { test } from "node:test";
import { viteFinal as frameworkViteFinal } from "storybook-solidjs-vite/preset";
import { transformWithOxc } from "vite";
import config from "./main.ts";

async function metadataPlugin() {
  const options = {
    configDir: fileURLToPath(new URL(".", import.meta.url)),
    presets: { apply: async () => config.framework },
  };
  // Match Storybook loading the product's Solid plugin before the framework preset.
  const base = await frameworkViteFinal({ plugins: [{ name: "solid" }] }, options);
  const final = config.viteFinal ? await config.viteFinal(base, options) : base;
  const plugins = (await Promise.all(final.plugins)).flat(Infinity);
  return plugins.find((plugin) => plugin.name === "storybook:solid-component-meta");
}

async function transform(plugin, source, id) {
  const hook = typeof plugin.transform === "function" ? plugin.transform : plugin.transform.handler;
  return hook.call({}, source, id);
}

test("Storybook compiles named default components without losing generated prop metadata", async () => {
  const id = fileURLToPath(new URL("../packages/kernel/src/components/JsonTree.tsx", import.meta.url));
  const source = await readFile(id, "utf8");
  const output = await transform(await metadataPlugin(), source, id);
  assert.ok(output.code.startsWith(source), "component source must remain byte-for-byte intact");
  await transformWithOxc(output.code, id);
  const metadata = output.code.slice(source.length);
  assert.match(metadata, /JsonTree\.__docgenInfo = /);
  const info = JSON.parse(metadata.trim().replace(/^JsonTree\.__docgenInfo = /, "").replace(/;$/, ""));
  assert.deepEqual(Object.keys(info.props).sort(), ["ariaLabel", "class", "text"]);
  assert.equal(info.props.text.required, true);
  assert.equal(info.props.text.type.name, "string");
});

test("valid metadata and unhandled transform results pass through unchanged", async () => {
  for (const result of [null, "unchanged", { code: "different output" }, {
    code: 'export function Example() {}\nExample.__docgenInfo = {"props":{}};',
    map: { mappings: "" },
  }]) {
    const plugin = { name: "storybook:solid-component-meta", transform: async () => result };
    await config.viteFinal({ plugins: [plugin] });
    assert.equal(await transform(plugin, "export function Example() {}", "Example.tsx"), result);
  }
});

test("the correction preserves source strings, metadata bytes, context and source maps", async () => {
  const source = 'export default function $Example() { return "default.__docgenInfo = "; }';
  const suffix = '\n\ndefault.__docgenInfo = {"props":{"text":{"required":true}}};\n';
  const map = { mappings: "AAAA" };
  const context = {};
  const options = { ssr: false };
  const plugin = {
    name: "storybook:solid-component-meta",
    transform(code, id, receivedOptions) {
      assert.equal(this, context);
      assert.equal(id, "Example.tsx");
      assert.equal(receivedOptions, options);
      return { code: code + suffix, map };
    },
  };
  await config.viteFinal({ plugins: [plugin] });
  const output = await plugin.transform.call(context, source, "Example.tsx", options);
  assert.equal(output.code, source + suffix.replace("default.", "$Example."));
  assert.equal(output.map, map);
});

test("unsupported default bindings and upstream errors fail rather than dropping metadata", async () => {
  const plugin = {
    name: "storybook:solid-component-meta",
    transform: async (code) => ({ code: code + '\n\ndefault.__docgenInfo = {};\n' }),
  };
  await config.viteFinal({ plugins: [plugin] });
  await assert.rejects(transform(plugin, "export default () => null;", "Example.tsx"), /Cannot resolve the default component binding/);
  const failure = new Error("upstream failure");
  const broken = { name: "storybook:solid-component-meta", transform() { throw failure; } };
  await config.viteFinal({ plugins: [broken] });
  await assert.rejects(transform(broken, "", "Example.tsx"), (error) => error === failure);
});
