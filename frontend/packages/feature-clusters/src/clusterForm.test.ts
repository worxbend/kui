import { describe, expect, it } from "vitest";
import {
  EMPTY_CLUSTER_FORM,
  PLAINTEXT,
  SASL_SSL,
  SSL,
  formFor,
  isSasl,
  isTls,
  suggestId,
  toRequest,
} from "./clusterForm.js";
import { isEditable, isRemovable, originOf } from "./data.js";

const valid = {
  ...EMPTY_CLUSTER_FORM,
  id: "staging-eu-west",
  name: "Staging (eu-west)",
  bootstrapServers: "broker-1:9092,broker-2:9092",
};

describe("the cluster form", () => {
  it("builds a request from a complete plaintext form", () => {
    const answer = toRequest(valid);
    expect(answer.ok).toBe(true);
    if (!answer.ok) return;
    expect(answer.request.name).toBe("Staging (eu-west)");
    expect(answer.request.bootstrapServers).toBe("broker-1:9092,broker-2:9092");
    // No mechanism, username or password on a PLAINTEXT cluster: those are settings that cannot
    // apply, and the server refuses them.
    expect(answer.request.security).toEqual({ protocol: PLAINTEXT, verifyHostname: true });
  });

  it("reports every problem at once, not the first", () => {
    /*
     * Somebody who got three fields wrong should be told about all three rather than discovering
     * them one save at a time. The server accumulates for the same reason (ADR-013).
     */
    const answer = toRequest({ ...EMPTY_CLUSTER_FORM, timeoutMs: "soon" });
    expect(answer.ok).toBe(false);
    if (answer.ok) return;
    expect(answer.problems.length).toBeGreaterThanOrEqual(4);
    expect(answer.problems.some((p) => /name is required/i.test(p))).toBe(true);
    expect(answer.problems.some((p) => /broker address/i.test(p))).toBe(true);
    expect(answer.problems.some((p) => /id is required/i.test(p))).toBe(true);
    expect(answer.problems.some((p) => /whole number of milliseconds/i.test(p))).toBe(true);
  });

  it("holds the id to the server's own rule", () => {
    // The id is in every URL this cluster appears in, so this is not tidiness: a slug with a slash
    // or a space in it is a path that does not route.
    const bad = toRequest({ ...valid, id: "Staging EU West" });
    expect(bad.ok).toBe(false);
    if (bad.ok) return;
    expect(bad.problems.some((p) => /lowercase letters, digits and hyphens/i.test(p))).toBe(true);
  });

  it("refuses an id the name does not identify", () => {
    /*
     * The add form used to invite a shorter id than the name. The server refuses that: the id has
     * to be the slug of the name, and the connection test never checks because it derives the id
     * from the name itself. "Staging EU West" with id `staging` must fail here, before either call.
     */
    const answer = toRequest({
      ...EMPTY_CLUSTER_FORM,
      name: "Staging EU West",
      id: "staging",
      bootstrapServers: "b:9092",
    });
    expect(answer.ok).toBe(false);
    if (answer.ok) return;
    expect(answer.problems.some((p) => /identifies the cluster/i.test(p))).toBe(true);
    expect(answer.problems.some((p) => p.includes("staging-eu-west") && p.includes("staging"))).toBe(
      true,
    );
  });

  it("refuses a rename whose slug is a different id", () => {
    // An edit keeps the record's id and still sends the name. "Orders Prod" renamed to
    // "Orders Production" passes the id pattern and is a different cluster to the server.
    const answer = toRequest({
      ...EMPTY_CLUSTER_FORM,
      name: "Orders Production",
      id: "orders-prod",
      bootstrapServers: "b:9092",
    });
    expect(answer.ok).toBe(false);
    if (answer.ok) return;
    expect(
      answer.problems.some((p) => p.includes("orders-production") && p.includes("orders-prod")),
    ).toBe(true);
  });

  it("asks for credentials only on a SASL connection", () => {
    expect(isSasl({ ...valid, protocol: PLAINTEXT })).toBe(false);
    expect(isSasl({ ...valid, protocol: SASL_SSL })).toBe(true);

    const sasl = toRequest({ ...valid, protocol: SASL_SSL });
    expect(sasl.ok).toBe(false);
    if (sasl.ok) return;
    expect(sasl.problems.some((p) => /needs a username/i.test(p))).toBe(true);
    expect(sasl.problems.some((p) => /needs a password/i.test(p))).toBe(true);

    const complete = toRequest({
      ...valid,
      protocol: SASL_SSL,
      username: "kui",
      password: "hunter2",
    });
    expect(complete.ok).toBe(true);
    if (!complete.ok) return;
    expect(complete.request.security).toMatchObject({ username: "kui", password: "hunter2" });
  });

  it("asks about hostname verification only where there is a certificate", () => {
    // It is a TLS setting. On a plaintext connection there is no certificate to verify a hostname
    // against, and showing the control would suggest otherwise.
    expect(isTls({ ...valid, protocol: PLAINTEXT })).toBe(false);
    expect(isTls({ ...valid, protocol: SSL })).toBe(true);
    expect(isTls({ ...valid, protocol: SASL_SSL })).toBe(true);
  });

  it("derives an id from a name the way the server slugs it", () => {
    expect(suggestId("Staging (eu-west)")).toBe("staging-eu-west");
    expect(suggestId("  Production!  ")).toBe("production");
    // Never a trailing hyphen, which the server's pattern refuses.
    expect(suggestId("orders --")).toBe("orders");
  });

  it("keeps a stored decision not to verify the broker hostname", () => {
    /*
     * formFor used to spread EMPTY_CLUSTER_FORM, so verifyHostname was always true. An SSL cluster
     * saved with the box unticked came back ticked, and the next save turned verification on.
     */
    const form = formFor({
      id: "orders-prod",
      name: "Orders Prod",
      bootstrapServers: "b:9092",
      readOnly: false,
      security: { protocol: SSL, mechanism: null, verifyHostname: false },
    });
    expect(form.verifyHostname).toBe(false);
  });

  it("does not turn hostname verification off when the read model omitted it", () => {
    // Absent is not a stored `false`. Treating it as one would disable verification on every
    // cluster the public read model describes, which is all of them today.
    const form = formFor({
      id: "orders-prod",
      name: "Orders Prod",
      bootstrapServers: "b:9092",
      readOnly: false,
      security: { protocol: SSL, mechanism: null },
    });
    expect(form.verifyHostname).toBe(true);
  });

  it("defaults the admin numbers rather than leaving them blank", () => {
    // A blank timeout is not a smaller decision than a wrong one — it is the same decision, made by
    // whoever wrote the fallback, invisibly.
    expect(EMPTY_CLUSTER_FORM.timeoutMs).not.toBe("");
    expect(EMPTY_CLUSTER_FORM.batchSize).not.toBe("");
    expect(EMPTY_CLUSTER_FORM.parallelism).not.toBe("");
  });
});

describe("what may be done to a cluster, by where it is defined", () => {
  it("only a stored cluster can be removed", () => {
    /*
     * Deleting the store record for a cluster the configuration file also names would leave the row
     * on screen, which reads as a delete that silently failed. The server refuses it; the screen
     * says so before the click rather than after.
     */
    expect(isRemovable("stored")).toBe(true);
    expect(isRemovable("static")).toBe(false);
    expect(isRemovable("staticThenStored")).toBe(false);
  });

  it("a cluster whose stored record already wins can still be edited", () => {
    // `staticThenStored` means the store's version is what is actually in force, so replacing it
    // changes what is in force. Withholding the edit would leave no way to change it at all.
    expect(isEditable("staticThenStored")).toBe(true);
    expect(isEditable("stored")).toBe(true);
    expect(isEditable("static")).toBe(false);
  });

  it("an origin the browser does not recognise is treated as not editable", () => {
    /*
     * The safe direction. Offering an edit for a cluster defined in the deployment's file would make
     * the file a lie — the next deployment would change it back — whereas withholding one for a
     * cluster that is in fact editable is an inconvenience, and the server refuses the write anyway.
     */
    expect(originOf("something-new")).toBe("unknown");
    expect(isEditable("unknown")).toBe(false);
    expect(isRemovable("unknown")).toBe(false);
  });

  it("reads the origins the server actually sends, in any case", () => {
    expect(originOf("static")).toBe("static");
    expect(originOf("stored")).toBe("stored");
    expect(originOf("staticThenStored")).toBe("staticThenStored");
    expect(originOf("STORED")).toBe("stored");
    expect(originOf(null)).toBe("unknown");
  });
});
