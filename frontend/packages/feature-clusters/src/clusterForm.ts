/**
 * The form behind "add a cluster", "change one" and "remove one", and the rules it enforces.
 *
 * ## What this form deliberately cannot do
 *
 * It cannot supply a truststore or a keystore. Those are configuration-file or API only, and that
 * is a real gap named here rather than papered over: a textarea for a base64 keystore is not a
 * usable way to give KUI a certificate, and the file upload that would be is a piece of work in
 * its own right. `SSL` and `SASL_SSL` against a cluster whose certificate the JVM's default trust
 * store already accepts — which is every managed service — work from this form today. A record
 * that already has a store must not be saved from here: a `PUT` replaces the profile, and this
 * form would send neither store. Client properties and, unless the row carried it, hostname
 * verification are the same kind of gap — the screen says so instead of pretending the defaults
 * are what is stored.
 *
 * ## Every reason, not the first one
 *
 * Somebody who got three fields wrong should be told about all three rather than discovering them
 * one save at a time. The server accumulates for the same reason (ADR-013) and refuses the same
 * things; these checks exist to answer *before* a round trip and are deliberately a subset — the
 * form never claims something is valid that the server then rejects. The id is not a gap in that
 * subset: it has to be the slug of the name, because the connection test derives the id from the
 * name and would otherwise report a cluster reachable that Save then refuses.
 */

export const PLAINTEXT = "PLAINTEXT";
export const SSL = "SSL";
export const SASL_PLAINTEXT = "SASL_PLAINTEXT";
export const SASL_SSL = "SASL_SSL";

export const PROTOCOLS: readonly { readonly value: string; readonly label: string }[] = [
  { value: PLAINTEXT, label: "PLAINTEXT — no encryption, no authentication" },
  { value: SSL, label: "SSL — encrypted, no authentication" },
  { value: SASL_PLAINTEXT, label: "SASL_PLAINTEXT — authenticated, not encrypted" },
  { value: SASL_SSL, label: "SASL_SSL — authenticated and encrypted" },
];

/**
 * The three mechanisms KUI is integration-tested against a real broker with.
 *
 * The vendor mechanisms — `AWS_MSK_IAM`, `OAUTHBEARER` and the rest — are configuration-file only,
 * and the write endpoint refuses them by name rather than accepting a value it cannot exercise.
 * Offering them here would produce a form whose Save is always refused.
 */
export const MECHANISMS: readonly { readonly value: string; readonly label: string }[] = [
  { value: "PLAIN", label: "PLAIN" },
  { value: "SCRAM-SHA-256", label: "SCRAM-SHA-256" },
  { value: "SCRAM-SHA-512", label: "SCRAM-SHA-512" },
];

export interface ClusterForm {
  /**
   * The slug this cluster is addressed by: it is in every URL and in every audit line, and it cannot
   * be changed afterwards without the links people have kept stopping working. Collected only when
   * adding — an edit addresses an existing record and must not move it. It is the slug of the name;
   * the server refuses any other value.
   */
  readonly id: string;
  readonly name: string;
  readonly bootstrapServers: string;
  readonly readOnly: boolean;
  readonly protocol: string;
  readonly mechanism: string;
  readonly username: string;
  readonly password: string;
  readonly verifyHostname: boolean;
  readonly timeoutMs: string;
  readonly batchSize: string;
  readonly parallelism: string;
}

/**
 * An empty form, with the defaults a new cluster starts from.
 *
 * The three admin numbers are the domain's own defaults spelled out rather than left blank, because
 * a blank timeout is not a smaller decision than a wrong one — it is the same decision, made by
 * whoever wrote the fallback, invisibly.
 */
export const EMPTY_CLUSTER_FORM: ClusterForm = {
  id: "",
  name: "",
  bootstrapServers: "",
  readOnly: false,
  protocol: PLAINTEXT,
  mechanism: "SCRAM-SHA-512",
  username: "",
  password: "",
  verifyHostname: true,
  timeoutMs: "30000",
  batchSize: "200",
  parallelism: "8",
};

/** Whether this protocol needs a mechanism, a username and a password. */
export function isSasl(form: ClusterForm): boolean {
  return form.protocol === SASL_PLAINTEXT || form.protocol === SASL_SSL;
}

/**
 * Whether hostname verification is a question at all.
 *
 * It is a TLS setting: on a plaintext connection there is no certificate to verify a hostname
 * against, and showing the control would suggest otherwise.
 */
export function isTls(form: ClusterForm): boolean {
  return form.protocol === SSL || form.protocol === SASL_SSL;
}

/** The server's own rule for a cluster id: `^[a-z0-9]([a-z0-9-]{0,62}[a-z0-9])?$`. */
const ID_PATTERN = /^[a-z0-9]([a-z0-9-]{0,62}[a-z0-9])?$/;

/**
 * The id the server derives from a name (`ClusterConfig.slug`).
 *
 * Not a suggestion. The write endpoint refuses a body whose name does not slug to the id in the
 * path: renaming a cluster creates a new one and removes the old one. A shorter id used to be
 * offered here, and the connection test — which derives the id from the name — would call that
 * cluster reachable, then Save would fail. The add form shows this value and does not let it be
 * typed over.
 */
export function suggestId(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 64)
    .replace(/-+$/, "");
}

/** A whole number, or `undefined`. `Number("")` is 0 and `Number("8x")` is NaN; neither is a count. */
function whole(text: string): number | undefined {
  const trimmed = text.trim();
  if (trimmed === "") return undefined;
  const value = Number(trimmed);
  return Number.isInteger(value) ? value : undefined;
}

/** The write request this form describes, or every reason it is not one yet. */
export function toRequest(
  form: ClusterForm,
):
  | { readonly ok: true; readonly request: Record<string, unknown> }
  | { readonly ok: false; readonly problems: readonly string[] } {
  const problems: string[] = [];

  const name = form.name.trim();
  const id = form.id.trim();

  if (name === "") problems.push("A name is required.");
  if (id === "") {
    problems.push("An id is required.");
  } else if (!ID_PATTERN.test(id)) {
    // The server's own rule, quoted. It is in every URL this cluster appears in, so the constraint
    // is not arbitrary tidiness — a slug with a slash or a space in it is a path that does not route.
    problems.push(
      "An id is lowercase letters, digits and hyphens, starting and ending with a letter or digit.",
    );
  } else if (name !== "" && suggestId(name) !== id) {
    /*
     * The same sentence the server returns. A well-formed shorter id ("staging" for "Staging EU
     * West") used to pass this form, the connection test derived the id from the name and reported
     * the cluster reachable, and Save was then refused. An edit that changes the slug is the same
     * refusal: the path id stays, and the new name identifies a different cluster.
     */
    const slugged = suggestId(name);
    problems.push(
      slugged === ""
        ? `'${name}' contains no letters or digits that can be made into a URL slug.`
        : `'${name}' identifies the cluster '${slugged}', not '${id}'; renaming a cluster creates a new one and removes the old one.`,
    );
  }
  if (form.bootstrapServers.trim() === "")
    problems.push("At least one broker address is required.");

  if (isSasl(form)) {
    if (form.mechanism.trim() === "") problems.push("A SASL connection needs a mechanism.");
    if (form.username.trim() === "") problems.push("A SASL connection needs a username.");
    /*
     * The one field this cannot check on an edit. KUI never sends a stored credential back to the
     * browser, so an edit form starts with the box empty — and an empty box means "leave the
     * password alone", which the write contract has no value for: a `PUT` replaces the record. See
     * `PASSWORD_WARNING` for what the screen says about it instead.
     */
    if (form.password === "") problems.push("A SASL connection needs a password.");
  }

  const timeout = whole(form.timeoutMs);
  const batch = whole(form.batchSize);
  const threads = whole(form.parallelism);

  if (timeout === undefined)
    problems.push("The admin timeout must be a whole number of milliseconds.");
  if (batch === undefined) problems.push("The batch size must be a whole number.");
  if (threads === undefined) problems.push("The parallelism must be a whole number.");

  if (
    problems.length > 0 ||
    timeout === undefined ||
    batch === undefined ||
    threads === undefined
  ) {
    return { ok: false, problems };
  }

  return {
    ok: true,
    request: {
      name,
      readOnly: form.readOnly,
      bootstrapServers: form.bootstrapServers.trim(),
      security: {
        protocol: form.protocol,
        // Omitted rather than sent empty on a non-SASL connection: a mechanism on a PLAINTEXT
        // cluster is a setting that cannot apply, and the server refuses it.
        ...(isSasl(form)
          ? {
              mechanism: form.mechanism.trim(),
              username: form.username.trim(),
              password: form.password,
            }
          : {}),
        verifyHostname: form.verifyHostname,
      },
      properties: {},
      admin: { timeoutMs: timeout, batchSize: batch, parallelism: threads },
    },
  };
}

/**
 * What the screen says about the password box on an edit.
 *
 * The honest sentence, because the alternative — showing dots and pretending there is a value —
 * would let an operator who did not touch the field wipe the credential by saving.
 */
export const PASSWORD_WARNING =
  "KUI never sends a stored password back to the browser, so this box starts empty. Saving replaces " +
  "the whole cluster definition, so type the password again even if it has not changed.";

/** And what it says about the three admin numbers, for the same reason. */
export const TUNING_WARNING =
  "The admin timeouts are not on the read model, so these show the defaults rather than what is " +
  "stored. Saving writes whatever is in these boxes. Hostname verification is the same when the " +
  "row did not carry it: the checkbox starts at the default (on).";

/**
 * Shown beside the hostname checkbox when the row did not say what is stored.
 *
 * The public read model has no `verifyHostname`. Leaving the box pre-ticked with no sentence made
 * an edit turn verification back on for brokers addressed by IP.
 */
export const HOSTNAME_WARNING =
  "Hostname verification is not on the read model, so this box starts at the default (on) rather " +
  "than what is stored. Saving writes whatever is ticked.";

/**
 * Shown on every edit. The read model does not say whether client properties exist, and a `PUT`
 * replaces the map, so an empty object here removes any that were stored.
 */
export const PROPERTIES_WARNING =
  "Client properties are not on the read model. Saving writes an empty property map and removes " +
  "any that were stored.";

/**
 * Why Save is refused. The read model does say whether a truststore or a keystore is configured,
 * and this form cannot send either back.
 */
export const STORES_WARNING =
  "This cluster has a truststore or a client keystore configured. This form cannot supply either, " +
  "and saving would remove them, so Save is disabled.";

/** The form for an existing cluster: everything the read model carries, and defaults for the rest. */
export function formFor(row: {
  readonly id: string;
  readonly name: string;
  readonly bootstrapServers: string;
  readonly readOnly: boolean;
  readonly security?:
    | {
        readonly protocol?: string;
        readonly mechanism?: string | null;
        /** Present only when the read model carried the stored choice. Absent is not `false`. */
        readonly verifyHostname?: boolean;
      }
    | undefined;
}): ClusterForm {
  const verifyHostname = row.security?.verifyHostname;
  return {
    ...EMPTY_CLUSTER_FORM,
    id: row.id,
    name: row.name,
    bootstrapServers: row.bootstrapServers,
    readOnly: row.readOnly,
    protocol: row.security?.protocol ?? PLAINTEXT,
    mechanism: row.security?.mechanism ?? EMPTY_CLUSTER_FORM.mechanism,
    // `false` is a stored choice. Missing is the default, and the screen says the box is not the record.
    ...(typeof verifyHostname === "boolean" ? { verifyHostname } : {}),
  };
}
