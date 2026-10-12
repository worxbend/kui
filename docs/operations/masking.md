# Masking fields so they never reach a screen

Some of what is in a Kafka topic should not be readable by everyone who can open a console: card
numbers, national insurance numbers, email addresses, a password somebody put in a header once.
KUI can hide named fields on the way out of the message service, so that a person browsing the topic
sees `***********************.com` where the record says `marta.zielinska@example.com`.

This page is about `kui.clusters.<n>.masking[]` — what it does, what it does not do, and the
mistakes it cannot catch for you. The key-by-key reference is in
[the worked configuration below](#what-it-is-in-one-file).

## What it is, in one file

```yaml
kui:
  clusters:
    - name: Production
      bootstrapServers: ["kafka:9092"]
      masking:
        - kind: mask
          fields: [cardNumber]
          keep:
            suffix: 4
          topicValuesPattern: "payments\\..*"
        - kind: remove
          fieldsNamePattern: ".*[Pp]assword.*"
```

Two rules. The first turns `4111111111111111` into `************1111` in the `cardNumber` field of
any topic whose name starts `payments.`; the second deletes any field whose name contains
`password`, on every topic, keys and values alike.

**There is a running example.** `deployment/quickstart/quickstart.sh` brings up a cluster whose
`customers.profiles` topic is masked on two fields, and browsing that topic is the fastest way to
see what a rule does to a record. The configuration is
[`deployment/quickstart/kui-quickstart.yaml`](../../deployment/quickstart/kui-quickstart.yaml), and
the comment above the `masking:` block says why each line is the way it is.

## The three kinds

| `kind` | What happens to a matched field | Reads |
| --- | --- | --- |
| `remove` | the field is deleted from the record — the key disappears from the object, the element from the array, the header from the list | — |
| `mask` | every character is replaced, cycling through `maskingCharsReplacement`, except the ends `keep` preserves | `maskingCharsReplacement`, `keep` |
| `replace` | the value becomes the literal you wrote, and its type becomes string | `replacement` |

Three things hold, and each is a decision rather than an accident. **Read the scope on the first
one**, because it is the one an operator is most likely to build a policy on:

* **A `mask` never returns a value longer than the value it replaced** — and `remove` returns no
  value at all. A mask that padded a four-digit field out to sixteen characters would announce that
  the field was not a card number, and somebody counting characters would learn what the mask was
  hiding. **This does not hold for `replace`**, and cannot: `replacement` is a literal you write, so
  `replacement: "<redacted by policy>"` over a four-character field returns twenty-two characters.
  `MaskingEngine` writes that literal verbatim with no length bound, deliberately: a `replace`
  whose text was truncated to fit the field would be unreadable exactly where it must be read.
  So if you are relying on the length of a masked field carrying no information, use `mask` or
  `remove`, or choose a `replacement` no longer than the shortest value it will cover. It is worth
  saying plainly: this page invited you to rely on the sentence unscoped until wave 11, and for
  `kind: replace` that invitation was wrong.
* **A masked number comes back as a string.** Masking a number and keeping it a number would either
  change its magnitude or fail to hide it.
* **A matched object or array is masked leaf by leaf**, not flattened into one long string.
  Replacing `{"a":"secret"}` with `*************` would publish how long the document was.

## Choosing what a rule applies to

**Which fields.** `fields` is a list of names; `fieldsNamePattern` is a regular expression matched
against the whole field name. They are mutually exclusive and writing both fails the load — the
engine would apply the list and never look at the pattern, and nothing in a running system would say
which of the two you got. Both match at any depth: a `cardNumber` nested three objects down is the
same field as one at the top.

A rule with **neither** applies to the whole value. That is the "this entire topic is sensitive"
case, and it is the reason neither key is required.

**Which topics, and which half of the record.** `topicKeysPattern` selects topics whose **keys** the
rule reaches; `topicValuesPattern` selects topics whose **values** it reaches. Both are matched
against the whole topic name, so `orders\..*` is `orders.v1` and not `legacy.orders.v1`. A rule with
neither applies to the keys and the values of every topic on that cluster.

Writing one and not the other is read as meaning it: a rule with only `topicKeysPattern` masks keys
and leaves every value alone. Reading the absent pattern as "everything" would mask far more than
you asked for, and masking too much is a silent, hard-to-notice kind of wrong.

**Headers follow the value scope.** A header belongs to the record rather than to its key or its
value, so value-scoped and unscoped rules reach headers and key-scoped ones do not. Header names are
matched the way field names are — a header called `authorization` is as sensitive as a field called
`authorization`, and you should not have to say so twice. A `remove` rule drops the header entirely.

**Order matters.** A JSON value gets *every* matching rule, in the order you wrote them, so "replace
this field" followed by "mask everything else" composes. A payload that is **not** JSON gets the
**first** matching rule only: there is no meaningful way to compose "remove" and "keep the last four
characters" over text with no structure.

## `keep`, and the bound on it

`keep.prefix` and `keep.suffix` are how many characters survive at each end — `keep: {suffix: 4}` is
"show the last four digits". It is the one knob that makes a mask reveal **more**, which is why it
is the one knob with a bound: each end may be at most 20, **and the two ends together may be at most
20**.

Two rules of arithmetic are worth knowing before you write one:

* **A `keep` that leaves nothing to mask masks everything.** `keep: {suffix: 4}` meeting a
  four-character value returns four asterisks, not the value. These numbers bound what a mask may
  *reveal*, so when the arithmetic runs out it fails towards hiding. A rule that quietly returned
  its input would be worse than one that masks more than its author intended, because the first
  looks like it is working.
* **The ends never overlap.** If `prefix` and `suffix` would both reach the same characters, the
  suffix yields.

### A configuration that used to load

Until wave 10 the bound was enforced **per end** and not on the sum, so this was accepted:

```yaml
        - kind: mask
          fields: [cardNumber]
          keep:
            prefix: 20
            suffix: 20
```

Twenty is a legal prefix and twenty is a legal suffix, and together they keep forty characters of a
sixteen-character card number — all of it. That file loaded, read correctly to whoever wrote it, and
returned `4111111111111111` untouched on every record.

**It is now refused at start-up**, naming the entry and the arithmetic. If your file has one, the
rule was not masking anything, so there is nothing to preserve: lower one of the two ends to
whatever you actually meant to show. The engine also fails safe on the same input now, so a rule
built in code rather than read from a file cannot reach the old behaviour either.

## Where masking happens, and where it deliberately does not

The mask is applied **once**, immediately after a record is decoded and before anything else in the
message service sees it. That placement is the whole feature: the record event, the string filter,
the smart filter and the paging cursor all read the same masked record, because there is only one.

| Path | Masked? |
| --- | --- |
| Browsing a topic's messages | **yes** |
| Tracking a record across topics | **yes** |
| The string predicate and the CEL filter a user types | **yes** — they read the masked text |
| Producing a record | **no**, and never |
| Resending records from one topic to another | **no** — nothing on that path is deserialized at all |

**Filters read the masked text, and that is the point.** A filter running on the original would
answer questions about the hidden value: `value contains "4111111111111111"` returning one row tells
the reader the card number without ever drawing it. The cost is that a masked field cannot be
searched on, and that is the honest consequence of hiding it.

**Masking is never applied on produce.** Masking a value on the way in would write the mask into the
topic and destroy the original — a user who could see a masked field and then produce the record
back would silently corrupt data.

**A resend copies bytes and never decodes them**, so a resend cannot show anybody a masked field.
What it *can* do is copy the original records into a destination topic — and if no rule covers that
destination, the same data is then readable there. A `topicValuesPattern` that covers the source and
not the dead-letter topic beside it is the common shape of this; the resend dialogue names the
destination, and it is worth reading.

**A decode error does not quote the payload of a masked half.** When a serde cannot read a record it
attaches a sentence saying why, and at least one of them quotes the bytes (*"this one starts with
`4`"*). On a half that a rule covers, that sentence is replaced with one saying the detail is
withheld; the serde and which half failed are still reported, because those are what you act on.

## What masking is not

**It is not access control.** A rule hides a field from **every** reader equally and knows nothing
about who is reading. There is no "show the card number to the payments team": that is a per-group
policy (DM-002) and it does not exist yet. Anything that must be visible to some people and not
others needs a different tool.

**It does not protect the data in Kafka.** The records in the topic are untouched. Anyone with a
Kafka client and the broker's credentials reads exactly what the producer wrote, and so does every
other consumer. Masking hides a field *in KUI*, which is worth doing precisely because a console is
the thing most people have access to — and is worth nothing if the broker is open.

**It is a property of the registered cluster, not of the broker.** Two entries under `kui.clusters`
pointing at the same `bootstrapServers` are two profiles over one Kafka, and a rule written on one
does nothing for the other. The quickstart ships exactly that shape, on purpose: its first cluster
masks `customers.profiles` and its second — the same broker, the same topic — does not, and the
records come back in full through it. In a real deployment, every profile that reaches a broker
carrying this data needs the block.

**Nothing on the screen says a value was masked.** The record arrives at the browser with the
asterisks already in it and no flag beside them: `MessageDto` carries the payload, its size, its
serde and any decode error, and nothing that says a rule touched it. So a reader who does not know
about the rule cannot tell `****` from a field whose value really is four asterisks, and the person
best placed to notice that a rule is masking the wrong field is the person with the least to go on.
Until that changes — it is a wire field and a screen, not a configuration key — **the start-up line
and the metric below are the only places that say masking is on**, and it is worth telling the
people who read the topic that it is.

**A rule that matches nothing cannot be detected for you.** The loader refuses six kinds of mistake,
[listed at the end of this page](#when-the-file-is-wrong) — but `fields: [cardnumber]` against a
payload whose field is `cardNumber` is a perfectly well-formed rule that protects nothing, and no
amount of validation can know which of the two spellings your producer uses. **Read the topic after
writing a rule.** It takes one browse and it is the only check there is.

## Telling whether it is on

Two places say so, and both are deliberately quiet about *what* is masked.

**The start-up log.** The message service writes one INFO line per cluster that configures masking:

```
cluster 'production' masks browsed and tracked records with 2 rule(s) (ADR-023, DM-001).
Masking is never applied on produce or resend
```

The **count** and never the rules, because a rule names the fields and topics somebody thought were
worth hiding — which is itself a map of where that cluster's secrets are, and `docker logs` is not
the place for one. A cluster that configures no masking writes no line at all rather than a line
saying zero.

**The metric.** `kui.masking.applied` counts reads on which a rule was in force, labelled
`{cluster, topic, target}` — one increment per read per half of the record, decided once before the
first record is fetched. It is deliberately **not** a count of fields or records masked: that number
is a function of the payload, so a topic whose card-number field is present on 3% of records would
publish that fact to everyone with access to the dashboard.

So the panel that answers "is this topic being masked in production" is a rate over
`kui.masking.applied` for that `topic`, and a rate that falls to zero after a deployment is the
question worth an alert.

## When the file is wrong

Every refusal names the key or the entry and says what to do. The six the loader makes:

| What you wrote | Why it is refused |
| --- | --- |
| a pattern that will not compile | the engine's rules are compiled at start-up precisely so a typo is a boot failure rather than a policy that matches nothing |
| `fields` beside `fieldsNamePattern` | the engine applies the list and never reads the pattern, so the pattern would mask nothing |
| `kind: replace` with no `replacement` | every matched field would become an empty string, which is a `remove` written the long way and does not even do that |
| a key this `kind` does not read — `replacement` on a `mask`, `keep` on a `remove` | the symptom otherwise is a field that comes back looking wrong with nothing to explain it |
| a `keep` end above 20 | see [the bound](#keep-and-the-bound-on-it) |
| a `keep` whose two ends together exceed 20 | the subtle member of the set — see [the migration note](#a-configuration-that-used-to-load) |

A masking list numbered `0` and `2` with no `1` is refused as well, the way every indexed section in
this configuration is, and for a sharper reason: renumbering would change which rule runs first, and
order decides the outcome.
