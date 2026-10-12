## What a replay does

KUI copies selected partition offset ranges into a destination topic. Keys, values, and headers are rewritten as the original bytes, without deserialization or transformation. The source stays unchanged; records are appended to the destination.

> A replay is not atomic and has no undo. If it fails or is cancelled partway through, records already written remain. Destination consumers cannot distinguish a copy from the original producer's output.

## Choose a destination and range

1. Open the source topic's message browser and its record-copy action.
2. Set **Copy into topic** to the intended destination.
3. Add the source partition and **From offset** / **Until offset** for each range.
4. Read the byte-preservation, append, partial-write, and retention notices.
5. Type the destination topic's name to confirm, then start the copy.

The upper bound is exclusive. A range from `100` until `103` names offsets 100, 101, and 102. It does not include offset 103.

## Keep requests bounded

A request may name at most **10,000 records** across its ranges. Split a larger replay into bounded ranges. This is an offset-range replay: a filter used while browsing does not turn it into a copy of only matching rows.

The destination must be appropriate for the payloads and consumers. Copying schema-backed bytes does not migrate schemas into another registry. This UI workflow is within a selected cluster; it is not a cross-cluster migration utility.

## Review the receipt

Read both the number of records **read** and **written**. Retention may have removed offsets after you selected the range. A successful HTTP response with zero written records is not evidence that the expected data was copied.

Inspect failed source records or incomplete ranges when shown. Verify the destination in the message browser before deciding whether to retry. Retrying an already written range can append duplicates.

## Copying versus a destructive plan

A replay has confirmation but no destructive plan-token endpoint, because it leaves the source intact. Consumer offset resets and destructive topic operations use separate plan workflows. See [consumer offset plans](../consumers/).
