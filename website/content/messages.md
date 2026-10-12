## Choose a starting point

Open a topic's message browser. Select earliest, latest, a specific offset, or a timestamp, depending on the question you are investigating. A partition offset identifies a position within that partition, not a global position across the topic.

## Decode the right format

Select the key and value serdes for the topic. KUI supports String, JSON, numeric values, UUID, Base64, Hex, and Schema Registry-backed Avro, JSON Schema, and Protobuf. Registry-backed formats require a configured registry and compatible schemas.

![Expanded record with copy controls](/screenshots/31-copy-controls.webp)

## Inspect and copy a payload

Expand a record to inspect its key, value, headers, partition, offset, and timestamp. Use the separate copy controls for the field you need. Payload copy writes to your clipboard; it is different from [copying records into another topic](../copying/).

## Control how records load

Use pagination for bounded pages or infinite scroll for continued reading. A browse streams results rather than loading an entire topic into the browser. Adjust the browse controls when a topic is large or a narrow time window is enough.

## Publish a record

Open the producer action, choose the appropriate formats, and enter the key, value, and supported headers. Check the topic and partition choices before submitting. Use the returned result to verify the write, and browse the destination to inspect the new record.

Producing requires write permission and a writable cluster. When working with schema-backed data, ensure that the input matches the schema expected by consumers.

## Track an event

The message tracker helps follow records across topics. Choose the relevant topic set and matching criteria in its controls, then inspect the matching events in context. It is a record investigation tool, not a replacement for distributed application tracing.
