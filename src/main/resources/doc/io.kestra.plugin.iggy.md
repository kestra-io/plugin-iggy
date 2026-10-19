# How to use the Apache Iggy plugin

Publish and consume messages on Apache Iggy streams and topics, track consumer offsets, and react to streaming events from Kestra flows.

## Common properties

Set `host` and `port` to your Apache Iggy server connection details (default port `8090` for TCP, `3000` for HTTP). Set `transport` to `TCP` (default) or `HTTP`. For authentication, provide `username` and `password` or a personal access `token`. Enable TLS via `tls: true`. Sensitive values should be stored in [secrets](https://kestra.io/docs/concepts/secret).

## Payload encoding

The Apache Iggy plugin is designed for UTF-8 text and JSON payloads. When publishing messages, string and JSON payloads are encoded as UTF-8. Consumed messages decode payloads as UTF-8 strings. Arbitrary binary payloads are not supported and cannot be guaranteed to round-trip without corruption.

## Tasks

- `Produce` publishes messages to an Apache Iggy `stream` and `topic` — pass message data via `from`. Messages can include `payload`, optional `key`, and `headers` map. Supports inline list of messages, map, or internal storage URI. By default, missing streams and topics are not created automatically; set `createIfMissing: true` to allow automatic creation of missing resources.
- `Consume` reads messages from an Apache Iggy `stream` and `topic`. Set `consumerId` to identify the consumer, `partitionId` to target a partition, and bound the batch with `maxRecords`, `maxDuration`, or `batchSize`. Messages are stored in Kestra internal storage.

## Triggers

- `Trigger` polls on a schedule (default 60 seconds) and starts one execution per batch of messages. Only `strategy: Next` and `autoCommit: true` are supported to prevent duplicate executions across scheduled evaluations. For other offset strategies or manual commit control, use the `Consume` task.
- `RealtimeTrigger` listens to new messages in real-time and starts one execution per message as it arrives. When `autoCommit` is enabled (default `true`), consumer offsets are committed after the message is emitted downstream into the reactive pipeline (providing at-least-once delivery up to trigger emission). If the execution engine crashes after emission but before workflow execution finishes, the message will not be re-polled unless offsets are reset. Set `autoCommit: false` if you want to prevent automatic offset commits.
