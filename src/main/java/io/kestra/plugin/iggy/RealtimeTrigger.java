package io.kestra.plugin.iggy;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.RealtimeTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingKind;
import org.apache.iggy.message.PollingStrategy;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on real-time Apache Iggy messages",
    description = "Reacts to Apache Iggy messages in real-time by creating one execution per message as soon as it arrives on the stream and topic. Designed for UTF-8 text and JSON payloads; arbitrary binary payloads are not supported. Consumer offsets are committed after the message is emitted downstream when autoCommit is enabled."
)
@Plugin(
    examples = {
        @Example(
            title = "Consume Apache Iggy messages in real-time and create an execution per message",
            full = true,
            code = """
                id: iggy_realtime_trigger
                namespace: company.team
                triggers:
                  - id: realtime_watch
                    type: io.kestra.plugin.iggy.RealtimeTrigger
                    host: iggy.example.com
                    port: 8090
                    username: "{{ secret('IGGY_USERNAME') }}"
                    password: "{{ secret('IGGY_PASSWORD') }}"
                    stream: telemetry
                    topic: sensors
                    consumerId: "1"
                    strategy: Next
                tasks:
                  - id: log_message
                    type: io.kestra.plugin.core.log.Log
                    message: "Received sensor data: {{ trigger.payload }} (id={{ trigger.id }}, offset={{ trigger.offset }})"
                """
        )
    }
)
public class RealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<RealtimeTrigger.Output>, IggyConnectionInterface {
    @NotNull
    @Schema(
        title = "Server host",
        description = "Hostname or IP address of the Apache Iggy server."
    )
    @PluginProperty(group = "connection")
    private Property<String> host;

    @Schema(
        title = "Server port",
        description = "Port of the Apache Iggy server. Defaults to 8090 for TCP and 3000 for HTTP."
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    private Property<Integer> port = Property.ofValue(8090);

    @Schema(
        title = "Transport protocol",
        description = "Transport protocol to use: TCP (default) or HTTP."
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    private Property<TransportType> transport = Property.ofValue(TransportType.TCP);

    @Schema(
        title = "Authentication username",
        description = "Username for authentication."
    )
    @PluginProperty(group = "connection")
    private Property<String> username;

    @ToString.Exclude
    @Schema(
        title = "Authentication password",
        description = "Password for authentication."
    )
    @PluginProperty(secret = true, group = "connection")
    private Property<String> password;

    @ToString.Exclude
    @Schema(
        title = "Personal access token",
        description = "Personal access token (PAT) for token-based authentication."
    )
    @PluginProperty(secret = true, group = "connection")
    private Property<String> token;

    @Schema(
        title = "Use TLS",
        description = "Whether to enable TLS encryption for the connection. Defaults to false."
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    private Property<Boolean> tls = Property.ofValue(false);

    @NotNull
    @Schema(
        title = "Stream identifier or name",
        description = "Name or ID of the Apache Iggy stream to consume messages from."
    )
    @PluginProperty(group = "main")
    private Property<String> stream;

    @NotNull
    @Schema(
        title = "Topic identifier or name",
        description = "Name or ID of the Apache Iggy topic to consume messages from."
    )
    @PluginProperty(group = "main")
    private Property<String> topic;

    @Schema(
        title = "Partition ID",
        description = "Optional partition ID to consume from."
    )
    @PluginProperty(group = "main")
    private Property<Long> partitionId;

    @Schema(
        title = "Consumer ID or Name",
        description = "Identifier of the consumer used to track offsets. Defaults to '1'."
    )
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<String> consumerId = Property.ofValue("1");

    @Schema(
        title = "Polling strategy kind",
        description = "Polling strategy: Offset, Timestamp, First, Last, or Next. Defaults to Next."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<PollingKind> strategy = Property.ofValue(PollingKind.Next);

    @Schema(
        title = "Offset value",
        description = "Offset or timestamp value to use when strategy is Offset or Timestamp."
    )
    @PluginProperty(group = "execution")
    private Property<BigInteger> offset;

    @Schema(
        title = "Auto-commit offsets",
        description = "Whether to automatically commit consumer offsets after successfully emitting each message downstream. Defaults to true."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Boolean> autoCommit = Property.ofValue(true);

    @Schema(
        title = "Poll wait duration",
        description = "Wait duration between empty poll attempts. Defaults to PT0.1S (100ms)."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Duration> pollDuration = Property.ofValue(Duration.ofMillis(100));

    @Schema(
        title = "Batch size",
        description = "Maximum messages fetched per poll cycle. Defaults to 10."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    @Min(1)
    private Property<Integer> batchSize = Property.ofValue(10);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final CountDownLatch waitForTermination = new CountDownLatch(1);

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        return Flux
            .from(publisher(conditionContext.getRunContext()))
            .map(record -> TriggerService.generateRealtimeExecution(this, conditionContext, context, record));
    }

    public Publisher<Output> publisher(final RunContext runContext) throws Exception {
        return Flux.create(emitter -> {
            emitter.onDispose(() -> stop(false));

            IggyBaseClient client = null;
            try {
                client = IggyConnection.createClient(this, runContext);

                String streamStr = runContext.render(this.stream).as(String.class).orElseThrow();
                String topicStr = runContext.render(this.topic).as(String.class).orElseThrow();

                StreamId streamId = IggyConnection.resolveStreamId(streamStr);
                TopicId topicId = IggyConnection.resolveTopicId(topicStr);

                Optional<Long> partId = this.partitionId != null ?
                    Optional.of(runContext.render(this.partitionId).as(Long.class).orElseThrow()) :
                    Optional.empty();

                String cId = this.consumerId != null ?
                    runContext.render(this.consumerId).as(String.class).orElse("1") : "1";
                Consumer consumer = NumberUtils.isDigits(cId) ?
                    Consumer.of(Long.parseLong(cId)) :
                    Consumer.of(ConsumerId.of(cId));

                PollingKind kind = this.strategy != null ?
                    runContext.render(this.strategy).as(PollingKind.class).orElse(PollingKind.Next) :
                    PollingKind.Next;

                BigInteger offsetVal = this.offset != null ?
                    runContext.render(this.offset).as(BigInteger.class).orElse(BigInteger.ZERO) :
                    BigInteger.ZERO;

                PollingStrategy pollingStrategy = switch (kind) {
                    case First -> PollingStrategy.first();
                    case Last -> PollingStrategy.last();
                    case Offset -> PollingStrategy.offset(offsetVal);
                    case Timestamp -> PollingStrategy.timestamp(offsetVal);
                    default -> PollingStrategy.next();
                };

                boolean commit = this.autoCommit == null ||
                    runContext.render(this.autoCommit).as(Boolean.class).orElse(true);

                Duration pollWait = this.pollDuration != null ?
                    runContext.render(this.pollDuration).as(Duration.class).orElse(Duration.ofMillis(100)) :
                    Duration.ofMillis(100);

                int rBatchSize = this.batchSize != null ?
                    runContext.render(this.batchSize).as(Integer.class).orElse(10) : 10;
                if (rBatchSize < 1) {
                    throw new IllegalArgumentException("batchSize must be greater than or equal to 1, but got: " + rBatchSize);
                }

                while (isActive.get() && !emitter.isCancelled()) {
                    if (emitter.requestedFromDownstream() <= 0) {
                        long remainingMillis = pollWait != null ? Math.max(pollWait.toMillis(), 10) : 100;
                        while (remainingMillis > 0 && isActive.get() && !emitter.isCancelled() && emitter.requestedFromDownstream() <= 0) {
                            long sleepTime = Math.min(remainingMillis, 50);
                            try {
                                Thread.sleep(sleepTime);
                            } catch (InterruptedException e) {
                                isActive.set(false);
                                Thread.currentThread().interrupt();
                                break;
                            }
                            remainingMillis -= sleepTime;
                        }
                        continue;
                    }

                    // Poll messages with autoCommit = false to avoid prematurely committing offsets before emission
                    PolledMessages polled = client.messages().pollMessages(
                        streamId, topicId, partId, consumer, pollingStrategy, (long) rBatchSize, false
                    );

                    List<Message> messages = (polled != null && polled.messages() != null) ?
                        polled.messages() : Collections.emptyList();

                    if (messages.isEmpty()) {
                        if (pollWait != null && !pollWait.isNegative() && !pollWait.isZero()) {
                            long remainingMillis = pollWait.toMillis();
                            while (remainingMillis > 0 && isActive.get() && !emitter.isCancelled()) {
                                long sleepTime = Math.min(remainingMillis, 50);
                                try {
                                    Thread.sleep(sleepTime);
                                } catch (InterruptedException e) {
                                    isActive.set(false);
                                    Thread.currentThread().interrupt();
                                    break;
                                }
                                remainingMillis -= sleepTime;
                            }
                        }
                    } else {
                        for (Message message : messages) {
                            if (!isActive.get() || emitter.isCancelled()) {
                                break;
                            }
                            Output output = toOutput(message, streamStr, topicStr, partId);
                            emitter.next(output);

                            if (commit && message.header() != null && message.header().offset() != null) {
                                client.consumerOffsets().storeConsumerOffset(
                                    streamId, topicId, partId, consumer, message.header().offset()
                                );
                            }
                        }
                    }

                    if (Thread.currentThread().isInterrupted()) {
                        isActive.set(false);
                        break;
                    }
                }
                emitter.complete();
            } catch (Throwable throwable) {
                emitter.error(throwable);
            } finally {
                IggyConnection.closeClient(client, runContext.logger());
                waitForTermination.countDown();
            }
        }, FluxSink.OverflowStrategy.BUFFER);
    }

    private Output toOutput(Message message, String stream, String topic, Optional<Long> partitionId) {
        String payload = new String(message.payload(), StandardCharsets.UTF_8);
        String id = (message.header() != null && message.header().id() != null) ? message.header().id().toString() : null;
        BigInteger offset = (message.header() != null) ? message.header().offset() : null;

        Instant timestamp = null;
        if (message.header() != null && message.header().timestamp() != null) {
            long rawTs = message.header().timestamp().longValue();
            timestamp = rawTs > 10_000_000_000_000L ? Instant.ofEpochMilli(rawTs / 1000) : Instant.ofEpochMilli(rawTs);
        }

        Map<String, String> headers = null;
        if (message.userHeaders() != null && !message.userHeaders().isEmpty()) {
            Map<String, String> headersMap = new HashMap<>();
            message.userHeaders().forEach((k, v) -> {
                if (k != null && v != null) {
                    headersMap.put(k.toString(), v.asString());
                }
            });
            headers = headersMap;
        }

        return Output.builder()
            .payload(payload)
            .id(id)
            .offset(offset)
            .timestamp(timestamp)
            .headers(headers)
            .stream(stream)
            .topic(topic)
            .partitionId(partitionId.orElse(null))
            .build();
    }

    @Override
    public void kill() {
        stop(true);
    }

    @Override
    public void stop() {
        stop(false);
    }

    private void stop(boolean wait) {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }

        if (wait) {
            try {
                this.waitForTermination.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Message payload",
            description = "Payload of the consumed message as a UTF-8 string. Designed for text and JSON payloads; arbitrary binary payloads are not supported."
        )
        private final String payload;

        @Schema(
            title = "Message ID",
            description = "Unique ID of the consumed message."
        )
        private final String id;

        @Schema(
            title = "Message offset",
            description = "Offset of the message within the partition."
        )
        private final BigInteger offset;

        @Schema(
            title = "Message timestamp",
            description = "Timestamp when the message was received by the broker."
        )
        private final Instant timestamp;

        @Schema(
            title = "Message headers",
            description = "Headers associated with the consumed message."
        )
        private final Map<String, String> headers;

        @Schema(
            title = "Stream identifier",
            description = "Stream from which the message was consumed."
        )
        private final String stream;

        @Schema(
            title = "Topic identifier",
            description = "Topic from which the message was consumed."
        )
        private final String topic;

        @Schema(
            title = "Partition ID",
            description = "Partition ID from which the message was consumed."
        )
        private final Long partitionId;
    }
}
