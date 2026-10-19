package io.kestra.plugin.iggy;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.RealtimeTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
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
public class RealtimeTrigger extends AbstractIggyTrigger implements RealtimeTriggerInterface, TriggerOutput<RealtimeTrigger.Output> {
    @JsonIgnore
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @Getter(AccessLevel.NONE)
    private final transient AtomicBoolean isActive = new AtomicBoolean(true);

    @JsonIgnore
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    @Getter(AccessLevel.NONE)
    private final transient CountDownLatch waitForTermination = new CountDownLatch(1);

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

                var rStream = runContext.render(this.stream).as(String.class)
                    .orElseThrow(() -> new IllegalArgumentException("stream is required: set it or check the expression"));
                var rTopic = runContext.render(this.topic).as(String.class)
                    .orElseThrow(() -> new IllegalArgumentException("topic is required: set it or check the expression"));

                var streamId = IggyConnection.resolveStreamId(rStream);
                var topicId = IggyConnection.resolveTopicId(rTopic);

                var rPartitionId = this.partitionId != null ?
                    Optional.of(runContext.render(this.partitionId).as(Long.class)
                        .orElseThrow(() -> new IllegalArgumentException("partitionId is invalid: check the expression"))) :
                    Optional.<Long>empty();

                var rConsumerId = this.consumerId != null ?
                    runContext.render(this.consumerId).as(String.class).orElse("1") : "1";
                var consumer = NumberUtils.isDigits(rConsumerId) ?
                    Consumer.of(Long.parseLong(rConsumerId)) :
                    Consumer.of(ConsumerId.of(rConsumerId));

                var rStrategy = this.strategy != null ?
                    runContext.render(this.strategy).as(PollingKind.class).orElse(PollingKind.Next) :
                    PollingKind.Next;

                var rOffset = this.offset != null ?
                    runContext.render(this.offset).as(BigInteger.class).orElse(BigInteger.ZERO) :
                    BigInteger.ZERO;

                var pollingStrategy = switch (rStrategy) {
                    case First -> PollingStrategy.first();
                    case Last -> PollingStrategy.last();
                    case Offset -> PollingStrategy.offset(rOffset);
                    case Timestamp -> PollingStrategy.timestamp(rOffset);
                    default -> PollingStrategy.next();
                };

                var rAutoCommit = this.autoCommit == null ||
                    runContext.render(this.autoCommit).as(Boolean.class).orElse(true);

                var rPollDuration = this.pollDuration != null ?
                    runContext.render(this.pollDuration).as(Duration.class).orElse(Duration.ofSeconds(1)) :
                    Duration.ofSeconds(1);

                var rBatchSize = this.batchSize != null ?
                    runContext.render(this.batchSize).as(Integer.class).orElse(50) : 50;
                if (rBatchSize < 1) {
                    throw new IllegalArgumentException("batchSize must be greater than or equal to 1, but got: " + rBatchSize);
                }

                BigInteger lastEmittedOffset = null;
                BigInteger lastStoredOffset = null;

                while (isActive.get() && !emitter.isCancelled()) {
                    if (emitter.requestedFromDownstream() <= 0) {
                        long remainingMillis = rPollDuration != null ? Math.max(rPollDuration.toMillis(), 10) : 100;
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

                    PolledMessages polled = client.messages().pollMessages(
                        streamId, topicId, rPartitionId, consumer, pollingStrategy, (long) rBatchSize, false
                    );

                    List<Message> messages = (polled != null && polled.messages() != null) ?
                        polled.messages() : Collections.emptyList();

                    if (messages.isEmpty()) {
                        if (rPollDuration != null && !rPollDuration.isNegative() && !rPollDuration.isZero()) {
                            long remainingMillis = rPollDuration.toMillis();
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
                        BigInteger batchLastEmittedOffset = null;
                        try {
                            for (Message message : messages) {
                                if (!isActive.get() || emitter.isCancelled()) {
                                    break;
                                }
                                Output output = toOutput(message, rStream, rTopic, rPartitionId);
                                emitter.next(output);

                                if (message.header() != null && message.header().offset() != null) {
                                    batchLastEmittedOffset = message.header().offset();
                                    lastEmittedOffset = batchLastEmittedOffset;
                                }
                            }
                        } finally {
                            if (rAutoCommit && batchLastEmittedOffset != null && !batchLastEmittedOffset.equals(lastStoredOffset)) {
                                client.consumerOffsets().storeConsumerOffset(
                                    streamId, topicId, rPartitionId, consumer, batchLastEmittedOffset
                                );
                                lastStoredOffset = batchLastEmittedOffset;
                            }
                        }

                        if (lastEmittedOffset != null && (!rAutoCommit || rStrategy != PollingKind.Next)) {
                            pollingStrategy = PollingStrategy.offset(lastEmittedOffset.add(BigInteger.ONE));
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
