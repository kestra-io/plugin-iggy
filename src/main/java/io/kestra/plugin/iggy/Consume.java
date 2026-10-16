package io.kestra.plugin.iggy;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume messages from Apache Iggy stream and topic",
    description = "Pulls messages from an Apache Iggy stream and topic, tracks consumer offsets, and stores consumed messages into Kestra internal storage. Designed for UTF-8 text and JSON payloads; arbitrary binary payloads are not supported."
)
@Plugin(
    examples = {
        @Example(
            title = "Consume messages from an Apache Iggy topic",
            full = true,
            code = """
                id: iggy_consume
                namespace: company.team
                tasks:
                  - id: consume
                    type: io.kestra.plugin.iggy.Consume
                    host: iggy.example.com
                    port: 8090
                    username: "{{ secret('IGGY_USERNAME') }}"
                    password: "{{ secret('IGGY_PASSWORD') }}"
                    stream: orders
                    topic: created
                    maxRecords: 100
                    maxDuration: PT30S
                  - id: log_count
                    type: io.kestra.plugin.core.log.Log
                    message: "Consumed {{ outputs.consume.messagesCount }} messages"
                """
        )
    }
)
public class Consume extends IggyConnection implements RunnableTask<Consume.Output>, ConsumeInterface {
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
        description = "Whether to automatically commit consumer offsets upon polling. Defaults to true."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Boolean> autoCommit = Property.ofValue(true);

    @Schema(
        title = "Maximum records to consume",
        description = "Optional cap on total messages before stopping."
    )
    @PluginProperty(group = "execution")
    private Property<Integer> maxRecords;

    @Schema(
        title = "Maximum polling duration",
        description = "Maximum duration to poll before stopping execution."
    )
    @PluginProperty(group = "execution")
    private Property<Duration> maxDuration;

    @Schema(
        title = "Poll interval duration",
        description = "Wait duration between empty poll attempts. Defaults to PT1S."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Duration> pollDuration = Property.ofValue(Duration.ofSeconds(1));

    @Schema(
        title = "Batch size",
        description = "Maximum messages fetched per poll. Defaults to 50."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    @Min(1)
    private Property<Integer> batchSize = Property.ofValue(50);

    @Getter(AccessLevel.NONE)
    @JsonIgnore
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final transient AtomicBoolean isActive = new AtomicBoolean(true);

    @Getter(AccessLevel.NONE)
    @JsonIgnore
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final transient CountDownLatch waitForTermination = new CountDownLatch(1);

    private static final Duration AWAIT_ON_KILL = Duration.ofMillis(50);

    @Override
    public Output run(RunContext runContext) throws Exception {
        int rBatchSize = this.batchSize != null ?
            runContext.render(this.batchSize).as(Integer.class).orElse(50) : 50;
        if (rBatchSize < 1) {
            throw new IllegalArgumentException("batchSize must be greater than or equal to 1, but got: " + rBatchSize);
        }

        String streamStr = runContext.render(this.stream).as(String.class).orElseThrow();
        String topicStr = runContext.render(this.topic).as(String.class).orElseThrow();

        StreamId streamId = resolveStreamId(streamStr);
        TopicId topicId = resolveTopicId(topicStr);

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

        IggyBaseClient client = createClient(runContext);
        try {
            Instant pollStart = Instant.now();
            int total = 0;
            File outputFile = runContext.workingDir().createTempFile(".ion").toFile();

            try (OutputStream output = new BufferedOutputStream(new FileOutputStream(outputFile))) {
                do {
                    if (!isActive.get()) {
                        break;
                    }

                    int currentTotal = total;
                    int maxRemaining = this.maxRecords != null ?
                        runContext.render(this.maxRecords).as(Integer.class).map(max -> max - currentTotal).orElse(rBatchSize) :
                        rBatchSize;

                    if (maxRemaining <= 0) {
                        break;
                    }

                    long fetchCount = Math.min(rBatchSize, maxRemaining);
                    PolledMessages polled = client.messages().pollMessages(
                        streamId, topicId, partId, consumer, pollingStrategy, fetchCount, commit
                    );

                    List<Message> messages = polled != null && polled.messages() != null ?
                        polled.messages() : Collections.emptyList();

                    if (messages.isEmpty()) {
                        Duration waitDur = this.pollDuration != null ?
                            runContext.render(this.pollDuration).as(Duration.class).orElse(Duration.ofSeconds(1)) :
                            Duration.ofSeconds(1);

                        if (waitDur != null && !waitDur.isNegative() && !waitDur.isZero()) {
                            long remainingMillis = waitDur.toMillis();
                            while (remainingMillis > 0 && isActive.get()) {
                                long sleepTime = Math.min(remainingMillis, 100);
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

                        // If empty, check whether we should break or continue if maxDuration allows
                        if (maxDuration == null) {
                            break;
                        }
                        if (isEnded(total, pollStart, runContext)) {
                            break;
                        }
                        continue;
                    }

                    for (Message message : messages) {
                        if (!isActive.get()) {
                            break;
                        }
                        Map<String, Object> map = toMessageMap(message);
                        FileSerde.write(output, map);
                        total++;
                    }
                } while (isActive.get() && !isEnded(total, pollStart, runContext));
            } finally {
                waitForTermination.countDown();
            }

            URI uri = runContext.storage().putFile(outputFile);

            return Output.builder()
                .messagesCount(total)
                .uri(uri)
                .build();
        } finally {
            closeClient(client, runContext.logger());
        }
    }

    private boolean isEnded(int total, Instant pollStart, RunContext runContext) throws Exception {
        if (this.maxRecords != null) {
            Integer max = runContext.render(this.maxRecords).as(Integer.class).orElse(null);
            if (max != null && total >= max) {
                return true;
            }
        }
        if (this.maxDuration != null) {
            Duration maxDur = runContext.render(this.maxDuration).as(Duration.class).orElse(null);
            if (maxDur != null && !Instant.now().isBefore(pollStart.plus(maxDur))) {
                return true;
            }
        }
        return false;
    }

    public static Map<String, Object> toMessageMap(Message message) {
        Map<String, Object> map = new HashMap<>();
        map.put("payload", new String(message.payload(), StandardCharsets.UTF_8));
        if (message.header() != null) {
            if (message.header().id() != null) {
                map.put("id", message.header().id().toString());
            }
            if (message.header().offset() != null) {
                map.put("offset", message.header().offset());
            }
            if (message.header().timestamp() != null) {
                long rawTs = message.header().timestamp().longValue();
                Instant ts = rawTs > 10_000_000_000_000L ?
                    Instant.ofEpochMilli(rawTs / 1000) :
                    Instant.ofEpochMilli(rawTs);
                map.put("timestamp", ts);
            }
        }
        if (message.userHeaders() != null && !message.userHeaders().isEmpty()) {
            Map<String, String> headersMap = new HashMap<>();
            message.userHeaders().forEach((k, v) -> {
                if (k != null && v != null) {
                    headersMap.put(k.toString(), v.asString());
                }
            });
            map.put("headers", headersMap);
        }
        return map;
    }

    public void kill() {
        stop(true);
    }

    public void stop() {
        stop(false);
    }

    private void stop(boolean wait) {
        isActive.set(false);
        if (wait) {
            try {
                waitForTermination.await(AWAIT_ON_KILL.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Messages consumed count",
            description = "Total number of messages consumed during this task execution."
        )
        private final Integer messagesCount;

        @Schema(
            title = "Output file URI",
            description = "Kestra internal storage URI of the file containing consumed messages."
        )
        private final URI uri;
    }

    @Builder
    @Getter
    public static class IggyMessageOutput implements io.kestra.core.models.tasks.Output {
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
    }
}
