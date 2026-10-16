package io.kestra.plugin.iggy;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import jakarta.inject.Inject;
import org.apache.iggy.message.HeaderKey;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.PollingKind;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class ConsumeTest extends IggyTest {
    @Inject
    protected RunContextFactory runContextFactory;

    public ConsumeTest(StorageInterface storageInterface) {
        super(storageInterface);
    }

    @Test
    void testConsumeTaskProperties() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of("testStream", "stream-consume", "testTopic", "topic-consume"));

        Consume consume = Consume.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue(8090))
            .username(Property.ofValue("iggy"))
            .password(Property.ofValue("iggy"))
            .stream(Property.ofExpression("{{ testStream }}"))
            .topic(Property.ofExpression("{{ testTopic }}"))
            .consumerId(Property.ofValue("consumer-group-1"))
            .strategy(Property.ofValue(PollingKind.Next))
            .offset(Property.ofValue(BigInteger.valueOf(10)))
            .autoCommit(Property.ofValue(true))
            .maxRecords(Property.ofValue(50))
            .maxDuration(Property.ofValue(Duration.ofSeconds(10)))
            .pollDuration(Property.ofValue(Duration.ofMillis(800)))
            .batchSize(Property.ofValue(25))
            .build();

        assertThat(runContext.render(consume.getHost()).as(String.class).orElse(null), is("localhost"));
        assertThat(runContext.render(consume.getPort()).as(Integer.class).orElse(null), is(8090));
        assertThat(runContext.render(consume.getStream()).as(String.class).orElse(null), is("stream-consume"));
        assertThat(runContext.render(consume.getTopic()).as(String.class).orElse(null), is("topic-consume"));
        assertThat(runContext.render(consume.getConsumerId()).as(String.class).orElse(null), is("consumer-group-1"));
        assertThat(runContext.render(consume.getStrategy()).as(PollingKind.class).orElse(null), is(PollingKind.Next));
        assertThat(runContext.render(consume.getOffset()).as(BigInteger.class).orElse(null), is(BigInteger.valueOf(10)));
        assertThat(runContext.render(consume.getAutoCommit()).as(Boolean.class).orElse(null), is(true));
        assertThat(runContext.render(consume.getMaxRecords()).as(Integer.class).orElse(null), is(50));
        assertThat(runContext.render(consume.getMaxDuration()).as(Duration.class).orElse(null), is(Duration.ofSeconds(10)));
        assertThat(runContext.render(consume.getPollDuration()).as(Duration.class).orElse(null), is(Duration.ofMillis(800)));
        assertThat(runContext.render(consume.getBatchSize()).as(Integer.class).orElse(null), is(25));

        Consume defaultConsume = Consume.builder()
            .host(Property.ofValue("localhost"))
            .stream(Property.ofValue("s"))
            .topic(Property.ofValue("t"))
            .build();
        assertThat(runContext.render(defaultConsume.getBatchSize()).as(Integer.class).orElse(null), is(50));
    }

    @Test
    void testInvalidBatchSize() {
        RunContext runContext = runContextFactory.of();
        Consume consume = Consume.builder()
            .host(Property.ofValue("localhost"))
            .stream(Property.ofValue("s"))
            .topic(Property.ofValue("t"))
            .batchSize(Property.ofValue(0))
            .build();

        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> consume.run(runContext)
        );
    }

    @Test
    void testToMessageMapConversion() {
        Message messageWithoutHeaders = Message.of("plain payload");
        Map<String, Object> map1 = Consume.toMessageMap(messageWithoutHeaders);
        assertThat(map1.get("payload"), is("plain payload"));

        Map<HeaderKey, HeaderValue> headers = Map.of(
            HeaderKey.fromString("traceId"), HeaderValue.fromString("trace-12345"),
            HeaderKey.fromString("env"), HeaderValue.fromString("production")
        );
        Message messageWithHeaders = Message.of("payload with headers", headers);
        Map<String, Object> map2 = Consume.toMessageMap(messageWithHeaders);
        assertThat(map2.get("payload"), is("payload with headers"));
        assertThat(map2.containsKey("headers"), is(true));

        @SuppressWarnings("unchecked")
        Map<String, String> parsedHeaders = (Map<String, String>) map2.get("headers");
        assertThat(parsedHeaders.get("traceId"), is("trace-12345"));
        assertThat(parsedHeaders.get("env"), is("production"));
    }

    @Test
    void testConsumeIntegration() throws Exception {
        String streamName = "consume-test-" + UUID.randomUUID().toString().substring(0, 8);
        String topicName = "topic-test-" + UUID.randomUUID().toString().substring(0, 8);
        RunContext runContext = runContextFactory.of();

        Produce.builder()
            .host(Property.ofValue(DEFAULT_HOST))
            .port(Property.ofValue(DEFAULT_PORT))
            .username(Property.ofValue(DEFAULT_USERNAME))
            .password(Property.ofValue(DEFAULT_PASSWORD))
            .stream(Property.ofValue(streamName))
            .topic(Property.ofValue(topicName))
            .createIfMissing(Property.ofValue(true))
            .from(Map.of("payload", "hello-consume-integration"))
            .build()
            .run(runContext);

        Consume.Output consumeOutput = Consume.builder()
            .host(Property.ofValue(DEFAULT_HOST))
            .port(Property.ofValue(DEFAULT_PORT))
            .username(Property.ofValue(DEFAULT_USERNAME))
            .password(Property.ofValue(DEFAULT_PASSWORD))
            .stream(Property.ofValue(streamName))
            .topic(Property.ofValue(topicName))
            .consumerId(Property.ofValue("1"))
            .maxRecords(Property.ofValue(1))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .build()
            .run(runContext);

        assertThat(consumeOutput.getMessagesCount(), is(1));
        List<Map<String, Object>> messages = toMessages(consumeOutput);
        assertThat(messages.size(), is(1));
        assertThat(messages.get(0).get("payload"), is("hello-consume-integration"));
    }

    @Test
    void testKillAndStopLifecycle() {
        Consume consume = Consume.builder()
            .host(Property.ofValue("localhost"))
            .stream(Property.ofValue("s"))
            .topic(Property.ofValue("t"))
            .build();

        // Should be non-blocking and safe even if not yet running
        consume.stop();
        consume.kill();
    }
}
