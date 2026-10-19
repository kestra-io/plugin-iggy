package io.kestra.plugin.iggy;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class ProduceTest extends IggyTest {
    @Inject
    protected RunContextFactory runContextFactory;

    public ProduceTest(StorageInterface storageInterface) {
        super(storageInterface);
    }

    @Test
    void testProduceTaskProperties() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of("testStream", "stream-1", "testTopic", "topic-1"));

        Produce produce = Produce.builder()
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue(8090))
            .username(Property.ofValue("iggy"))
            .password(Property.ofValue("iggy"))
            .stream(Property.ofExpression("{{ testStream }}"))
            .topic(Property.ofExpression("{{ testTopic }}"))
            .partitionId(Property.ofValue(1L))
            .from(Map.of("payload", "hello world", "headers", Map.of("k1", "v1")))
            .build();

        assertThat(runContext.render(produce.getHost()).as(String.class).orElse(null), is("localhost"));
        assertThat(runContext.render(produce.getPort()).as(Integer.class).orElse(null), is(8090));
        assertThat(runContext.render(produce.getStream()).as(String.class).orElse(null), is("stream-1"));
        assertThat(runContext.render(produce.getTopic()).as(String.class).orElse(null), is("topic-1"));
        assertThat(runContext.render(produce.getPartitionId()).as(Long.class).orElse(null), is(1L));
        assertThat(runContext.render(produce.getCreateIfMissing()).as(Boolean.class).orElse(null), is(false));
        assertThat(produce.getFrom(), notNullValue());

        Produce produceWithCreate = Produce.builder()
            .host(Property.ofValue("localhost"))
            .stream(Property.ofValue("stream-1"))
            .topic(Property.ofValue("topic-1"))
            .from("data")
            .createIfMissing(Property.ofValue(true))
            .build();
        assertThat(runContext.render(produceWithCreate.getCreateIfMissing()).as(Boolean.class).orElse(null), is(true));
    }

    @Test
    void testToMessageUtf8AndJsonEncoding() throws Exception {
        Produce produce = Produce.builder()
            .host(Property.ofValue("localhost"))
            .stream(Property.ofValue("s"))
            .topic(Property.ofValue("t"))
            .from("test")
            .build();

        org.apache.iggy.message.Message textMsg = produce.toMessage("plain text");
        assertThat(new String(textMsg.payload(), java.nio.charset.StandardCharsets.UTF_8), is("plain text"));

        org.apache.iggy.message.Message unicodeMsg = produce.toMessage("Hello, 世界! 🚀");
        assertThat(new String(unicodeMsg.payload(), java.nio.charset.StandardCharsets.UTF_8), is("Hello, 世界! 🚀"));

        org.apache.iggy.message.Message jsonMsg = produce.toMessage(Map.of("key", "value", "count", 42));
        assertThat(new String(jsonMsg.payload(), java.nio.charset.StandardCharsets.UTF_8), containsString("\"key\":\"value\""));

        org.apache.iggy.message.Message mapPayloadMsg = produce.toMessage(Map.of("payload", "explicit-payload", "headers", Map.of("h1", "v1")));
        assertThat(new String(mapPayloadMsg.payload(), java.nio.charset.StandardCharsets.UTF_8), is("explicit-payload"));
    }

    @Test
    void testProduceAndConsumeIntegration() throws Exception {
        assumeIggyReachable();
        String streamName = "test-stream-" + UUID.randomUUID().toString().substring(0, 8);
        String topicName = "test-topic-" + UUID.randomUUID().toString().substring(0, 8);
        RunContext runContext = runContextFactory.of();

        Produce.Output produceOutput = Produce.builder()
            .host(Property.ofValue(DEFAULT_HOST))
            .port(Property.ofValue(DEFAULT_PORT))
            .username(Property.ofValue(DEFAULT_USERNAME))
            .password(Property.ofValue(DEFAULT_PASSWORD))
            .stream(Property.ofValue(streamName))
            .topic(Property.ofValue(topicName))
            .createIfMissing(Property.ofValue(true))
            .from(List.of(
                Map.of("payload", "message-one", "headers", Map.of("key1", "val1")),
                Map.of("payload", "message-two"),
                Map.of("payload", "message-utf8-🚀-accentué")
            ))
            .build()
            .run(runContext);

        assertThat(produceOutput.getMessagesCount(), is(3));

        Consume.Output consumeOutput = Consume.builder()
            .host(Property.ofValue(DEFAULT_HOST))
            .port(Property.ofValue(DEFAULT_PORT))
            .username(Property.ofValue(DEFAULT_USERNAME))
            .password(Property.ofValue(DEFAULT_PASSWORD))
            .stream(Property.ofValue(streamName))
            .topic(Property.ofValue(topicName))
            .consumerId(Property.ofValue("1"))
            .maxRecords(Property.ofValue(3))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .build()
            .run(runContext);

        assertThat(consumeOutput.getMessagesCount(), is(3));
        List<Map<String, Object>> messages = toMessages(consumeOutput);
        assertThat(messages.size(), is(3));
        assertThat(messages.get(0).get("payload"), is("message-one"));
        assertThat(messages.get(1).get("payload"), is("message-two"));
        assertThat(messages.get(2).get("payload"), is("message-utf8-🚀-accentué"));
    }

    @Test
    void testProduceFromInternalStorageIntegration() throws Exception {
        assumeIggyReachable();
        String streamName = "storage-stream-" + UUID.randomUUID().toString().substring(0, 8);
        String topicName = "storage-topic-" + UUID.randomUUID().toString().substring(0, 8);
        RunContext runContext = runContextFactory.of();

        List<Map<String, Object>> records = List.of(
            Map.of("payload", "from-storage-1", "headers", Map.of("env", "test")),
            Map.of("payload", "from-storage-2")
        );

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (OutputStream outputStream = new FileOutputStream(tempFile)) {
            for (Map<String, Object> record : records) {
                FileSerde.write(outputStream, record);
            }
        }

        URI storageUri = storageInterface.put(
            TenantService.MAIN_TENANT, null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new FileInputStream(tempFile)
        );

        Produce.Output produceOutput = Produce.builder()
            .host(Property.ofValue(DEFAULT_HOST))
            .port(Property.ofValue(DEFAULT_PORT))
            .username(Property.ofValue(DEFAULT_USERNAME))
            .password(Property.ofValue(DEFAULT_PASSWORD))
            .stream(Property.ofValue(streamName))
            .topic(Property.ofValue(topicName))
            .createIfMissing(Property.ofValue(true))
            .from(storageUri.toString())
            .build()
            .run(runContext);

        assertThat(produceOutput.getMessagesCount(), is(2));

        Consume.Output consumeOutput = Consume.builder()
            .host(Property.ofValue(DEFAULT_HOST))
            .port(Property.ofValue(DEFAULT_PORT))
            .username(Property.ofValue(DEFAULT_USERNAME))
            .password(Property.ofValue(DEFAULT_PASSWORD))
            .stream(Property.ofValue(streamName))
            .topic(Property.ofValue(topicName))
            .consumerId(Property.ofValue("1"))
            .maxRecords(Property.ofValue(2))
            .maxDuration(Property.ofValue(Duration.ofSeconds(5)))
            .build()
            .run(runContext);

        assertThat(consumeOutput.getMessagesCount(), is(2));
        List<Map<String, Object>> messages = toMessages(consumeOutput);
        assertThat(messages.size(), is(2));
        assertThat(messages.get(0).get("payload"), is("from-storage-1"));
        assertThat(messages.get(1).get("payload"), is("from-storage-2"));
    }
}
