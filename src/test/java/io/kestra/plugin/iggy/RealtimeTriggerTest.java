package io.kestra.plugin.iggy;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.iggy.message.PollingKind;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest(startRunner = true, startScheduler = true)
class RealtimeTriggerTest extends IggyTest {
    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;
    @Inject
    private LocalFlowRepositoryLoader localFlowRepositoryLoader;
    @Inject
    protected RunContextFactory runContextFactory;

    public RealtimeTriggerTest(StorageInterface storageInterface) {
        super(storageInterface);
    }

    @Test
    void testRealtimeTriggerProperties() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of("streamKey", "realtime-stream", "topicKey", "realtime-topic"));

        RealtimeTrigger trigger = RealtimeTrigger.builder()
            .id("iggy-realtime-unit")
            .type(RealtimeTrigger.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue(8090))
            .username(Property.ofValue("iggy"))
            .password(Property.ofValue("iggy"))
            .stream(Property.ofExpression("{{ streamKey }}"))
            .topic(Property.ofExpression("{{ topicKey }}"))
            .consumerId(Property.ofValue("c-group"))
            .strategy(Property.ofValue(PollingKind.Next))
            .pollDuration(Property.ofValue(Duration.ofMillis(750)))
            .batchSize(Property.ofValue(15))
            .build();

        assertThat(runContext.render(trigger.getHost()).as(String.class).orElse(null), is("localhost"));
        assertThat(runContext.render(trigger.getPort()).as(Integer.class).orElse(null), is(8090));
        assertThat(runContext.render(trigger.getStream()).as(String.class).orElse(null), is("realtime-stream"));
        assertThat(runContext.render(trigger.getTopic()).as(String.class).orElse(null), is("realtime-topic"));
        assertThat(runContext.render(trigger.getConsumerId()).as(String.class).orElse(null), is("c-group"));
        assertThat(runContext.render(trigger.getStrategy()).as(PollingKind.class).orElse(null), is(PollingKind.Next));
        assertThat(runContext.render(trigger.getPollDuration()).as(Duration.class).orElse(null), is(Duration.ofMillis(750)));
        assertThat(runContext.render(trigger.getBatchSize()).as(Integer.class).orElse(null), is(15));

        RealtimeTrigger defaultTrigger = RealtimeTrigger.builder()
            .id("default")
            .type(RealtimeTrigger.class.getName())
            .host(Property.ofValue("localhost"))
            .stream(Property.ofValue("s"))
            .topic(Property.ofValue("t"))
            .build();
        assertThat(runContext.render(defaultTrigger.getBatchSize()).as(Integer.class).orElse(null), is(50));
    }

    @Test
    void testOutputBuilder() {
        RealtimeTrigger.Output output = RealtimeTrigger.Output.builder()
            .payload("test-payload")
            .id("msg-1")
            .offset(BigInteger.valueOf(42))
            .timestamp(Instant.now())
            .headers(Map.of("h1", "v1"))
            .stream("stream-a")
            .topic("topic-b")
            .partitionId(1L)
            .build();

        assertThat(output.getPayload(), is("test-payload"));
        assertThat(output.getId(), is("msg-1"));
        assertThat(output.getOffset(), is(BigInteger.valueOf(42)));
        assertThat(output.getTimestamp(), notNullValue());
        assertThat(output.getHeaders(), hasEntry("h1", "v1"));
        assertThat(output.getStream(), is("stream-a"));
        assertThat(output.getTopic(), is("topic-b"));
        assertThat(output.getPartitionId(), is(1L));
    }

    @Test
    void testRealtimeTriggerIntegration() throws Exception {
        assumeIggyReachable();
        CountDownLatch queueCount = new CountDownLatch(1);
        Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
            queueCount.countDown();
            assertThat(execution.getLeft().getFlowId(), is("iggy-realtime"));
        });

        localFlowRepositoryLoader.load(this.getClass().getClassLoader().getResource("flows/iggy-realtime.yml"));

        Produce.builder()
            .host(Property.ofValue(DEFAULT_HOST))
            .port(Property.ofValue(DEFAULT_PORT))
            .username(Property.ofValue(DEFAULT_USERNAME))
            .password(Property.ofValue(DEFAULT_PASSWORD))
            .stream(Property.ofValue("test-stream-realtime"))
            .topic(Property.ofValue("test-topic-realtime"))
            .createIfMissing(Property.ofValue(true))
            .from(Map.of("payload", "hello realtime trigger"))
            .build()
            .run(runContextFactory.of());

        boolean await = queueCount.await(1, TimeUnit.MINUTES);
        assertThat(await, is(true));

        Map<String, Object> result = receive.blockLast().getTrigger().getVariables();
        assertThat(result.get("payload"), is("hello realtime trigger"));
        assertThat(result.get("stream"), is("test-stream-realtime"));
        assertThat(result.get("topic"), is("test-topic-realtime"));
    }

    @Test
    void testRealtimeTriggerLifecycle() {
        RealtimeTrigger trigger = RealtimeTrigger.builder()
            .id("realtime-lifecycle-test")
            .type(RealtimeTrigger.class.getName())
            .host(Property.ofValue("localhost"))
            .stream(Property.ofValue("stream"))
            .topic(Property.ofValue("topic"))
            .build();

        // Must safely execute without throwing
        trigger.stop();
        trigger.kill();
    }

    @Test
    void testRealtimeTriggerCancellation() throws Exception {
        RunContext runContext = runContextFactory.of();
        RealtimeTrigger trigger = RealtimeTrigger.builder()
            .id("realtime-cancel-test")
            .type(RealtimeTrigger.class.getName())
            .host(Property.ofValue("localhost"))
            .stream(Property.ofValue("stream"))
            .topic(Property.ofValue("topic"))
            .build();

        org.reactivestreams.Publisher<RealtimeTrigger.Output> pub = trigger.publisher(runContext);
        reactor.core.Disposable disposable = Flux.from(pub).subscribe(msg -> {}, err -> {});
        disposable.dispose();
        trigger.stop();
        trigger.kill();
    }

    @Test
    void testCloseClientWithErrorHandling() {
        org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RealtimeTriggerTest.class);
        IggyConnection.closeClient(null, logger);
        IggyConnection.closeClient(null);
    }
}
