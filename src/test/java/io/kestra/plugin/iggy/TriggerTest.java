package io.kestra.plugin.iggy;

import io.kestra.core.junit.annotations.EvaluateTrigger;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import jakarta.inject.Inject;
import org.apache.iggy.message.PollingKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.math.BigInteger;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TriggerTest extends IggyTest {
    @Inject
    protected RunContextFactory runContextFactory;

    public TriggerTest(StorageInterface storageInterface) {
        super(storageInterface);
    }

    @BeforeEach
    void setUp(TestInfo testInfo) throws Exception {
        if (testInfo.getTestMethod().map(m -> m.isAnnotationPresent(EvaluateTrigger.class)).orElse(false)) {
            assumeIggyReachable();
            Produce.builder()
                .host(Property.ofValue(DEFAULT_HOST))
                .port(Property.ofValue(DEFAULT_PORT))
                .username(Property.ofValue(DEFAULT_USERNAME))
                .password(Property.ofValue(DEFAULT_PASSWORD))
                .stream(Property.ofValue("test-stream"))
                .topic(Property.ofValue("test-topic"))
                .createIfMissing(Property.ofValue(true))
                .from(Map.of("payload", "hello from trigger flow"))
                .build()
                .run(runContextFactory.of());
        }
    }

    @Test
    void testTriggerProperties() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of("streamVal", "my-stream", "topicVal", "my-topic"));

        Trigger trigger = Trigger.builder()
            .id("iggy-trigger-unit")
            .type(Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            .port(Property.ofValue(8090))
            .username(Property.ofValue("iggy"))
            .password(Property.ofValue("iggy"))
            .stream(Property.ofExpression("{{ streamVal }}"))
            .topic(Property.ofExpression("{{ topicVal }}"))
            .strategy(Property.ofValue(PollingKind.Next))
            .offset(Property.ofValue(BigInteger.ONE))
            .maxRecords(Property.ofValue(10))
            .batchSize(Property.ofValue(20))
            .build();

        assertThat(runContext.render(trigger.getHost()).as(String.class).orElse(null), is("localhost"));
        assertThat(runContext.render(trigger.getPort()).as(Integer.class).orElse(null), is(8090));
        assertThat(runContext.render(trigger.getStream()).as(String.class).orElse(null), is("my-stream"));
        assertThat(runContext.render(trigger.getTopic()).as(String.class).orElse(null), is("my-topic"));
        assertThat(runContext.render(trigger.getStrategy()).as(PollingKind.class).orElse(null), is(PollingKind.Next));
        assertThat(runContext.render(trigger.getOffset()).as(BigInteger.class).orElse(null), is(BigInteger.ONE));
        assertThat(runContext.render(trigger.getMaxRecords()).as(Integer.class).orElse(null), is(10));
        assertThat(runContext.render(trigger.getBatchSize()).as(Integer.class).orElse(null), is(20));
        assertThat(trigger.getInterval(), is(Duration.ofSeconds(60)));

        Trigger defaultTrigger = Trigger.builder()
            .id("default")
            .type(Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            .stream(Property.ofValue("s"))
            .topic(Property.ofValue("t"))
            .build();
        assertThat(runContext.render(defaultTrigger.getBatchSize()).as(Integer.class).orElse(null), is(50));
    }

    @Test
    @EvaluateTrigger(flow = "flows/iggy-listen.yml", triggerId = "watch")
    void testTriggerIntegration(Optional<Execution> optionalExecution) throws Exception {
        assumeIggyReachable();
        assertThat(optionalExecution.isPresent(), is(true));

        Execution execution = optionalExecution.get();

        List<Map<String, Object>> result = toMessages(URI.create((String) execution.getTrigger().getVariables().get("uri")));

        assertThat(execution.getTrigger().getVariables().get("messagesCount"), is(1));
        assertThat(result.size(), is(1));
        assertThat(result.get(0).get("payload"), is("hello from trigger flow"));
    }

    @Test
    void testTriggerKillAndStop() {
        Trigger trigger = Trigger.builder()
            .id("trigger-kill-test")
            .type(Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            .stream(Property.ofValue("stream"))
            .topic(Property.ofValue("topic"))
            .build();

        // Must safely execute without throwing
        trigger.stop();
        trigger.kill();
    }

    @Test
    void testTriggerRejectsAutoCommitFalse() {
        var runContext = runContextFactory.of();
        var conditionContext = ConditionContext.builder().runContext(runContext).build();
        var triggerContext = TriggerContext.builder().build();

        var trigger = Trigger.builder()
            .id("test-reject-autocommit")
            .type(Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            .stream(Property.ofValue("stream"))
            .topic(Property.ofValue("topic"))
            .autoCommit(Property.ofValue(false))
            .build();

        var ex = assertThrows(
            IllegalArgumentException.class,
            () -> trigger.evaluate(conditionContext, triggerContext)
        );
        assertThat(ex.getMessage(), containsString("Trigger only supports autoCommit: true"));
    }

    @Test
    void testTriggerRejectsNonNextStrategy() {
        var runContext = runContextFactory.of();
        var conditionContext = ConditionContext.builder().runContext(runContext).build();
        var triggerContext = TriggerContext.builder().build();

        var trigger = Trigger.builder()
            .id("test-reject-strategy")
            .type(Trigger.class.getName())
            .host(Property.ofValue("localhost"))
            .stream(Property.ofValue("stream"))
            .topic(Property.ofValue("topic"))
            .strategy(Property.ofValue(PollingKind.First))
            .build();

        var ex = assertThrows(
            IllegalArgumentException.class,
            () -> trigger.evaluate(conditionContext, triggerContext)
        );
        assertThat(ex.getMessage(), containsString("Trigger only supports strategy: Next"));
    }
}
