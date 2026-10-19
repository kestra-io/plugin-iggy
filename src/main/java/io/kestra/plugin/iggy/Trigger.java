package io.kestra.plugin.iggy;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.iggy.message.PollingKind;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on polled Apache Iggy messages",
    description = "Polls an Apache Iggy stream and topic on a schedule (default every 60s) and starts one execution per batch. Only supports strategy: Next and autoCommit: true to prevent duplicate executions across scheduled evaluations."
)
@Plugin(
    examples = {
        @Example(
            title = "Poll an Apache Iggy topic on a schedule and trigger an execution when messages arrive",
            full = true,
            code = """
                id: iggy_trigger
                namespace: company.team
                triggers:
                  - id: watch
                    type: io.kestra.plugin.iggy.Trigger
                    host: iggy.example.com
                    port: 8090
                    username: "{{ secret('IGGY_USERNAME') }}"
                    password: "{{ secret('IGGY_PASSWORD') }}"
                    stream: orders
                    topic: created
                    maxRecords: 10
                tasks:
                  - id: process
                    type: io.kestra.plugin.core.log.Log
                    message: "Consumed {{ trigger.messagesCount }} messages from URI {{ trigger.uri }}"
                """
        )
    }
)
public class Trigger extends AbstractIggyTrigger implements PollingTriggerInterface, TriggerOutput<Consume.Output>, ConsumeInterface {
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

    @Builder.Default
    @Schema(
        title = "Evaluation interval",
        description = "Interval between trigger polling evaluations. Defaults to 60s."
    )
    private final Duration interval = Duration.ofSeconds(60);

    @Getter(AccessLevel.NONE)
    @JsonIgnore
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final transient AtomicReference<Consume> activeConsumeTask = new AtomicReference<>();

    @Getter(AccessLevel.NONE)
    @JsonIgnore
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final transient AtomicBoolean killedOrStopped = new AtomicBoolean(false);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var logger = runContext.logger();

        if (killedOrStopped.get()) {
            logger.debug("Iggy polling trigger id={} received kill()/stop(); skipping poll", this.id);
            return Optional.empty();
        }

        var rAutoCommit = this.autoCommit == null ||
            runContext.render(this.autoCommit).as(Boolean.class).orElse(true);
        if (!rAutoCommit) {
            throw new IllegalArgumentException("Trigger only supports autoCommit: true; disable it only on Consume tasks.");
        }

        var rStrategy = this.strategy != null ?
            runContext.render(this.strategy).as(PollingKind.class).orElse(PollingKind.Next) :
            PollingKind.Next;
        if (rStrategy != PollingKind.Next) {
            throw new IllegalArgumentException("Trigger only supports strategy: Next; use specific offset strategies on Consume tasks.");
        }

        var task = Consume.builder()
            .id(id)
            .type(Consume.class.getName())
            .host(host)
            .port(port)
            .transport(transport)
            .username(username)
            .password(password)
            .token(token)
            .tls(tls)
            .stream(stream)
            .topic(topic)
            .partitionId(partitionId)
            .consumerId(consumerId)
            .strategy(strategy)
            .offset(offset)
            .autoCommit(autoCommit)
            .maxRecords(maxRecords)
            .maxDuration(maxDuration)
            .pollDuration(pollDuration)
            .batchSize(batchSize)
            .build();

        this.activeConsumeTask.set(task);
        if (killedOrStopped.get()) {
            this.activeConsumeTask.compareAndSet(task, null);
            return Optional.empty();
        }

        Consume.Output run;
        try {
            run = task.run(runContext);
        } finally {
            this.activeConsumeTask.compareAndSet(task, null);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Found '{}' messages from stream '{}' topic '{}'",
                run.getMessagesCount(), runContext.render(stream).as(String.class).orElse(""), runContext.render(topic).as(String.class).orElse(""));
        }

        if (run.getMessagesCount() == null || run.getMessagesCount() == 0) {
            return Optional.empty();
        }

        var execution = TriggerService.generateExecution(this, conditionContext, context, run);
        return Optional.of(execution);
    }

    @Override
    public void kill() {
        killedOrStopped.set(true);
        Optional.ofNullable(this.activeConsumeTask.get()).ifPresent(Consume::kill);
    }

    @Override
    public void stop() {
        killedOrStopped.set(true);
        Optional.ofNullable(this.activeConsumeTask.get()).ifPresent(Consume::stop);
    }
}
