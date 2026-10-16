package io.kestra.plugin.iggy;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
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
import org.apache.iggy.message.PollingKind;
import org.slf4j.Logger;

import java.math.BigInteger;
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
    description = "Polls an Apache Iggy stream and topic on a schedule (default every 60s) and starts one execution per batch."
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
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Consume.Output>, IggyConnectionInterface, ConsumeInterface {
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
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        if (killedOrStopped.get()) {
            logger.debug("Iggy polling trigger id={} received kill()/stop(); skipping poll", this.id);
            return Optional.empty();
        }

        Consume task = Consume.builder()
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

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, run);
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
