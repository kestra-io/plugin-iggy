package io.kestra.plugin.iggy;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.iggy.message.PollingKind;

import java.math.BigInteger;
import java.time.Duration;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractIggyTrigger extends AbstractTrigger implements IggyConnectionInterface {
    @NotNull
    @Schema(
        title = "Server host",
        description = "Hostname or IP address of the Apache Iggy server."
    )
    @PluginProperty(group = "connection")
    protected Property<String> host;

    @Schema(
        title = "Server port",
        description = "Port of the Apache Iggy server. Defaults to 8090 for TCP and 3000 for HTTP."
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    protected Property<Integer> port = Property.ofValue(8090);

    @Schema(
        title = "Transport protocol",
        description = "Transport protocol to use: TCP (default) or HTTP."
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    protected Property<TransportType> transport = Property.ofValue(TransportType.TCP);

    @Schema(
        title = "Authentication username",
        description = "Username for authentication."
    )
    @PluginProperty(group = "connection")
    protected Property<String> username;

    @ToString.Exclude
    @Schema(
        title = "Authentication password",
        description = "Password for authentication."
    )
    @PluginProperty(secret = true, group = "connection")
    protected Property<String> password;

    @ToString.Exclude
    @Schema(
        title = "Personal access token",
        description = "Personal access token (PAT) for token-based authentication."
    )
    @PluginProperty(secret = true, group = "connection")
    protected Property<String> token;

    @Schema(
        title = "Use TLS",
        description = "Whether to enable TLS encryption for the connection. Defaults to false."
    )
    @Builder.Default
    @PluginProperty(group = "connection")
    protected Property<Boolean> tls = Property.ofValue(false);

    @NotNull
    @Schema(
        title = "Stream identifier or name",
        description = "Name or ID of the Apache Iggy stream to consume messages from."
    )
    @PluginProperty(group = "main")
    protected Property<String> stream;

    @NotNull
    @Schema(
        title = "Topic identifier or name",
        description = "Name or ID of the Apache Iggy topic to consume messages from."
    )
    @PluginProperty(group = "main")
    protected Property<String> topic;

    @Schema(
        title = "Partition ID",
        description = "Optional partition ID to consume from."
    )
    @PluginProperty(group = "main")
    protected Property<Long> partitionId;

    @Schema(
        title = "Consumer ID or Name",
        description = "Identifier of the consumer used to track offsets. Defaults to '1'."
    )
    @Builder.Default
    @PluginProperty(group = "main")
    protected Property<String> consumerId = Property.ofValue("1");

    @Schema(
        title = "Polling strategy kind",
        description = "Polling strategy: Offset, Timestamp, First, Last, or Next. Defaults to Next."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<PollingKind> strategy = Property.ofValue(PollingKind.Next);

    @Schema(
        title = "Offset or Timestamp value",
        description = "Starting offset or timestamp when strategy is Offset or Timestamp."
    )
    @PluginProperty(group = "execution")
    protected Property<BigInteger> offset;

    @Schema(
        title = "Auto-commit offsets",
        description = "Whether to automatically commit the consumer offset of processed messages. Defaults to true."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<Boolean> autoCommit = Property.ofValue(true);

    @Schema(
        title = "Poll interval duration",
        description = "Wait duration between empty poll attempts. Defaults to PT1S."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<Duration> pollDuration = Property.ofValue(Duration.ofSeconds(1));

    @Schema(
        title = "Batch size",
        description = "Maximum messages fetched per poll. Must be greater than or equal to 1. Defaults to 50."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<Integer> batchSize = Property.ofValue(50);
}
