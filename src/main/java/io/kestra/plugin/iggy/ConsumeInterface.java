package io.kestra.plugin.iggy;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import org.apache.iggy.message.PollingKind;

import java.time.Duration;

public interface ConsumeInterface {
    @NotNull
    @Schema(
        title = "Stream identifier or name",
        description = "Name or ID of the Apache Iggy stream to consume messages from."
    )
    @PluginProperty(group = "main")
    Property<String> getStream();

    @NotNull
    @Schema(
        title = "Topic identifier or name",
        description = "Name or ID of the Apache Iggy topic to consume messages from."
    )
    @PluginProperty(group = "main")
    Property<String> getTopic();

    @Schema(
        title = "Partition ID",
        description = "Optional partition ID to consume from."
    )
    @PluginProperty(group = "main")
    Property<Long> getPartitionId();

    @Schema(
        title = "Consumer ID or Name",
        description = "Identifier of the consumer used to track offsets. Defaults to '1'."
    )
    @PluginProperty(group = "main")
    Property<String> getConsumerId();

    @Schema(
        title = "Polling strategy kind",
        description = "Polling strategy: Offset, Timestamp, First, Last, or Next. Defaults to Next."
    )
    @PluginProperty(group = "execution")
    Property<PollingKind> getStrategy();

    @Schema(
        title = "Auto-commit offsets",
        description = "Whether to automatically commit consumer offsets upon polling. Defaults to true."
    )
    @PluginProperty(group = "execution")
    Property<Boolean> getAutoCommit();

    @Schema(
        title = "Maximum records to consume",
        description = "Optional cap on total messages before stopping."
    )
    @PluginProperty(group = "execution")
    Property<Integer> getMaxRecords();

    @Schema(
        title = "Maximum polling duration",
        description = "Maximum duration to poll before stopping execution."
    )
    @PluginProperty(group = "execution")
    Property<Duration> getMaxDuration();

    @Schema(
        title = "Poll interval duration",
        description = "Wait duration between empty poll attempts. Defaults to PT1S."
    )
    @PluginProperty(group = "execution")
    Property<Duration> getPollDuration();

    @Schema(
        title = "Batch size",
        description = "Maximum messages fetched per poll. Defaults to 50."
    )
    @PluginProperty(group = "advanced")
    Property<Integer> getBatchSize();
}
