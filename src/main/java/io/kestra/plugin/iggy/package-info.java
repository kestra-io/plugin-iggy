/**
 * This package provides tasks and triggers for integrating with <a href="https://iggy.apache.org/">Apache Iggy</a>,
 * an ultra-fast, persistent, distributed streaming platform written in Rust.
 *
 * <p>Key components included in this package:</p>
 * <ul>
 *   <li>{@link io.kestra.plugin.iggy.Produce} - Publishes messages to an Apache Iggy stream and topic.</li>
 *   <li>{@link io.kestra.plugin.iggy.Consume} - Consumes batches of messages from an Apache Iggy stream and topic,
 *       storing them into Kestra internal storage and tracking consumer offsets.</li>
 *   <li>{@link io.kestra.plugin.iggy.Trigger} - Polling trigger that checks an Apache Iggy topic on a schedule
 *       and initiates workflow executions when new messages are available.</li>
 *   <li>{@link io.kestra.plugin.iggy.RealtimeTrigger} - Real-time streaming trigger that maintains a persistent
 *       connection and emits executions per incoming message.</li>
 * </ul>
 *
 * <p>Authentication is supported via username/password or Personal Access Tokens (PAT). Both TCP (binary)
 * and HTTP transport protocols are supported with optional TLS encryption.</p>
 */
@PluginSubGroup(
    title = "Apache Iggy",
    description = "Tasks and triggers for integrating with the Apache Iggy message streaming platform.",
    categories = { PluginSubGroup.PluginCategory.DATA }
)
package io.kestra.plugin.iggy;

import io.kestra.core.models.annotations.PluginSubGroup;