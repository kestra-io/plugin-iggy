package io.kestra.plugin.iggy;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface IggyConnectionInterface {
    @Schema(
        title = "Server host",
        description = "Hostname or IP address of the Apache Iggy server."
    )
    @NotNull
    @PluginProperty(group = "connection")
    Property<String> getHost();

    @Schema(
        title = "Server port",
        description = "Port of the Apache Iggy server. Defaults to 8090 for TCP and 3000 for HTTP."
    )
    @NotNull
    @PluginProperty(group = "connection")
    Property<Integer> getPort();

    @Schema(
        title = "Transport protocol",
        description = "Transport protocol to use: TCP (binary protocol, default) or HTTP (REST API)."
    )
    @PluginProperty(group = "connection")
    Property<TransportType> getTransport();

    @Schema(
        title = "Authentication username",
        description = "Username for authentication."
    )
    @PluginProperty(group = "connection")
    Property<String> getUsername();

    @Schema(
        title = "Authentication password",
        description = "Password for authentication."
    )
    @PluginProperty(secret = true, group = "connection")
    Property<String> getPassword();

    @Schema(
        title = "Personal access token",
        description = "Personal access token (PAT) for token-based authentication."
    )
    @PluginProperty(secret = true, group = "connection")
    Property<String> getToken();

    @Schema(
        title = "Use TLS",
        description = "Whether to enable TLS encryption for the connection. Defaults to false."
    )
    @PluginProperty(group = "connection")
    Property<Boolean> getTls();
}
