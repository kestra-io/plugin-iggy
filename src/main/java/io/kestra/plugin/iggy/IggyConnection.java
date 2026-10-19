package io.kestra.plugin.iggy;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.client.blocking.http.IggyHttpClient;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.slf4j.Logger;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class IggyConnection extends Task implements IggyConnectionInterface {
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

    public static IggyBaseClient createClient(IggyConnectionInterface connection, RunContext runContext) throws Exception {
        var rHost = runContext.render(connection.getHost()).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("host is required: set it or check the expression"));
        var rTransport = connection.getTransport() != null ?
            runContext.render(connection.getTransport()).as(TransportType.class).orElse(TransportType.TCP) : TransportType.TCP;

        var defaultPort = rTransport == TransportType.HTTP ? 3000 : 8090;
        var rPort = connection.getPort() != null ?
            runContext.render(connection.getPort()).as(Integer.class).orElse(defaultPort) : defaultPort;

        var rTls = connection.getTls() != null && runContext.render(connection.getTls()).as(Boolean.class).orElse(false);

        var rUser = connection.getUsername() != null ? runContext.render(connection.getUsername()).as(String.class).orElse(null) : null;
        var rPassword = connection.getPassword() != null ? runContext.render(connection.getPassword()).as(String.class).orElse(null) : null;
        var rToken = connection.getToken() != null ? runContext.render(connection.getToken()).as(String.class).orElse(null) : null;

        if (rTransport == TransportType.TCP) {
            var builder = IggyTcpClient.builder()
                .host(rHost)
                .port(rPort)
                .tls(rTls);

            if (rUser != null && rPassword != null) {
                builder.credentials(rUser, rPassword);
                return builder.buildAndLogin();
            } else {
                var client = builder.build();
                client.connect();
                if (rToken != null) {
                    client.personalAccessTokens().loginWithPersonalAccessToken(rToken);
                }
                return client;
            }
        } else {
            var builder = IggyHttpClient.builder()
                .host(rHost)
                .port(rPort)
                .tls(rTls);

            if (rUser != null && rPassword != null) {
                builder.credentials(rUser, rPassword);
                return builder.buildAndLogin();
            } else {
                var client = builder.build();
                if (rToken != null) {
                    client.personalAccessTokens().loginWithPersonalAccessToken(rToken);
                }
                return client;
            }
        }
    }

    protected IggyBaseClient createClient(RunContext runContext) throws Exception {
        return createClient(this, runContext);
    }

    public static void closeClient(IggyBaseClient client, Logger logger) {
        if (client instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                if (logger != null) {
                    logger.warn("Failed to close Apache Iggy client cleanly: {}", e.getMessage(), e);
                }
            }
        }
    }

    public static void closeClient(IggyBaseClient client) {
        closeClient(client, null);
    }

    public static StreamId resolveStreamId(String stream) {
        if (NumberUtils.isDigits(stream)) {
            return StreamId.of(Long.parseLong(stream));
        }
        return StreamId.of(stream);
    }

    public static TopicId resolveTopicId(String topic) {
        if (NumberUtils.isDigits(topic)) {
            return TopicId.of(Long.parseLong(topic));
        }
        return TopicId.of(topic);
    }
}
