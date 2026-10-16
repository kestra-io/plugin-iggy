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
import org.apache.iggy.client.blocking.http.IggyHttpClientBuilder;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.client.blocking.tcp.IggyTcpClientBuilder;
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
        String hostStr = runContext.render(connection.getHost()).as(String.class).orElseThrow();
        TransportType transportType = connection.getTransport() != null ?
            runContext.render(connection.getTransport()).as(TransportType.class).orElse(TransportType.TCP) : TransportType.TCP;

        int defaultPort = transportType == TransportType.HTTP ? 3000 : 8090;
        int portInt = connection.getPort() != null ?
            runContext.render(connection.getPort()).as(Integer.class).orElse(defaultPort) : defaultPort;

        boolean tlsBool = connection.getTls() != null && runContext.render(connection.getTls()).as(Boolean.class).orElse(false);

        String userStr = connection.getUsername() != null ? runContext.render(connection.getUsername()).as(String.class).orElse(null) : null;
        String passStr = connection.getPassword() != null ? runContext.render(connection.getPassword()).as(String.class).orElse(null) : null;
        String tokenStr = connection.getToken() != null ? runContext.render(connection.getToken()).as(String.class).orElse(null) : null;

        if (transportType == TransportType.TCP) {
            IggyTcpClientBuilder builder = IggyTcpClient.builder()
                .host(hostStr)
                .port(portInt)
                .tls(tlsBool);

            if (userStr != null && passStr != null) {
                builder.credentials(userStr, passStr);
                return builder.buildAndLogin();
            } else {
                IggyTcpClient client = builder.build();
                client.connect();
                if (tokenStr != null) {
                    client.personalAccessTokens().loginWithPersonalAccessToken(tokenStr);
                }
                return client;
            }
        } else {
            IggyHttpClientBuilder builder = IggyHttpClient.builder()
                .host(hostStr)
                .port(portInt)
                .tls(tlsBool);

            if (userStr != null && passStr != null) {
                builder.credentials(userStr, passStr);
                return builder.buildAndLogin();
            } else {
                IggyHttpClient client = builder.build();
                if (tokenStr != null) {
                    client.personalAccessTokens().loginWithPersonalAccessToken(tokenStr);
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
