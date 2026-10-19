package io.kestra.plugin.iggy;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assumptions;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@KestraTest
public class IggyTest {
    protected final StorageInterface storageInterface;

    public static final String DEFAULT_HOST = Optional.ofNullable(System.getenv("IGGY_HOST"))
        .orElse(System.getProperty("iggy.host", "localhost"));
    public static final int DEFAULT_PORT = Optional.ofNullable(System.getenv("IGGY_PORT"))
        .map(Integer::parseInt)
        .orElseGet(() -> Integer.getInteger("iggy.port", 8090));
    public static final String DEFAULT_USERNAME = Optional.ofNullable(System.getenv("IGGY_USERNAME"))
        .orElse(System.getProperty("iggy.username", "iggy"));
    public static final String DEFAULT_PASSWORD = Optional.ofNullable(System.getenv("IGGY_PASSWORD"))
        .orElse(System.getProperty("iggy.password", "iggy"));

    @Inject
    public IggyTest(StorageInterface storageInterface) {
        this.storageInterface = storageInterface;
    }

    public static boolean isIggyReachable() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(DEFAULT_HOST, DEFAULT_PORT), 500);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static void assumeIggyReachable() {
        Assumptions.assumeTrue(isIggyReachable(), "Apache Iggy broker is not reachable at " + DEFAULT_HOST + ":" + DEFAULT_PORT + "; skipping integration test");
    }

    protected List<Map<String, Object>> toMessages(URI uri) throws IOException {
        List<Map<String, Object>> result = new ArrayList<>();
        try (InputStream inputStream = storageInterface.get(TenantService.MAIN_TENANT, null, uri)) {
            FileSerde.read(inputStream, r -> result.add((Map<String, Object>) r));
        }
        return result;
    }

    protected List<Map<String, Object>> toMessages(Consume.Output output) throws IOException {
        return toMessages(output.getUri());
    }
}
