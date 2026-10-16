package io.kestra.plugin.iggy;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import jakarta.inject.Inject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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

    protected List<Map<String, Object>> toMessages(Consume.Output output) throws IOException {
        BufferedReader inputStream = new BufferedReader(
            new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri()))
        );
        List<Map<String, Object>> result = new ArrayList<>();
        FileSerde.reader(inputStream, r -> result.add((Map<String, Object>) r));
        return result;
    }
}
