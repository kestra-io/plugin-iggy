package io.kestra.plugin.iggy;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "Transport protocol used to communicate with the Apache Iggy server.")
public enum TransportType {
    @Schema(description = "TCP binary protocol.")
    TCP,

    @Schema(description = "HTTP REST API protocol.")
    HTTP
}
