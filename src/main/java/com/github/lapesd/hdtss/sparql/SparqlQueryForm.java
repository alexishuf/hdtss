package com.github.lapesd.hdtss.sparql;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.core.annotation.Nullable;
import org.checkerframework.checker.nullness.qual.NonNull;

@Introspected
public record SparqlQueryForm(@NonNull String query,
                              @JsonProperty("default-graph-uri") @Nullable String defaultGraphUri,
                              @JsonProperty("named-graph-uri") @Nullable String namedGraphUri) {

    public SparqlQueryForm(@NonNull String query) {
        this(query, null, null);
    }
}
