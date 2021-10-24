package com.github.lapesd.hdtss.sparql;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.stream.Collectors;

@EqualsAndHashCode(callSuper = true) @Accessors(fluent = true)
public class SparqlParserException extends RuntimeException {
    @Getter private final @NonNull String reason, query;

    public SparqlParserException(@NonNull String reason, @NonNull String query) {
        super(String.format("%s. Query:\n%s", reason,
                Arrays.stream(query.split("\n"))
                      .map(s -> "  "+s).collect(Collectors.joining("\n"))));
        this.reason = reason;
        this.query = query;
    }
}
