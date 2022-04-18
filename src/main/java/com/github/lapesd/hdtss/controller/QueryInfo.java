package com.github.lapesd.hdtss.controller;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@Builder @Value @Accessors(fluent = true)
public class QueryInfo {
    long id;
    @NonNull String sparql;
    long rows;
    @Nullable Throwable error;
    boolean cancelled;
    long parseNs;
    long optimizeNs;
    long dispatchNs;
    long totalNs;

    double    parseMs() { return     parseNs/1000000.0; }
    double optimizeMs() { return  optimizeNs/1000000.0; }
    double dispatchMs() { return  dispatchNs/1000000.0; }
    double    totalMs() { return     totalNs/1000000.0; }

    public @NonNull String indentedSparql(int spaces) {
        return sparql.replace("\n", "\n"+" ".repeat(Math.max(0, spaces)));
    }
}
