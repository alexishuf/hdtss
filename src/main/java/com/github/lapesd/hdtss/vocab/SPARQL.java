package com.github.lapesd.hdtss.vocab;

import io.micronaut.http.MediaType;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;

@SuppressWarnings("unused")
public class SPARQL {
    private static final @NonNull String JSON = "application/sparql-results+json";
    private static final @NonNull String XML  = "application/sparql-results+xml";
    private static final @NonNull String TSV  = "text/tab-separated-values";
    private static final @NonNull String CSV  = "text/csv";
    private static final @NonNull String TSV_U8  = "text/tab-separated-values; charset=utf-8";
    private static final @NonNull String CSV_U8  = "text/csv; charset=utf-8";

    private static final @NonNull MediaType JSON_TYPE = new MediaType("application", "sparql-results+json");
    private static final @NonNull MediaType XML_TYPE = new MediaType("application", "sparql-results+xml");
    private static final @NonNull MediaType TSV_TYPE = new MediaType("text", "tab-separated-values");
    private static final @NonNull MediaType CSV_TYPE = new MediaType("text", "csv");
    private static final @NonNull MediaType TSV_TYPE_U8 = new MediaType("text", "tab-separated-values", Map.of("charset", "utf-8"));
    private static final @NonNull MediaType CSV_TYPE_U8 = new MediaType("text", "csv", Map.of("charset", "utf-8"));

}
