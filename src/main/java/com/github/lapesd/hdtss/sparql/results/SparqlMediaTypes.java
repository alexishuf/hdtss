package com.github.lapesd.hdtss.sparql.results;

import io.micronaut.http.MediaType;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class SparqlMediaTypes {
    public static final String QUERY = "application/sparql-query";
    public static final @NonNull MediaType QUERY_TYPE =
            new MediaType(QUERY, ".rq");

    public static final String RESULTS_JSON = "application/sparql-results+json";
    public static final @NonNull MediaType RESULTS_JSON_TYPE =
            new MediaType(RESULTS_JSON, "srj");

    public static final String RESULTS_CSV = "text/csv";
    public static final @NonNull MediaType RESULTS_CSV_TYPE =
            new MediaType(RESULTS_CSV, "csv");
    public static final @NonNull MediaType RESULTS_CSV_U8_TYPE =
            new MediaType(RESULTS_CSV, "csv", Map.of("charset", "utf-8"));

    public static final String RESULTS_TSV = "text/tab-separated-values";
    public static final @NonNull MediaType RESULTS_TSV_TYPE =
            new MediaType(RESULTS_TSV, "tsv");
    public static final @NonNull MediaType RESULTS_TSV_U8_TYPE =
            new MediaType(RESULTS_TSV, "tsv", Map.of("charset", "utf-8"));

    public static final String RESULTS_BAD_TSV = "text/tsv";
    public static final @NonNull MediaType RESULTS_BAD_TSV_TYPE =
            new MediaType(RESULTS_BAD_TSV, "tsv");

    public static final String RESULTS_XML = "application/sparql-results+xml";
    public static final @NonNull MediaType RESULTS_XML_TYPE =
            new MediaType(RESULTS_XML, "srx");

    public static final @NonNull List<MediaType> RESULT_TYPES = Arrays.asList(
            RESULTS_TSV_TYPE,
            RESULTS_JSON_TYPE,
            RESULTS_XML_TYPE,
            RESULTS_CSV_TYPE);

    public static @Nullable MediaType firstResultType(@NonNull Collection<MediaType> collection) {
        for (MediaType request : collection) {
            for (MediaType offer : RESULT_TYPES) {
                if (request.matches(offer))
                    return offer;
            }
        }
        return null;
    }

    private static final @NonNull Map<String, MediaType> NAME_TO_RESULT_TYPES = Map.of(
            "tsv", RESULTS_TSV_TYPE,
            "tab-separated-values", RESULTS_TSV_TYPE,
            "json", RESULTS_JSON_TYPE,
            "sparql-results+json", RESULTS_XML_TYPE,
            "xml", RESULTS_XML_TYPE,
            "sparql-results+xml", RESULTS_JSON_TYPE,
            "csv", RESULTS_CSV_TYPE
    );

    public static @Nullable MediaType resultTypeFromShortName(String... names) {
        for (String name : names) {
            if (name == null)
                continue;
            MediaType mt = NAME_TO_RESULT_TYPES.getOrDefault(name.toLowerCase().strip(), null);
            if (mt != null)
                return mt;
        }
        return null;
    }
}
