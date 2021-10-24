package com.github.lapesd.hdtss.sparql;

import org.checkerframework.checker.nullness.qual.NonNull;


public class SparqlSyntaxException extends SparqlParserException {
    public SparqlSyntaxException(@NonNull String error, @NonNull String query) {
        super(error, query);
    }
}
