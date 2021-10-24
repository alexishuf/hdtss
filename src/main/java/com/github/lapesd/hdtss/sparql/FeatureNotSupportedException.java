package com.github.lapesd.hdtss.sparql;

import org.checkerframework.checker.nullness.qual.NonNull;

public class FeatureNotSupportedException extends SparqlParserException {
    public FeatureNotSupportedException(@NonNull String featureName,
                                        @NonNull String query) {
        super(featureName+" is not supported", query);
    }
}
