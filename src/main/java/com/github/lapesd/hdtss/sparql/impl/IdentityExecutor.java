package com.github.lapesd.hdtss.sparql.impl;

import com.github.lapesd.hdtss.model.Row;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.Op.Type;
import com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.OpExecutor;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Set;

import static java.util.Collections.emptyList;

@Singleton
@Named("identity")
public class IdentityExecutor implements OpExecutor {
    private static final @NonNull Set<Type> SUPPORTED_TYPES = Set.of(Type.IDENTITY);
    private final BatchQuerySolutions SOLUTIONS =
            new BatchQuerySolutions(emptyList(), Row.SINGLE_EMPTY);

    @Override public @NonNull Set<Type> supportedTypes() {
        return SUPPORTED_TYPES;
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        return SOLUTIONS;
    }
}
