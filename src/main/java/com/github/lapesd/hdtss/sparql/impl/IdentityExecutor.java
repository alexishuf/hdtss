package com.github.lapesd.hdtss.sparql.impl;

import com.github.lapesd.hdtss.model.Row;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.Op.Type;
import com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.OpExecutor;
import com.github.lapesd.hdtss.utils.Binding;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Set;

@Singleton
@Named("identity")
public class IdentityExecutor implements OpExecutor {
    private static final @NonNull Set<Type> SUPPORTED_TYPES = Set.of(Type.IDENTITY);
    private final BatchQuerySolutions SOLUTIONS =
            new BatchQuerySolutions(List.of(), Row.SINGLE_EMPTY);

    @Override public @NonNull Set<Type> supportedTypes() {
        return SUPPORTED_TYPES;
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        return SOLUTIONS;
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node, @Nullable Binding binding) {
        return SOLUTIONS;
    }
}
