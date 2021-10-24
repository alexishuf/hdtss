package com.github.lapesd.hdtss.sparql.impl.distinct;

import com.github.lapesd.hdtss.model.nodes.Op.Type;
import com.github.lapesd.hdtss.sparql.DistinctStrategy;
import com.github.lapesd.hdtss.sparql.OpExecutor;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Set;

abstract class DistinctExecutor implements OpExecutor {
    private static final @NonNull Set<Type> SUPPORTED_TYPES = Set.of(Type.DISTINCT);
    protected final @NonNull OpExecutorDispatcher dispatcher;
    protected final @NonNull DistinctStrategy distinctStrategy;

    protected DistinctExecutor(@NonNull OpExecutorDispatcher dispatcher,
                               @NonNull DistinctStrategy distinctStrategy) {
        this.dispatcher = dispatcher;
        this.distinctStrategy = distinctStrategy;
    }

    @Override public @NonNull Set<Type> supportedTypes() {
        return SUPPORTED_TYPES;
    }
}
