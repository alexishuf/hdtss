package com.github.lapesd.hdtss.sparql.impl.project;

import com.github.lapesd.hdtss.model.nodes.Op.Type;
import com.github.lapesd.hdtss.sparql.OpExecutor;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Set;

abstract class ProjectExecutor implements OpExecutor {
    private static final @NonNull Set<@NonNull Type> SUPPORTED_TYPES = Set.of(Type.PROJECT);
    protected final @NonNull OpExecutorDispatcher dispatcher;

    protected ProjectExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override public @NonNull Set<Type> supportedTypes() {
        return SUPPORTED_TYPES;
    }
}
