package com.github.lapesd.hdtss.sparql.impl.limit;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.sparql.OpExecutor;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Set;

abstract class LimitExecutor implements OpExecutor {
    private static final @NonNull Set<Op.Type> SUPPORTED_TYPES = Set.of(Op.Type.LIMIT);
    protected final @NonNull OpExecutorDispatcher dispatcher;

    public LimitExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override public @NonNull Set<Op.Type> supportedTypes() {
        return SUPPORTED_TYPES;
    }
}
