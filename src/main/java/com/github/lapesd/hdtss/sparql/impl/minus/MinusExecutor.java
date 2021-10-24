package com.github.lapesd.hdtss.sparql.impl.minus;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.sparql.OpExecutor;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Set;

abstract class MinusExecutor implements OpExecutor {
    private final @NonNull Set<Op.Type> SUPPORTED_TYPES = Set.of(Op.Type.MINUS);

    protected final @NonNull OpExecutorDispatcher dispatcher;
    protected final @NonNull MinusStrategy strategy;

    public MinusExecutor(@NonNull OpExecutorDispatcher dispatcher, @NonNull MinusStrategy strategy) {
        this.dispatcher = dispatcher;
        this.strategy = strategy;
    }

    @Override public @NonNull Set<Op.Type> supportedTypes() {
        return SUPPORTED_TYPES;
    }
}
