package com.github.lapesd.hdtss.sparql.impl.slice;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.sparql.OpExecutor;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Set;

public abstract class SliceExecutor implements OpExecutor {
    private static final Set<Op.Type> SUPPORTED_TYPES = Set.of(Op.Type.SLICE);
    protected final @NonNull OpExecutorDispatcher dispatcher;

    protected SliceExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override public @NonNull Set<Op.Type> supportedTypes() { return SUPPORTED_TYPES; }
}
