package com.github.lapesd.hdtss.sparql.impl.distinct;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.sparql.OpExecutor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Set;

public abstract class WeakDistinctExecutor implements OpExecutor {
    private static final @NonNull Set<Op.Type> SUPPORTED_TYPES = Set.of(Op.Type.WEAK_DISTINCT);

    @Override public @NonNull Set<Op.Type> supportedTypes() {
        return SUPPORTED_TYPES;
    }
}
