package com.github.lapesd.hdtss.sparql.impl.triple;

import com.github.lapesd.hdtss.data.query.HdtQueryService;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.sparql.OpExecutor;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Set;

public abstract class TriplePatternExecutor implements OpExecutor {
    private static final @NonNull Set<Op.Type> SUPPORTED_TYPES = Set.of(Op.Type.TRIPLE);
    protected final @NonNull HdtQueryService hdtQueryService;

    public TriplePatternExecutor(@NonNull HdtQueryService hdtQueryService) {
        this.hdtQueryService = hdtQueryService;
    }

    @Override public @NonNull Set<Op.Type> supportedTypes() {
        return SUPPORTED_TYPES;
    }
}
