package com.github.lapesd.hdtss.sparql.impl.project;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.Op.Type;
import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import com.github.lapesd.hdtss.sparql.OpExecutor;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
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

    protected static int @NonNull[] findIndices(@NonNull Op node) {
        List<@NonNull String> exposed = node.varNames(), actual = node.children().get(0).varNames();
        int[] indices = new int[exposed.size()];
        for (int i = 0; i < indices.length; i++)
            indices[i] = actual.indexOf(exposed.get(i));
        return indices;
    }

    protected static @NonNull SolutionRow project(@NonNull SolutionRow full, int @NonNull [] indices) {
        Term[] terms = new Term[indices.length];
        for (int i = 0; i < terms.length; i++)
            terms[i] = indices[i] < 0 ? null : full.terms()[indices[i]];
        return new SolutionRow(terms);
    }
}
