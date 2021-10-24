package com.github.lapesd.hdtss.sparql.impl.union;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Op.Type;
import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import com.github.lapesd.hdtss.sparql.OpExecutor;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

abstract class UnionExecutor implements OpExecutor {
    private static final @NonNull Set<Type> SUPPORTED_TYPES = Set.of(Type.UNION);
    protected final @NonNull OpExecutorDispatcher dispatcher;

    protected UnionExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override public @NonNull Set<Type> supportedTypes() {
        return SUPPORTED_TYPES;
    }

    protected static final class TermsOrder
            implements Function<@NonNull SolutionRow, @NonNull SolutionRow> {
        private final int[] indices;
        private final @NonNull List<@NonNull String> exposedVars;
        private boolean passThrough = false;

        public TermsOrder(@NonNull List<@NonNull String> exposedVars) {
            this.indices = new int[exposedVars.size()];
            this.exposedVars = exposedVars;
        }

        public @NonNull TermsOrder reset(@NonNull List<@NonNull String> currentVars) {
            passThrough = true;
            for (int i = 0; i < indices.length; i++) {
                int idx = currentVars.indexOf(exposedVars.get(i));
                indices[i] = idx;
                if (idx != i)
                    passThrough = false;
            }
            return this;
        }

        @Override public @NonNull SolutionRow apply(@NonNull SolutionRow row) {
            if (passThrough)
                return row;
            Term[] reordered = new Term[indices.length], terms = row.terms();
            for (int i = 0; i < indices.length; i++) {
                reordered[i] = indices[i] < 0 ? null : terms[indices[i]];
            }
            return new SolutionRow(reordered);
        }
    }
}
