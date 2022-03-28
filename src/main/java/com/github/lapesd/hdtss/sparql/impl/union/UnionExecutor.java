package com.github.lapesd.hdtss.sparql.impl.union;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Op.Type;
import com.github.lapesd.hdtss.sparql.OpExecutor;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.ExecutorUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

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
            implements Function<@Nullable Term @NonNull[], @Nullable Term @NonNull[]> {
        private int @Nullable[] indices;
        private final @NonNull List<@NonNull String> exposedVars;

        public TermsOrder(@NonNull List<@NonNull String> exposedVars) {
            this.indices = null;
            this.exposedVars = exposedVars;
        }

        public @NonNull TermsOrder reset(@NonNull List<@NonNull String> currentVars) {
            if (exposedVars.equals(currentVars))
                indices = null;
            else
                indices = ExecutorUtils.findIndices(exposedVars, currentVars);
            return this;
        }

        @Override public @Nullable Term @NonNull[] apply(@Nullable Term @NonNull[] row) {
            return ExecutorUtils.project(indices, row);
        }
    }
}
