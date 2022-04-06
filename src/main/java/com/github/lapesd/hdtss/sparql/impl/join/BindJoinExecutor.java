package com.github.lapesd.hdtss.sparql.impl.join;

import com.github.lapesd.hdtss.model.nodes.Join;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.Op.Type;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.OpExecutor;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Set;

abstract class BindJoinExecutor implements OpExecutor {
    private static final @NonNull Set<Type> SUPPORTED_TYPES = Set.of(Type.JOIN, Type.LEFT_JOIN);
    protected final @NonNull OpExecutorDispatcher dispatcher;
    protected final @NonNull JoinReorderStrategy reorderStrategy;

    protected BindJoinExecutor(@NonNull OpExecutorDispatcher dispatcher,
                               @NonNull JoinReorderStrategy reorderStrategy) {
        this.dispatcher = dispatcher;
        this.reorderStrategy = reorderStrategy;
    }

    @Override public @NonNull Set<Type> supportedTypes() {
        return SUPPORTED_TYPES;
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        Type type = node.type();
        if (type != Type.JOIN && type != Type.LEFT_JOIN)
            throw new IllegalArgumentException("Expected JOIN or LEFT_JOIN op");
        List<@NonNull Op> operands = node.children();
        int[] projection = null;
        if (type == Type.JOIN) {
            JoinReorder reorder = reorderStrategy.reorder((Join) node);
            if (reorder != null) {
                operands = reorder.operands();
                projection = reorder.projection();
            }
        }
        return execute(type == Type.LEFT_JOIN, operands, node.outputVars(), projection);
    }

    protected abstract @NonNull QuerySolutions
    execute(boolean isLeft, @NonNull List<@NonNull Op> operands,
            @NonNull List<@NonNull String> varNames, int @Nullable [] projection);
}
