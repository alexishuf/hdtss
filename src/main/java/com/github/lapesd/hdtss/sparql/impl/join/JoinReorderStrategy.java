package com.github.lapesd.hdtss.sparql.impl.join;

import com.github.lapesd.hdtss.model.nodes.Join;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface JoinReorderStrategy {
    /**
     * Return a Join operator operands reordered with the goal of making join execution more
     * efficient.
     *
     * @param join the operation to optimize
     *
     * @return null if no better reordering was generated or a non-null reordering of
     *         {@code join.children()} bundled with a projection array that gives for the i-th
     *         var in  {@code join.varNames()} where it is in a {@code Join.of(reorder.operands())}.
     */
    @Nullable JoinReorder reorder(@NonNull Join join);
}
