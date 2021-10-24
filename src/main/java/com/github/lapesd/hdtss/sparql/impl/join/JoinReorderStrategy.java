package com.github.lapesd.hdtss.sparql.impl.join;

import com.github.lapesd.hdtss.model.nodes.Join;
import org.checkerframework.checker.nullness.qual.NonNull;

public interface JoinReorderStrategy {
    /**
     * Return a Join operator operands reordered with the goal of making join execution more
     * efficient.
     *
     * @param join the operation to optmize
     *
     * @return a new {@link Join} instance with reordered operands. In case a implementation
     *         performs no change, it may return a new {@link Join} instance or the input
     *         argument {@code join} itself.
     */
    @NonNull Join reorder(@NonNull Join join);
}
