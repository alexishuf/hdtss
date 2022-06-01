package com.github.lapesd.hdtss.sparql.optimizer;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.utils.Binding;
import org.checkerframework.checker.nullness.qual.NonNull;

public interface OptimizerRunner {
    /**
     * Return a plan that produces the same solutions as {@code op}, but faster.
     * @param op the plan to optimize.
     * @return an optimized plan that yields the same solutions.
     */
    @NonNull Op optimize(@NonNull Op op);

    /**
     * Optimize {@code op} using the given {@code binding} to estimate cardinalities.
     *
     * @param op the plan to optimize.
     * @param binding assignment of values to vars to consider when estimating cardinalities.
     * @return an optimized plan for {@code op} without binding the vars mentioned in {@code binding}.
     */
    @NonNull Op optimize(@NonNull Op op, @NonNull Binding binding);
}
