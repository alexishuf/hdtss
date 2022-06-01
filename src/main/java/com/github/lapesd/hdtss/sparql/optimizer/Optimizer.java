package com.github.lapesd.hdtss.sparql.optimizer;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.utils.Binding;
import org.checkerframework.checker.nullness.qual.NonNull;

public interface Optimizer {
    /**
     * Find a plan that yields the same solutions as {@code op} but faster.
     *
     * @param op the plan to optimize.
     * @return {@code op} or an equivalent plan that yields the same solutions.
     */
    @NonNull Op optimize(@NonNull Op op);

    /**
     * Optimize {@code op} but replaces vars mentioned in {@code binding}
     * when estimating cardinalities.
     *
     * @param op the plan to optimize.
     * @param binding values to assign to vars in {@code op} when estimating cardinalities.
     * @return {@code op} or an optimized plan that yields the same solutions as {@code op}.
     */
    @NonNull Op optimize(@NonNull Op op, @NonNull Binding binding);
}
