package com.github.lapesd.hdtss.sparql.optimizer.impl;

import com.github.lapesd.hdtss.data.query.CardinalityEstimator;
import com.github.lapesd.hdtss.model.nodes.Join;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.sparql.optimizer.Optimizer;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.annotation.Order;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

@Singleton
@Order(100)
@Requires(property = "sparql.optimizer.join", pattern = "(?i)t(rue)?|1|on", defaultValue = "true")
@Requires(missingBeans = {FilterAwareJoinOrderOptimizer.class})
public class JoinOrderOptimizer implements Optimizer {
    private final @NonNull JoinOrderHelper helper;

    @Inject public JoinOrderOptimizer(@NonNull CardinalityEstimator estimator) {
        helper = new JoinOrderHelper(estimator);
    }

    @Override public @NonNull Op optimize(@NonNull Op op) {
        if (op.type() == Op.Type.JOIN)
            return helper.reorder((Join) op);
        return OptimizerUtils.optimizeChildren(op, this);
    }
}
