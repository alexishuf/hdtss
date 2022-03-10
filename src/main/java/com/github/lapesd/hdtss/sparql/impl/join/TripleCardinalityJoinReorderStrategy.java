package com.github.lapesd.hdtss.sparql.impl.join;

import com.github.lapesd.hdtss.data.query.CardinalityEstimator;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

@Singleton
@Requires(property = "sparql.join.reorder", value = "TRIPLE_CARD", defaultValue = "TRIPLE_CARD")
public class TripleCardinalityJoinReorderStrategy extends WeighedJoinReorderStrategy {
    private final CardinalityEstimator estimator;

    public TripleCardinalityJoinReorderStrategy(@NonNull CardinalityEstimator estimator) {
        this.estimator = estimator;
    }

    @Override protected long estimate(Op op) {
        if (op instanceof TriplePattern tp)
            return estimator.estimate(tp);
        long sum = 0;
        for (Op child : op.children())
            sum += estimate(child);
        return sum;
    }
}
