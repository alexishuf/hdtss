package com.github.lapesd.hdtss.sparql.impl.join;

import com.github.lapesd.hdtss.data.query.impl.PatternCardinalityEstimator;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

@Singleton
@Requires(property = "sparql.join.reorder", value = "VARS_POS")
public class VarPositionsJoinReorderStrategy extends WeighedJoinReorderStrategy {
    private static final PatternCardinalityEstimator ESTIMATOR = new PatternCardinalityEstimator();

    @Override public long estimate(@NonNull Op op) {
        if (op instanceof TriplePattern tp) {
            return ESTIMATOR.estimate(tp);
        } else {
            int sum = 0;
            for (Op child : op.children())
                sum += estimate(child);
            return sum;
        }
    }
}
