package com.github.lapesd.hdtss.data.query.impl;

import com.github.lapesd.hdtss.data.query.CardinalityEstimator;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

@Singleton
@Requires(property = "hdt.estimator", value = "NONE")
public class NoCardinalityEstimator implements CardinalityEstimator {
    @Override public long estimate(TriplePattern triple) {
        return 1;
    }
}
