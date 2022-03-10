package com.github.lapesd.hdtss.data.query.impl;

import com.github.lapesd.hdtss.data.query.CardinalityEstimator;
import com.github.lapesd.hdtss.model.TermPosition;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

@Singleton
@Requires(property = "hdt.estimator", value = "PATTERN")
public class PatternCardinalityEstimator implements CardinalityEstimator {
    @Override public long estimate(TriplePattern tp) {
        var positions = tp.collectVarsInfo().positions();
        return switch (positions.length) {
            case 0 -> 1;
            case 1 -> switch (positions[0]) {
                case SUB -> 1000;
                case PRE -> 10;
                case OBJ -> 20;
            };
            case 2 -> switch (positions[0]) {
                case SUB -> switch (positions[1]) {
                    case PRE -> 2000 ;
                    case OBJ -> 10000;
                    default -> throw new IllegalArgumentException("unordered collectVarsInfo");
                };
                case PRE -> {
                    if (positions[1] != TermPosition.OBJ)
                        throw new IllegalArgumentException("unordered collectVarsInfo");
                    yield 100;
                }
                default -> throw new IllegalArgumentException("unordered collectVarsInfo()");
            };
            default -> 100000;
        };
    }
}
