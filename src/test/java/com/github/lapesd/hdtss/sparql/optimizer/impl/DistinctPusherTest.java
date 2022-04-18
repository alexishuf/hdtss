package com.github.lapesd.hdtss.sparql.optimizer.impl;

import com.github.lapesd.hdtss.model.nodes.*;
import com.github.lapesd.hdtss.sparql.optimizer.Optimizer;
import io.micronaut.context.ApplicationContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.vocab.FOAF.ageTerm;
import static com.github.lapesd.hdtss.vocab.FOAF.knowsTerm;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class DistinctPusherTest {
    @SuppressWarnings("unused")
    static Stream<Arguments> test() {
        return Stream.of(
                /* cases where the optimization is not triggered */
                arguments(new TriplePattern(Alice, knowsTerm, x), null),
                arguments(new Project(List.of("x"), new TriplePattern(x, knowsTerm, y)), null),
                arguments(new Join(new TriplePattern(Alice, knowsTerm, x),
                        new TriplePattern(x, knowsTerm, y)), null),
                arguments(new Join(new TriplePattern(Alice, knowsTerm, x),
                        new Union(new TriplePattern(x, knowsTerm, y),
                                new TriplePattern(x, ageTerm, z))), null),
                arguments(new Project(List.of("x"),
                        new Join(new TriplePattern(Alice, knowsTerm, x),
                                new Union(new TriplePattern(x, knowsTerm, y),
                                        new TriplePattern(x, ageTerm, z)))), null),
                /* cases where the optimization is triggered but has no effect */
                arguments(new Ask(new TriplePattern(Alice, knowsTerm, x)), null),
                arguments(new Distinct(new TriplePattern(Alice, knowsTerm, x)), null),
                arguments(new Distinct(new Project(List.of("x"),
                        new TriplePattern(x, knowsTerm, y))), null),
                arguments(new Minus(new TriplePattern(Alice, knowsTerm, x),
                                new TriplePattern(x, ageTerm, y)),
                        new Minus(new TriplePattern(Alice, knowsTerm, x),
                                new TriplePattern(x, ageTerm, y))),
                /* cases where the optimization is triggered and has an effect */
                arguments(new Distinct(new Join(new TriplePattern(Alice, knowsTerm, x),
                                new TriplePattern(x, knowsTerm, y))),
                        new Distinct(new Join(new WeakDistinct(new TriplePattern(Alice, knowsTerm, x)),
                                new WeakDistinct(new TriplePattern(x, knowsTerm, y))))),
                arguments(new Ask(new Union(new TriplePattern(Alice, knowsTerm, x),
                                new Join(new TriplePattern(Alice, knowsTerm, y),
                                        new TriplePattern(y, knowsTerm, x)))),
                        new Ask(new Union(new TriplePattern(Alice, knowsTerm, x),
                                new Join(new WeakDistinct(new TriplePattern(Alice, knowsTerm, y)),
                                        new WeakDistinct(new TriplePattern(y, knowsTerm, x)))))),
                arguments(new Distinct(new Project(List.of("x"),
                                new Filter(new TriplePattern(x, ageTerm, y), "?y > 23"))),
                        new Distinct(new Project(List.of("x"),
                                new Filter(new WeakDistinct(new TriplePattern(x, ageTerm, y)), "?y > 23"))))
        );
    }

    @ParameterizedTest @MethodSource
    void test(Op in, Op expected) {
        Op actual = new DistinctPusher().optimize(in);
        assertEquals((expected == null ? in : expected).toString(), actual.toString());
    }

    @Test
    void testEnabledByDefault() {
        try (var ctx = ApplicationContext.builder().args("-hdt.estimator=PATTERN").start()) {
            Collection<Optimizer> beans = ctx.getBeansOfType(Optimizer.class);
            assertTrue(beans.stream().anyMatch(DistinctPusher.class::isInstance),
                       "DistinctPusher not enabled by default");
        }
    }

    @Test
    void testRunsAfterProjectionPusher() {
        try (var ctx = ApplicationContext.builder().args("-hdt.estimator=PATTERN").start()) {
            var beans = new ArrayList<>(ctx.getBeansOfType(Optimizer.class));
            int distinctIdx = -1, projectionIdx = -1;
            for (int i = 0; i < beans.size(); i++) {
                if (beans.get(i) instanceof DistinctPusher) {
                    assertEquals(-1, distinctIdx);
                    distinctIdx = i;
                } else if (beans.get(i) instanceof ProjectionPusher) {
                    assertEquals(-1, projectionIdx);
                    projectionIdx = i;
                }
            }
            if (projectionIdx >= 0) {
                assertTrue(projectionIdx < distinctIdx,
                           "ProjectionPusher should run before DistinctPusher");
            }
        }
    }
}