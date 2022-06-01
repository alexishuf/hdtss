package com.github.lapesd.hdtss.sparql.optimizer.impl;

import com.github.lapesd.hdtss.data.query.impl.PatternCardinalityEstimator;
import com.github.lapesd.hdtss.model.nodes.*;
import com.github.lapesd.hdtss.sparql.optimizer.Optimizer;
import com.github.lapesd.hdtss.utils.Binding;
import io.micronaut.context.ApplicationContext;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.vocab.FOAF.ageTerm;
import static com.github.lapesd.hdtss.vocab.FOAF.knowsTerm;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class FilterAwareJoinOrderOptimizerTest {
    private static final JoinOrderHelper helper = new JoinOrderHelper(new PatternCardinalityEstimator());
    private static final FilterAwareJoinOrderOptimizer optimizer
            = new FilterAwareJoinOrderOptimizer(new PatternCardinalityEstimator(), 20);

    private static long cost(Op op) {
        return helper.estimate(op, Binding.EMPTY);
    }

    @SuppressWarnings("unused") static Stream<Arguments> testJoin() {
        return Stream.of(
                /* trivial cases */
        /*  1 */arguments(Join.of(new TriplePattern(x, knowsTerm, y)), null),
        /*  2 */arguments(Join.of(new TriplePattern(Alice, knowsTerm, x)), null),
        /*  3 */arguments(Join.of(new TriplePattern(Alice, ageTerm, i23)), null),
        /*  4 */arguments(Join.of(new TriplePattern(x, ageTerm, i23)), null),
        /*  5 */arguments(Join.of(new TriplePattern(Alice, y, i23)), null),
                /* no effect */
        /*  6 */arguments(Join.of(new TriplePattern(Alice, knowsTerm, x),
                                  new TriplePattern(x, knowsTerm, y)), null),
        /*  7 */arguments(Join.of(new TriplePattern(Alice, knowsTerm, x),
                                  new TriplePattern(x, ageTerm, i23)), null),
                // reverse two operands
        /*  8 */arguments(Join.of(new TriplePattern(x, knowsTerm, y),
                                  new TriplePattern(Alice, knowsTerm, x)),
                          Join.of(new TriplePattern(Alice, knowsTerm, x),
                                  new TriplePattern(x, knowsTerm, y))),
                // reverse 2 operands and project
        /*  9 */arguments(Join.of(new TriplePattern(y, knowsTerm, x),
                                  new TriplePattern(x, knowsTerm, Bob)),
                          new Project(List.of("y", "x"),
                                      Join.of(new TriplePattern(x, knowsTerm, Bob),
                                              new TriplePattern(y, knowsTerm, x)))),
                // reorder 3 operands and project
        /* 10 */arguments(Join.of(new TriplePattern(x, knowsTerm, y),
                                  new Union(new TriplePattern(Alice, knowsTerm, y),
                                            new TriplePattern(Bob, knowsTerm, y)),
                                  new TriplePattern(Charlie, knowsTerm, y)),
                          new Project(List.of("x", "y"),
                                      Join.of(new TriplePattern(Charlie, knowsTerm, y),
                                              new Union(new TriplePattern(Alice, knowsTerm, y),
                                                        new TriplePattern(Bob, knowsTerm, y)),
                                              new TriplePattern(x, knowsTerm, y))))
        );
    }

    @ParameterizedTest @MethodSource
    void testJoin(@NonNull Op in, @Nullable Op expected) {
        doTest(in, expected);
    }

    @SuppressWarnings("unused") static Stream<Arguments> testLeftJoin() {
        return testJoin()
                .filter(a -> ((Op)a.get()[0]).children().size() == 2)
                .map(a -> arguments(new LeftJoin(((Op)a.get()[0]).children())));
    }
    @ParameterizedTest @MethodSource
    void testLeftJoin(@NonNull Op in) {
        doTest(in, null);
    }

    @SuppressWarnings("unused") static Stream<Arguments> testWithFilters() {
        return Stream.of(
                // penalty for first triple does not overcome cost of second
        /*  1 */arguments(new Filter(Join.of(new TriplePattern(Alice, knowsTerm, x),
                                             new TriplePattern(x, ageTerm, y)),
                                     "?y > 23"),
                          null),
                // penalty causes reorder
        /*  2 */arguments(new Filter(Join.of(new TriplePattern(x, knowsTerm, y),
                                             new TriplePattern(y, ageTerm, z)),
                                     "?z > 23"),
                          new Filter(new Project(List.of("x", "y", "z"),
                                                 Join.of(new TriplePattern(y, ageTerm, z),
                                                         new TriplePattern(x, knowsTerm, y))),
                                     "?z > 23")),
                // no penalty (no match)
        /*  3 */arguments(new Filter(Join.of(new TriplePattern(x, knowsTerm, y),
                                             new TriplePattern(y, ageTerm, z)), "?w > 23"), null),
                // no penalty (partial match)
        /*  4 */arguments(new Filter(Join.of(new TriplePattern(x, knowsTerm, y),
                                             new TriplePattern(y, ageTerm, z)), "?z > ?w"), null),
                //penalize only the first triple
        /*  5 */arguments(new Filter(Join.of(new TriplePattern(x, knowsTerm, y),
                                             new TriplePattern(y, ageTerm, z),
                                             new TriplePattern(x, ageTerm, w)),
                                     "?z > ?w"),
                        new Filter(new Project(List.of("x", "y", "z", "w"),
                                               Join.of(new TriplePattern(y, ageTerm, z),
                                                       new TriplePattern(x, knowsTerm, y),
                                                       new TriplePattern(x, ageTerm, w))),
                                   "?z > ?w"))
        );
    }

    @ParameterizedTest @MethodSource
    void testWithFilters(@NonNull Op in, @Nullable Op expected) {
        doTest(in, expected);
    }

    private void doTest(@NonNull Op in, @Nullable Op expected) {
        Op actual = optimizer.optimize(in);
        boolean same = expected == null;
        if (same)
            expected = in;
        assertEquals(expected.toString(), actual.toString());
        assertTrue(expected.deepEquals(actual));
        assertTrue(actual.deepEquals(expected));
        if (same)
            assertSame(in, actual);
    }

    @Test
    void testDisabledByDefault() {
        try (var ctx = ApplicationContext.builder().args("-hdt.estimator=PATTERN").start()) {
            var beans = ctx.getBeansOfType(Optimizer.class);
            long count = beans.stream()
                    .filter(FilterAwareJoinOrderOptimizer.class::isInstance).count();
            assertEquals(0, count, "Expected 0 FilterAwareJoinOrderOptimizer, got "+count);
            assertTrue(beans.stream().anyMatch(JoinOrderOptimizer.class::isInstance),
                       "Plain JoinOrderOptimizer should be enabled");
        }
    }
}