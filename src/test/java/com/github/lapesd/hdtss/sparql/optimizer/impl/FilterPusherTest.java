package com.github.lapesd.hdtss.sparql.optimizer.impl;

import com.github.lapesd.hdtss.model.nodes.Filter;
import com.github.lapesd.hdtss.model.nodes.Join;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import com.github.lapesd.hdtss.sparql.optimizer.Optimizer;
import io.micronaut.context.ApplicationContext;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.vocab.FOAF.ageTerm;
import static com.github.lapesd.hdtss.vocab.FOAF.knowsTerm;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class FilterPusherTest {

    @SuppressWarnings("unused") static Stream<Arguments> test() {
        return Stream.of(
                // push filter into join
        /*  1 */arguments(new Filter(Join.of(new TriplePattern(Alice, knowsTerm, x),
                                             new TriplePattern(x, ageTerm, y)),
                                     "?y > 23"),
                        Join.of(new TriplePattern(Alice, knowsTerm, x),
                                new Filter(new TriplePattern(x, ageTerm, y), "?y > 23"))),
                // push filter into first join operand
        /*  2 */arguments(new Filter(Join.of(new TriplePattern(x, ageTerm, y),
                                             new TriplePattern(x, knowsTerm, z)),
                                     "?y > 23"),
                          Join.of(new Filter(new TriplePattern(x, ageTerm, y), "?y > 23"),
                                             new TriplePattern(x, knowsTerm, z))),
                // do not push filter with no match
        /*  3 */arguments(new Filter(Join.of(new TriplePattern(Alice, knowsTerm, x),
                                             new TriplePattern(x, ageTerm, y)), "?z > 23"), null),
                // do not push filter with only partial match
        /*  4 */arguments(new Filter(Join.of(new TriplePattern(Alice, knowsTerm, x),
                                             new TriplePattern(x, ageTerm, y)), "?y > ?z"), null),
                // do nothing if both join operands are required
        /*  5 */arguments(new Filter(Join.of(new TriplePattern(x, ageTerm, y),
                                             new TriplePattern(Alice, ageTerm, z)),
                                     "?y > ?z"),
                          null),
                // do nothing if all 3 join operands are required
        /*  6 */arguments(new Filter(Join.of(new TriplePattern(x, ageTerm, y),
                                             new TriplePattern(Alice, ageTerm, z),
                                             new TriplePattern(Bob, ageTerm, w)),
                                     "?y > ?z && ?z > ?w"),
                          null),
                //push filter that needs join of two operands
        /*  7 */arguments(new Filter(Join.of(new TriplePattern(Alice, knowsTerm, x),
                                             new TriplePattern(x, ageTerm, y),
                                             new TriplePattern(Alice, ageTerm, z)), "?y > ?z"),
                        Join.of(new TriplePattern(Alice, knowsTerm, x),
                                new Filter(Join.of(new TriplePattern(x, ageTerm, y),
                                                   new TriplePattern(Alice, ageTerm, z)),
                                           "?y > ?z"))),
        /*  8 */arguments(new Filter(Join.of(new TriplePattern(x, ageTerm, y),
                                             new TriplePattern(Alice, ageTerm, z),
                                             new TriplePattern(Alice, knowsTerm, x)), "?y > ?z"),
                Join.of(new Filter(Join.of(new TriplePattern(x, ageTerm, y),
                                           new TriplePattern(Alice, ageTerm, z)),
                                   "?y > ?z"),
                        new TriplePattern(Alice, knowsTerm, x)))
        );
    }

    @ParameterizedTest @MethodSource
    void test(@NonNull Op in, @Nullable Op expected) {
        boolean same = expected == null;
        if (same)
            expected = in;
        Op actual = new FilterPusher().optimize(in);
        assertEquals(expected.toString(), actual.toString());
        assertTrue(expected.deepEquals(actual));
        assertTrue(actual.deepEquals(expected));
        if (same)
            assertSame(actual, in);
    }

    @Test
    void testRunsAfterFilterAwareJoinOrderOptimizer() {
        try (var ctx = ApplicationContext.builder().args("-hdt.estimator=PATTERN").start()) {
            var list = new ArrayList<>(ctx.getBeansOfType(Optimizer.class));
            int pusherIdx = -1, orderIdx = -1;
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i) instanceof FilterPusher) {
                    assertEquals(-1, pusherIdx, "FilterPusher appears twice");
                    pusherIdx = i;
                }
                if (list.get(i) instanceof FilterAwareJoinOrderOptimizer) {
                    assertEquals(-1, orderIdx, "FilterAwareJoinOrderOptimizer appears twice");
                    orderIdx = i;
                }
            }
            if (pusherIdx > -1)
                assertTrue(orderIdx < pusherIdx, "join order is running after pusher!");
            else
                assertEquals(-1, orderIdx, "Filter pusher is disabled, should not use FilterAwareJoinOrderOptimizer");
        }
    }

    @Test
    void testDisabledByDefault() {
        try (var ctx = ApplicationContext.builder().args("-hdt.estimator=PATTERN").start()) {
            var enabled = ctx.getBeansOfType(Optimizer.class);
            assertTrue(enabled.stream().noneMatch(FilterPusher.class::isInstance),
                       "FilterPusher should be disabled by default");
        }
    }

    @Test
    void testEnable() {
        try (var ctx = ApplicationContext.builder().args("-hdt.estimator=PATTERN",
                                                         "-sparql.optimizer.filter=1").start()) {
            var beans = ctx.getBeansOfType(Optimizer.class);
            assertTrue(beans.stream().anyMatch(FilterPusher.class::isInstance));
        }
    }
}