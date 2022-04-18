package com.github.lapesd.hdtss.sparql.optimizer.impl;

import com.github.lapesd.hdtss.data.query.impl.PatternCardinalityEstimator;
import com.github.lapesd.hdtss.model.nodes.Project;
import com.github.lapesd.hdtss.model.nodes.*;
import com.github.lapesd.hdtss.sparql.optimizer.Optimizer;
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
import static com.github.lapesd.hdtss.vocab.FOAF.*;
import static com.github.lapesd.hdtss.vocab.RDF.typeTerm;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class JoinOrderOptimizerTest {
    @SuppressWarnings("unused") static Stream<Arguments> testReorderWithPattern() {
        return Stream.of(
                // trivial
        /*  1 */arguments(new Join(new TriplePattern(Alice, knowsTerm, Bob)), null),
                // no problem with literals
        /*  2 */arguments(new Join(new TriplePattern(Alice, nameTerm, AliceEN)), null),
                // swap two triples
        /*  3 */arguments(Join.of(new TriplePattern(x, nameTerm, y),
                                  new TriplePattern(x, knowsTerm, Alice)),
                          Join.of(new TriplePattern(x, knowsTerm, Alice),
                                  new TriplePattern(x, nameTerm, y))),
                // swap two triples requiring a projection
        /*  4 */arguments(Join.of(new TriplePattern(x, nameTerm, y),
                                new TriplePattern(y, knowsTerm, Alice)),
                          new Project(List.of("x", "y"),
                                      Join.of(new TriplePattern(y, knowsTerm, Alice),
                                              new TriplePattern(x, nameTerm, y)))),
                // two triples and no change
        /*  5 */arguments(Join.of(new TriplePattern(x, knowsTerm, Alice),
                                  new TriplePattern(x, nameTerm, y)),
                          null),
                // three triples and no change
        /*  6 */arguments(Join.of(new TriplePattern(x, typeTerm, PersonTerm),
                                  new TriplePattern(x, knowsTerm, Bob),
                                  new TriplePattern(x, nameTerm, y)),
                          null),
                // three triples: move first last and do not change other two
        /*  7 */arguments(Join.of(new TriplePattern(x, nameTerm, y),
                                new TriplePattern(x, typeTerm, PersonTerm),
                                new TriplePattern(x, knowsTerm, Bob)),
                          Join.of(new TriplePattern(x, typeTerm, PersonTerm),
                                  new TriplePattern(x, knowsTerm, Bob),
                                  new TriplePattern(x, nameTerm, y))),
                // three triples: move first to last, do not change other two and project
        /*  8 */arguments(Join.of(new TriplePattern(x, nameTerm, y),
                                new TriplePattern(y, typeTerm, PersonTerm),
                                new TriplePattern(y, knowsTerm, Bob)),
                          new Project(List.of("x", "y"),
                                      Join.of(new TriplePattern(y, typeTerm, PersonTerm),
                                              new TriplePattern(y, knowsTerm, Bob),
                                              new TriplePattern(x, nameTerm, y)))),
                // three triples: move first last and do not change other two
        /*  9 */arguments(Join.of(new TriplePattern(x, nameTerm, y),
                                  new TriplePattern(x, knowsTerm, Bob),
                                  new TriplePattern(x, typeTerm, PersonTerm)),
                          Join.of(new TriplePattern(x, knowsTerm, Bob),
                                  new TriplePattern(x, typeTerm, PersonTerm),
                                  new TriplePattern(x, nameTerm, y))),
                // prioritize non-union
        /* 10 */arguments(Join.of(Union.of(new TriplePattern(x, nameTerm, bob),
                                        new TriplePattern(x, nameTerm, roberto)),
                                new TriplePattern(x, knowsTerm, Alice)),
                          Join.of(new TriplePattern(x, knowsTerm, Alice),
                                  Union.of(new TriplePattern(x, nameTerm, bob),
                                          new TriplePattern(x, nameTerm, roberto)))),
                // prioritize non-union: no change
        /* 11 */arguments(Join.of(new TriplePattern(x, knowsTerm, Alice),
                                Union.of(new TriplePattern(x, nameTerm, bob),
                                        new TriplePattern(x, nameTerm, roberto))),
                          null),
                // do not introduce cartesian products
        /* 12 */arguments(Join.of(new TriplePattern(x, knowsTerm, y),
                                new TriplePattern(Alice, knowsTerm, x),
                                new TriplePattern(Bob, knowsTerm, y)),
                          Join.of(new TriplePattern(Alice, knowsTerm, x),
                                  new TriplePattern(x, knowsTerm, y),
                                  new TriplePattern(Bob, knowsTerm, y))),
                // do not introduce cartesian products, requiring projection
        /* 13 */arguments(Join.of(new TriplePattern(x, knowsTerm, y),
                                new TriplePattern(Bob, knowsTerm, y),
                                new TriplePattern(Alice, knowsTerm, x)),
                          new Project(List.of("x", "y"),
                                      Join.of(new TriplePattern(Bob, knowsTerm, y),
                                              new TriplePattern(x, knowsTerm, y),
                                              new TriplePattern(Alice, knowsTerm, x)))),
                //same as above but more triples and no projection
        /* 14 */arguments(Join.of(new TriplePattern(x, knowsTerm, y),
                                new TriplePattern(x, nameTerm, AliceEN),
                                new TriplePattern(y, ageTerm, i23),
                                new TriplePattern(x, ageTerm, i23)),
                          Join.of(new TriplePattern(x, nameTerm, AliceEN),
                                  new TriplePattern(x, ageTerm, i23),
                                  new TriplePattern(x, knowsTerm, y),
                                  new TriplePattern(y, ageTerm, i23))),
                // count all previous variables when preventing cartesian products
        /* 15 */arguments(Join.of(new TriplePattern(x, knowsTerm, y),
                                  new TriplePattern(y, knowsTerm, z),
                                  new TriplePattern(x, knowsTerm, w)),
                          null),
                // do not change LeftJoin
        /* 16 */arguments(new LeftJoin(new TriplePattern(x, knowsTerm, y),
                                       new TriplePattern(y, nameTerm, bob)),
                          null),
        /* 17 */arguments(new LeftJoin(new TriplePattern(x, knowsTerm, y),
                                       new TriplePattern(Bob, knowsTerm, y)),
                          null)
        );
    }

    @ParameterizedTest @MethodSource
    void testReorderWithPattern(@NonNull Join in, @Nullable Op expected) {
        JoinOrderOptimizer optimizer = new JoinOrderOptimizer(new PatternCardinalityEstimator());
        if (expected == null)
            expected = in;
        Op actual = optimizer.optimize(in);
        assertEquals(expected.toString(), actual.toString());
        assertTrue(expected.deepEquals(actual));
        assertTrue(actual.deepEquals(expected));
    }

    static Stream<Arguments> testEnabledByDefaultWhenFilterPusherIsDisabled() {
        return Stream.of(
                arguments("-sparql.optimizer.filter=false"),
                arguments("-sparql.optimizer.filter-join-penalty=0.00")
        );
    }

    @ParameterizedTest @MethodSource
    void testEnabledByDefaultWhenFilterPusherIsDisabled(String arg) {
        try (var ctx = ApplicationContext.builder().args("-hdt.estimator=PATTERN", arg).start()) {
            var beans = ctx.getBeansOfType(Optimizer.class);
            long count = beans.stream().filter(JoinOrderOptimizer.class::isInstance).count();
            long filterCount = beans.stream()
                    .filter(FilterAwareJoinOrderOptimizer.class::isInstance).count();
            assertEquals(1, count, "JoinOrderOptimizer disabled");
            assertEquals(0, filterCount, "FilterAwareJoinOrderOptimizer remains enabled");
        }
    }

    @Test
    void testDisabledWhenFilterPusherIsEnabled() {
        try (var ctx = ApplicationContext.builder().args("-hdt.estimator=PATTERN",
                                                         "-sparql.optimizer.filter=true").start()) {
            var beans = ctx.getBeansOfType(Optimizer.class);
            assertTrue(beans.stream().anyMatch(FilterPusher.class::isInstance));
            assertFalse(beans.stream().anyMatch(JoinOrderOptimizer.class::isInstance));
            assertTrue(beans.stream().anyMatch(FilterAwareJoinOrderOptimizer.class::isInstance));
        }
    }
}