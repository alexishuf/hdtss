package com.github.lapesd.hdtss.sparql.optimizer.impl;

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

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.vocab.FOAF.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class ProjectionPusherTest {

    @SuppressWarnings("unused")
    static Stream<Arguments> test() {
        return Stream.of(
        /*  1 */arguments(new TriplePattern(Alice, knowsTerm, x), null),
                // Project is safe, but useless
        /*  2 */arguments(new Ask(new TriplePattern(Alice, knowsTerm, x)), null),
                // Project leaves
        /*  3 */arguments(new Ask(new Union(new TriplePattern(Alice, knowsTerm, x),
                                            new TriplePattern(Bob, knowsTerm, x))),
                          new Ask(new Union(new Project(List.of(), new TriplePattern(Alice, knowsTerm, x)),
                                            new Project(List.of(), new TriplePattern(Bob, knowsTerm, x))))),
                // Do not project Slice because Ask already does it (see test case above)
                // Also do not project Union because projecting children is enough
        /*  4 */arguments(new Ask(new Slice(new Union(new TriplePattern(Alice, knowsTerm, x),
                                                      new TriplePattern(Bob, knowsTerm, x)),
                                            Long.MAX_VALUE, 1)),
                          new Ask(new Slice(new Union(new Project(List.of(), new TriplePattern(Alice, knowsTerm, x)),
                                                      new Project(List.of(), new TriplePattern(Bob, knowsTerm, x))),
                                            Long.MAX_VALUE, 1))),
                // no project found
        /*  5 */arguments(new TriplePattern(x, ageTerm, y), null),
        /*  6 */arguments(new Join(new TriplePattern(x, knowsTerm, y), new TriplePattern(y, ageTerm, z)), null),
        /*  7 */arguments(new Join(new Union(new TriplePattern(Alice, knowsTerm, y),
                                             new TriplePattern(Bob, knowsTerm, y)),
                                   new TriplePattern(y, ageTerm, z)),
                          null),
                // no-op project
        /*  8 */arguments(new Project(List.of("x", "y"), new TriplePattern(x, ageTerm, y)), null),
                // project introduces null var
        /*  9 */arguments(new Project(List.of("y", "z"), new TriplePattern(x, ageTerm, y)), null),
                // has project but no work to do
        /* 10 */arguments(new Project(List.of("y"), new TriplePattern(x, ageTerm, y)), null),
        /* 11 */arguments(new Project(List.of("y", "x"), new TriplePattern(x, ageTerm, y)), null),
                // test join vars are preserved (2 operands)
        /* 12 */arguments(new Project(List.of("x"), new Join(new TriplePattern(x, knowsTerm, y),
                                                             new TriplePattern(y, ageTerm, i23))),
                          null),
                // test join vars are preserved (left join)
        /* 13 */arguments(new Project(List.of("x", "z"), new LeftJoin(new TriplePattern(x, knowsTerm, y),
                                                                      new TriplePattern(y, ageTerm, z))),
                          null),
        /* 14 */arguments(new Project(List.of("z"), new LeftJoin(new TriplePattern(x, knowsTerm, y),
                                                                 new TriplePattern(y, ageTerm, z))),
                          new Project(List.of("z"), new LeftJoin(new Project(List.of("y"), new TriplePattern(x, knowsTerm, y)),
                                                                 new TriplePattern(y, ageTerm, z)))),
                // test join vars are preserved (3 operands)
        /* 15 */arguments(new Project(List.of("x", "z", "w"),
                                      new Join(new TriplePattern(x, knowsTerm, y),
                                               new TriplePattern(y, nameTerm, z),
                                               new TriplePattern(y, ageTerm, w))),
                          null),
        /* 16 */arguments(new Project(List.of("z", "w"),
                                       new Join(new TriplePattern(x, knowsTerm, y),
                                                new TriplePattern(y, nameTerm, z),
                                                new TriplePattern(y, ageTerm, w))),
                        new Project(List.of("z", "w"),
                                    new Join(new Project(List.of("y"), new TriplePattern(x, knowsTerm, y)),
                                             new TriplePattern(y, nameTerm, z),
                                             new TriplePattern(y, ageTerm, w)))),
        /* 17 */arguments(new Project(List.of("z"),
                                      new Join(new TriplePattern(x, knowsTerm, y),
                                               new TriplePattern(y, nameTerm, z),
                                               new TriplePattern(y, ageTerm, w))),
                          new Project(List.of("z"),
                                      new Join(new Project(List.of("y"), new TriplePattern(x, knowsTerm, y)),
                                               new TriplePattern(y, nameTerm, z),
                                               new Project(List.of("y"), new TriplePattern(y, ageTerm, w)))))
        );
    }

    @ParameterizedTest @MethodSource
    void test(@NonNull Op in, @Nullable Op expected) {
        expected = expected == null ? in : expected;
        Op actual = new ProjectionPusher().optimize(in);
        assertEquals(expected.toString(), actual.toString());
        assertTrue(expected.deepEquals(actual));
        assertTrue(actual.deepEquals(expected));
    }

    @Test
    void testEnabledByDefault() {
        try (var ctx = ApplicationContext.builder().args("-hdt.estimator=PATTERN").start()) {
            Collection<Optimizer> candidates = ctx.getBeansOfType(Optimizer.class);
            assertTrue(candidates.stream().anyMatch(ProjectionPusher.class::isInstance),
                       "ProjectionPusher disabled");
        }
    }

}