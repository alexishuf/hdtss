package com.github.lapesd.hdtss.model.nodes;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.vocab.FOAF.ageTerm;
import static com.github.lapesd.hdtss.vocab.FOAF.knowsTerm;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class ExistsTest {
    static Stream<Arguments> testEquals() {
        return Stream.of(
                arguments(Exists.create(new TriplePattern(Alice, knowsTerm, x),
                                        new TriplePattern(x, knowsTerm, Alice)),
                          Exists.create(new TriplePattern(Alice, knowsTerm, x),
                                        new TriplePattern(x, knowsTerm, Alice)),
                          true),
                arguments(Exists.create(new TriplePattern(Alice, knowsTerm, x),
                                        new TriplePattern(x, knowsTerm, Alice)),
                          Exists.create(new TriplePattern(x, knowsTerm, Alice),
                                        new TriplePattern(Alice, knowsTerm, x)),
                          false),
                arguments(Exists.create(new TriplePattern(Alice, knowsTerm, x),
                                        new TriplePattern(x, knowsTerm, Alice)),
                          Exists.create(new TriplePattern(Alice, knowsTerm, x),
                                        new TriplePattern(x, ageTerm, Alice)),
                         false),

                arguments(Exists.not(new TriplePattern(Alice, knowsTerm, x),
                                     new TriplePattern(x, ageTerm, y)),
                        Exists.not(new TriplePattern(Alice, knowsTerm, x),
                                     new TriplePattern(x, ageTerm, y)),
                        true),
                arguments(Exists.not(new TriplePattern(Alice, knowsTerm, x),
                                     new TriplePattern(x, ageTerm, y)),
                        Exists.not(new TriplePattern(Bob, knowsTerm, x),
                                     new TriplePattern(x, ageTerm, y)),
                        false),
                arguments(Exists.not(new TriplePattern(Alice, knowsTerm, x),
                                     new TriplePattern(x, ageTerm, y)),
                        Exists.not(new TriplePattern(Alice, knowsTerm, x),
                                     new TriplePattern(y, ageTerm, x)),
                        false),
                arguments(Exists.not(new TriplePattern(Alice, knowsTerm, x),
                                     new TriplePattern(x, ageTerm, y)),
                        new TriplePattern(Alice, knowsTerm, x),
                        false)
        );
    }

    @ParameterizedTest @MethodSource
    public void testEquals(@NonNull Op a, @NonNull Op b, boolean expected) {
        assertEquals(expected, a.toString().equals(b.toString()));
        assertEquals(expected, a.deepEquals(b));
        assertEquals(expected, b.deepEquals(a));
    }

    static Stream<Arguments> testVarNames() {
        return Stream.of(
                arguments(Exists.create(new TriplePattern(Alice, knowsTerm, x),
                                        new TriplePattern(x, knowsTerm, Bob)),
                          List.of("x")),
                arguments(Exists.create(new TriplePattern(Alice, knowsTerm, x),
                                        new TriplePattern(x, knowsTerm, y)),
                          List.of("x")),
                arguments(Exists.create(new TriplePattern(Alice, knowsTerm, Bob),
                                        new TriplePattern(Bob, ageTerm, y)),
                          List.of()),
                arguments(Exists.not(new TriplePattern(Alice, knowsTerm, x),
                                     new TriplePattern(x, ageTerm, y)),
                          List.of("x")),
                arguments(Exists.not(new TriplePattern(Alice, knowsTerm, Bob),
                                     new TriplePattern(Bob, ageTerm, y)),
                          List.of()),
                arguments(Exists.not(new TriplePattern(x, knowsTerm, y),
                                     new TriplePattern(y, knowsTerm, z)),
                          asList("x", "y"))
        );
    }

    @ParameterizedTest @MethodSource
    public void testVarNames(@NonNull Exists exists, @NonNull List<String> expected) {
        assertEquals(expected, exists.outputVars());
    }

    static Stream<Arguments> testInputFilterVarNames() {
        return Stream.of(
        /*  1 */arguments(Exists.create(new TriplePattern(Alice, knowsTerm, x),
                                        new TriplePattern(x, ageTerm, y)),
                          Set.of("y")),
        /*  2 */arguments(Exists.create(new TriplePattern(Alice, knowsTerm, x),
                                        new Filter(new TriplePattern(x, ageTerm, y), "?y > 23")),
                          Set.of("y")),
        /*  3 */arguments(Exists.create(new TriplePattern(Alice, knowsTerm, x),
                                        new Filter(new TriplePattern(x, ageTerm, y), "?y > ?x")),
                          Set.of("y")),
        /*  4 */arguments(Exists.create(new Filter(new TriplePattern(x, ageTerm, y), "?y > ?x"),
                                        new TriplePattern(Alice, knowsTerm, x)),
                          Set.of()),
        /*  5 */arguments(Exists.create(new TriplePattern(Alice, knowsTerm, x),
                                        new Filter(new TriplePattern(x, ageTerm, y), "?y > ?z")),
                          Set.of("y", "z")),
                // bind is directional
        /*  6 */arguments(Exists.create(new Filter(new TriplePattern(Alice, ageTerm, x), "?x > ?y"),
                                        new TriplePattern(Bob, ageTerm, y)),
                          Set.of("y"))
        );
    }

    @ParameterizedTest @MethodSource
    public void testInputFilterVarNames(@NonNull Exists op,
                                        @NonNull Set<@NonNull String> expected) {
        assertEquals(expected, op.inputVars());
    }
}