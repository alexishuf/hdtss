package com.github.lapesd.hdtss.model.nodes;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Set;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.vocab.FOAF.ageTerm;
import static com.github.lapesd.hdtss.vocab.FOAF.knowsTerm;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class JoinTest {
    static Stream<Arguments> testInputFilterVarNames() {
        return Stream.of(
        /*  1 */arguments(new Join(new TriplePattern(Alice, knowsTerm, x),
                                   new TriplePattern(x, knowsTerm, y)),
                          Set.of()),
        /*  2 */arguments(new Join(new Filter(new TriplePattern(Alice, ageTerm, x), "?x > 23"),
                                   new TriplePattern(Bob, ageTerm, y)),
                          Set.of()),
        /*  3 */arguments(new Join(new Filter(new TriplePattern(Alice, ageTerm, x), "?x > ?y"),
                                              new TriplePattern(Bob, ageTerm, y)),
                          Set.of()),
        /*  4 */arguments(new Join(new TriplePattern(Alice, ageTerm, x),
                                   new Filter(new TriplePattern(Bob, ageTerm, y), "?y > ?x")),
                          Set.of()),
        /*  5 */arguments(new Join(new Filter(new TriplePattern(Alice, ageTerm, x), "?x > ?z"),
                                   new TriplePattern(Bob, ageTerm, y)),
                          Set.of("z")),
        /*  6 */arguments(new Join(new TriplePattern(Alice, ageTerm, x),
                                new Filter(new TriplePattern(Bob, ageTerm, y), "?y > ?z")),
                          Set.of("z")),
        /*  7 */arguments(new Join(new Filter(new TriplePattern(Alice, ageTerm, x), "?x > ?z"),
                                   new Filter(new TriplePattern(Bob, ageTerm, y), "?y > ?w")),
                          Set.of("z", "w"))
        );
    }

    @ParameterizedTest @MethodSource
    void testInputFilterVarNames(@NonNull Join op, @NonNull Set<@NonNull String> expected) {
        assertEquals(expected, op.inputVars());
    }

}