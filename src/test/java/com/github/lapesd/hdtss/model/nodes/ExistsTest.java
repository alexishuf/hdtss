package com.github.lapesd.hdtss.model.nodes;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.vocab.FOAF.ageTerm;
import static com.github.lapesd.hdtss.vocab.FOAF.knowsTerm;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class ExistsTest {
    static Stream<Arguments> testEquals() {
        return Stream.of(
                arguments(new Exists(new TriplePattern(Alice, knowsTerm, x),
                                     new TriplePattern(x, knowsTerm, Alice)),
                          new Exists(new TriplePattern(Alice, knowsTerm, x),
                                     new TriplePattern(x, knowsTerm, Alice)),
                          true),
                arguments(new Exists(new TriplePattern(Alice, knowsTerm, x),
                                     new TriplePattern(x, knowsTerm, Alice)),
                          new Exists(new TriplePattern(x, knowsTerm, Alice),
                                     new TriplePattern(Alice, knowsTerm, x)),
                          false),
                arguments(new Exists(new TriplePattern(Alice, knowsTerm, x),
                                     new TriplePattern(x, knowsTerm, Alice)),
                          new Exists(new TriplePattern(Alice, knowsTerm, x),
                                     new TriplePattern(x, ageTerm, Alice)),
                         false)
        );
    }

    @ParameterizedTest @MethodSource
    public void testEquals(@NonNull Op a, @NonNull Op b, boolean expected) {
        assertEquals(expected, a.deepEquals(b));
        assertEquals(expected, b.deepEquals(a));
    }

    static Stream<Arguments> testVarNames() {
        return Stream.of(
                arguments(new Exists(new TriplePattern(Alice, knowsTerm, x),
                                     new TriplePattern(x, knowsTerm, Bob)),
                          List.of("x")),
                arguments(new Exists(new TriplePattern(Alice, knowsTerm, x),
                                new TriplePattern(x, knowsTerm, y)),
                          List.of("x")),
                arguments(new Exists(new TriplePattern(Alice, knowsTerm, Bob),
                                new TriplePattern(Bob, ageTerm, y)),
                          List.of())
        );
    }

    @ParameterizedTest @MethodSource
    public void testVarNames(@NonNull Exists exists, @NonNull List<String> expected) {
        assertEquals(expected, exists.varNames());
    }
}