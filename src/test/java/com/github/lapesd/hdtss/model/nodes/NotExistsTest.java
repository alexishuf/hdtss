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
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class NotExistsTest {
    static Stream<Arguments> testEquals() {
        return Stream.of(
                arguments(new NotExists(new TriplePattern(Alice, knowsTerm, x),
                                        new TriplePattern(x, ageTerm, y)),
                          new NotExists(new TriplePattern(Alice, knowsTerm, x),
                                        new TriplePattern(x, ageTerm, y)),
                          true),
                arguments(new NotExists(new TriplePattern(Alice, knowsTerm, x),
                                        new TriplePattern(x, ageTerm, y)),
                          new NotExists(new TriplePattern(Bob, knowsTerm, x),
                                        new TriplePattern(x, ageTerm, y)),
                          false),
                arguments(new NotExists(new TriplePattern(Alice, knowsTerm, x),
                                        new TriplePattern(x, ageTerm, y)),
                          new NotExists(new TriplePattern(Alice, knowsTerm, x),
                                        new TriplePattern(y, ageTerm, x)),
                          false),
                arguments(new NotExists(new TriplePattern(Alice, knowsTerm, x),
                                        new TriplePattern(x, ageTerm, y)),
                          new TriplePattern(Alice, knowsTerm, x),
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
                arguments(new NotExists(new TriplePattern(Alice, knowsTerm, x),
                                        new TriplePattern(x, ageTerm, y)),
                          List.of("x")),
                arguments(new NotExists(new TriplePattern(Alice, knowsTerm, Bob),
                                new TriplePattern(Bob, ageTerm, y)),
                        List.of()),
                arguments(new NotExists(new TriplePattern(x, knowsTerm, y),
                                        new TriplePattern(y, knowsTerm, z)),
                        asList("x", "y"))
        );
    }

    @ParameterizedTest @MethodSource
    public void testVarNames(@NonNull NotExists notExists, @NonNull List<String> expected) {
        assertEquals(expected, notExists.varNames());
    }

}