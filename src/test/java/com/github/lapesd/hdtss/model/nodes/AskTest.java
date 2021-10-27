package com.github.lapesd.hdtss.model.nodes;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.vocab.FOAF.knowsTerm;
import static com.github.lapesd.hdtss.vocab.FOAF.nameTerm;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class AskTest {
    static Stream<Arguments> testEquals() {
        return Stream.of(
                arguments(new Ask(new TriplePattern(Alice, knowsTerm, x)),
                          new Ask(new TriplePattern(Alice, knowsTerm, x)), true),
                arguments(new Ask(new TriplePattern(Alice, knowsTerm, x)),
                          new Ask(new TriplePattern(Alice, knowsTerm, y)), false),
                arguments(new Ask(new TriplePattern(Alice, knowsTerm, x)),
                          new Ask(new TriplePattern(x, knowsTerm, Alice)), false),
                arguments(new Ask(new TriplePattern(Alice, knowsTerm, x)),
                                  new TriplePattern(Alice, knowsTerm, x), false)
        );
    }

    @ParameterizedTest @MethodSource
    void testEquals(@NonNull Op a, @NonNull Op b, boolean expected) {
        assertEquals(expected, a.deepEquals(b));
        assertEquals(expected, b.deepEquals(a));
    }

    static Stream<Arguments> testWithChildren() {
        return Stream.of(
                arguments(new Ask(new TriplePattern(Alice, knowsTerm, x)),
                          List.of(new TriplePattern(Bob, knowsTerm, x)),
                          new Ask(new TriplePattern(Bob, knowsTerm, x))),
                arguments(new Ask(new TriplePattern(Alice, knowsTerm, x)),
                          List.of(new TriplePattern(Bob, knowsTerm, x)),
                          new Ask(new TriplePattern(Bob, knowsTerm, x))),
                arguments(new Ask(new TriplePattern(x, nameTerm, y)),
                          List.of(),
                          new Ask(IdentityNode.INSTANCE)),
                arguments(new Ask(new TriplePattern(x, nameTerm, y)),
                          List.of(new Join(new TriplePattern(Alice,knowsTerm,x),
                                           new TriplePattern(x, nameTerm, bob))),
                          new Ask(new Join(new TriplePattern(Alice,knowsTerm,x),
                                           new TriplePattern(x, nameTerm, bob))))
        );
    }

    @ParameterizedTest @MethodSource
    void testWithChildren(@NonNull Ask ask, @NonNull List<Op> children, @NonNull Ask expected) {
        assertEquals(ask.varNames(), List.of());
        Op withChildren = ask.withChildren(children);
        assertTrue(withChildren.deepEquals(expected));
        assertTrue(expected.deepEquals(withChildren));
        assertEquals(ask.varNames(), List.of());
    }

}