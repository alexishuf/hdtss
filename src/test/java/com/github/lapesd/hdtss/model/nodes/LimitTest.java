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
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class LimitTest {

    static Stream<Arguments> testEquals() {
        return Stream.of(
                arguments(new Limit(1, new TriplePattern(x, knowsTerm, y)),
                          new Limit(1, new TriplePattern(x, knowsTerm, y)),
                          true),
                arguments(new Limit(1, new TriplePattern(x, knowsTerm, y)),
                          new Limit(1, new TriplePattern(y, knowsTerm, x)),
                          false),
                arguments(new Limit(1, new TriplePattern(x, knowsTerm, y)),
                          new Limit(1, new TriplePattern(x, nameTerm, y)),
                          false),
                arguments(new Limit(1, new TriplePattern(x, knowsTerm, y)),
                          new Limit(2, new TriplePattern(x, knowsTerm, y)),
                          false)
        );
    }

    @ParameterizedTest @MethodSource
    public void testEquals(@NonNull Limit a, @NonNull Limit b, boolean expected) {
        assertEquals(expected, a.deepEquals(b));
        assertEquals(expected, b.deepEquals(a));
    }

    static Stream<Arguments> testWithChildren() {
        return Stream.of(
        /* 1 */ arguments(new Limit(23, new TriplePattern(x, knowsTerm, y)),
                          List.of(new TriplePattern(y, knowsTerm, x)),
                          new Limit(23, new TriplePattern(y, knowsTerm, x))),
        /* 2 */ arguments(new Limit(23, new TriplePattern(x, knowsTerm, y)),
                          List.of(new TriplePattern(Alice, nameTerm, x)),
                          new Limit(23, new TriplePattern(Alice, nameTerm, x))),
        /* 3 */ arguments(new Limit(23, new TriplePattern(x, knowsTerm, y)),
                          List.of(IdentityNode.INSTANCE),
                          new Limit(23, IdentityNode.INSTANCE)),
        /* 4 */ arguments(new Limit(23, new TriplePattern(x, knowsTerm, y)),
                          List.of(),
                          new Limit(23, IdentityNode.INSTANCE))
        );
    }

    @ParameterizedTest @MethodSource
    public void testWithChildren(@NonNull Limit op, @NonNull List<Op> children,
                                 @NonNull Limit expected) {
        Op oldInner = op.children().get(0);

        Op withChildren = op.withChildren(children);
        assertSame(oldInner, op.children.get(0));
        assertEquals(op.varNames(), oldInner.varNames());
        assertSame(op.varNames(), oldInner.varNames());

        assertTrue(withChildren.deepEquals(expected));
        assertTrue(expected.deepEquals(withChildren));
    }
}