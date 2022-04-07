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
class SliceTest {
    static Stream<Arguments> testEquals() {
        return Stream.of(
                arguments(new Slice(new TriplePattern(x, knowsTerm, y), 1, 0),
                          new Slice(new TriplePattern(x, knowsTerm, y), 1, 0),
                          true),
                arguments(new Slice(new TriplePattern(x, knowsTerm, y), 1, 0),
                          new Slice(new TriplePattern(y, knowsTerm, x), 1, 0),
                          false),
                arguments(new Slice(new TriplePattern(x, knowsTerm, y), 1, 0),
                          new Slice(new TriplePattern(x, nameTerm, y), 1, 0),
                          false),
                arguments(new Slice(new TriplePattern(x, knowsTerm, y), 1, 0),
                          new Slice(new TriplePattern(x, knowsTerm, y), 2, 0),
                          false),

                arguments(new Slice(new TriplePattern(Alice, knowsTerm, x), Long.MAX_VALUE, 23),
                          new Slice(new TriplePattern(Alice, knowsTerm, x), Long.MAX_VALUE, 23),
                          true),
                arguments(new Slice(new TriplePattern(Alice, knowsTerm, x), Long.MAX_VALUE, 23),
                          new Slice(new TriplePattern(x, knowsTerm, Alice), Long.MAX_VALUE, 23),
                          false),
                arguments(new Slice(new TriplePattern(Alice, knowsTerm, x), Long.MAX_VALUE, 23),
                          new Slice(new TriplePattern(Alice, knowsTerm, x), Long.MAX_VALUE, 25),
                          false),
                arguments(new Slice(new TriplePattern(Alice, knowsTerm, x), Long.MAX_VALUE, 23),
                          new Slice(new TriplePattern(Alice, knowsTerm, x), Long.MAX_VALUE, 0),
                          false)
        );
    }

    @ParameterizedTest @MethodSource
    public void testEquals(@NonNull Slice a, @NonNull Slice b, boolean expected) {
        assertEquals(expected, a.deepEquals(b));
        assertEquals(expected, b.deepEquals(a));
    }

    static Stream<Arguments> testWithChildren() {
        return Stream.of(
                /* 1 */ arguments(new Slice(new TriplePattern(x, knowsTerm, y), 23, 0),
                                  List.of(new TriplePattern(y, knowsTerm, x)),
                                  new Slice(new TriplePattern(y, knowsTerm, x), 23, 0)),
                /* 2 */ arguments(new Slice(new TriplePattern(x, knowsTerm, y), 23, 0),
                                  List.of(new TriplePattern(Alice, nameTerm, x)),
                                  new Slice(new TriplePattern(Alice, nameTerm, x), 23, 0)),
                /* 3 */ arguments(new Slice(new TriplePattern(x, knowsTerm, y), 23, 0),
                                  List.of(IdentityNode.INSTANCE),
                                  new Slice(IdentityNode.INSTANCE, 23, 0)),
                /* 4 */ arguments(new Slice(new TriplePattern(x, knowsTerm, y), 23, 0),
                                  List.of(),
                                  new Slice(IdentityNode.INSTANCE, 23, 0)),

                /* 5 */ arguments(new Slice(new TriplePattern(x, knowsTerm, y), Long.MAX_VALUE, 23),
                        List.of(new TriplePattern(x, nameTerm, y)),
                        new Slice(new TriplePattern(x, nameTerm, y), Long.MAX_VALUE, 23)),
                /* 6 */ arguments(new Slice(new TriplePattern(x, knowsTerm, y), Long.MAX_VALUE, 23),
                        List.of(new TriplePattern(x, knowsTerm, Alice)),
                        new Slice(new TriplePattern(x, knowsTerm, Alice), Long.MAX_VALUE, 23)),
                /* 7 */ arguments(new Slice(new TriplePattern(x, knowsTerm, y), Long.MAX_VALUE, 23),
                        List.of(IdentityNode.INSTANCE),
                        new Slice(IdentityNode.INSTANCE, Long.MAX_VALUE, 23)),
                /* 8 */ arguments(new Slice(new TriplePattern(x, knowsTerm, y), Long.MAX_VALUE, 23),
                        List.of(IdentityNode.INSTANCE, new TriplePattern(x, nameTerm, y)),
                        new Slice(new TriplePattern(x, nameTerm, y), Long.MAX_VALUE, 23))
        );
    }

    @ParameterizedTest @MethodSource
    public void testWithChildren(@NonNull Slice op, @NonNull List<Op> children,
                                 @NonNull Slice expected) {
        Op oldInner = op.children().get(0);

        Op withChildren = op.withChildren(children);
        assertSame(oldInner, op.children.get(0));
        assertEquals(op.outputVars(), oldInner.outputVars());
        assertSame(op.outputVars(), oldInner.outputVars());

        assertTrue(withChildren.deepEquals(expected));
        assertTrue(expected.deepEquals(withChildren));
    }
}