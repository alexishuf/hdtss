package com.github.lapesd.hdtss.model.nodes;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.vocab.FOAF.knowsTerm;
import static com.github.lapesd.hdtss.vocab.FOAF.nameTerm;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class OffsetTest {

    static Stream<Arguments> testEquals() {
        return Stream.of(
                arguments(new Offset(23, new TriplePattern(Alice, knowsTerm, x)),
                          new Offset(23, new TriplePattern(Alice, knowsTerm, x)),
                          true),
                arguments(new Offset(23, new TriplePattern(Alice, knowsTerm, x)),
                          new Offset(23, new TriplePattern(x, knowsTerm, Alice)),
                          false),
                arguments(new Offset(23, new TriplePattern(Alice, knowsTerm, x)),
                          new Offset(25, new TriplePattern(Alice, knowsTerm, x)),
                          false),
                arguments(new Offset(23, new TriplePattern(Alice, knowsTerm, x)),
                          new Offset(0, new TriplePattern(Alice, knowsTerm, x)),
                          false)
        );
    }

    @ParameterizedTest @MethodSource
    public void testEquals(@NonNull Offset a, @NonNull Offset b, boolean expected) {
        assertEquals(a.deepEquals(b), expected);
        assertEquals(b.deepEquals(a), expected);
    }

    static Stream<Arguments> testWithChildren() {
        return Stream.of(
    /* 1 */     arguments(new Offset(23, new TriplePattern(x, knowsTerm, y)),
                          List.of(new TriplePattern(x, nameTerm, y)),
                          new Offset(23, new TriplePattern(x, nameTerm, y))),
    /* 2 */     arguments(new Offset(23, new TriplePattern(x, knowsTerm, y)),
                          List.of(new TriplePattern(x, knowsTerm, Alice)),
                          new Offset(23, new TriplePattern(x, knowsTerm, Alice))),
    /* 3 */     arguments(new Offset(23, new TriplePattern(x, knowsTerm, y)),
                          List.of(IdentityNode.INSTANCE),
                          new Offset(23, IdentityNode.INSTANCE)),
    /* 4 */     arguments(new Offset(23, new TriplePattern(x, knowsTerm, y)),
                        List.of(IdentityNode.INSTANCE, new TriplePattern(x, nameTerm, y)),
                        new Offset(23, new TriplePattern(x, nameTerm, y)))
                );
    }

    @ParameterizedTest @MethodSource
    public void testWithChildren(@NonNull Offset a, @NonNull List<Op> replacements,
                                 @NonNull Offset expected) {
        var oldVars = new ArrayList<>(a.outputVars());
        Op oldInner = a.children().get(0);
        Op withChildren = a.withChildren(replacements);

        assertTrue(withChildren.deepEquals(expected));
        assertTrue(expected.deepEquals(withChildren));

        assertEquals(oldVars, a.outputVars());
        assertSame(oldInner, a.children().get(0));
    }

}