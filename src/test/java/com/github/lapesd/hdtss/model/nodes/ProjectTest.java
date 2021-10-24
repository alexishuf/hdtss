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
class ProjectTest {

    static Stream<Arguments> testEquals() {
        return Stream.of(
                arguments(
                        new Project(List.of("x", "y"), new TriplePattern(x, knowsTerm, y)),
                        new Project(List.of("x", "y"), new TriplePattern(x, knowsTerm, y)),
                        true
                ),
                arguments(
                        new Project(List.of("x", "y"), new TriplePattern(x, knowsTerm, y)),
                        new Project(List.of("y", "x"), new TriplePattern(x, knowsTerm, y)),
                        false
                ),
                arguments(
                        new Project(List.of("x", "y"), new TriplePattern(x, knowsTerm, y)),
                        new Project(List.of("x", "y"), new TriplePattern(y, knowsTerm, x)),
                        false
                ),
                arguments(
                        new Project(List.of("x", "y"), new TriplePattern(x, knowsTerm, y)),
                        new Project(List.of("x", "y"), new TriplePattern(x, nameTerm, y)),
                        false
                )
        );
    }

    @ParameterizedTest @MethodSource
    void testEquals(@NonNull Project a, @NonNull Project b, boolean expected) {
        assertEquals(expected, a.deepEquals(b));
        assertEquals(expected, b.deepEquals(a));
    }

    static Stream<Arguments> testWithChildren() {
        return Stream.of(
                arguments(new Project(List.of("x"), new TriplePattern(x, knowsTerm, y)),
                          List.of(new TriplePattern(x, knowsTerm, Alice)),
                          new Project(List.of("x"), new TriplePattern(x, knowsTerm, Alice))),
                arguments(new Project(List.of("x"), new TriplePattern(x, knowsTerm, y)),
                          List.of(new TriplePattern(Alice, knowsTerm, y)),
                          new Project(List.of("x"), new TriplePattern(Alice, knowsTerm, y))),
                arguments(new Project(List.of("x", "y"), new TriplePattern(x, knowsTerm, y)),
                          List.of(IdentityNode.INSTANCE),
                          new Project(List.of("x", "y"), IdentityNode.INSTANCE)),
                arguments(new Project(List.of("x", "y"), new TriplePattern(x, knowsTerm, y)),
                          List.of(),
                          new Project(List.of("x", "y"), IdentityNode.INSTANCE))
        );
    }

    @ParameterizedTest @MethodSource
    void testWithChildren(@NonNull Project op, @NonNull List<Op> children,
                          @NonNull Project expected) {
        Op oldInner = op.inner();
        List<@NonNull String> oldVars = new ArrayList<>(op.varNames());

        Op withChildren = op.withChildren(children);
        assertSame(op.inner(), oldInner);
        assertEquals(op.varNames(), oldVars);

        assertTrue(withChildren.deepEquals(expected));
        assertTrue(expected.deepEquals(withChildren));
    }


}