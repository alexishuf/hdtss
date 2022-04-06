package com.github.lapesd.hdtss.model.nodes;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.vocab.FOAF.ageTerm;
import static com.github.lapesd.hdtss.vocab.FOAF.knowsTerm;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class AssignTest {
    static Stream<Arguments> testEquals() {
        return Stream.of(
                arguments(new Assign(new TriplePattern(x, ageTerm, y), "z", "?y * 2"),
                          new Assign(new TriplePattern(x, ageTerm, y), "z", "?y * 2"),
                          true),
                arguments(new Assign(new TriplePattern(x, ageTerm, y), "y", "?y * 2"),
                          new Assign(new TriplePattern(x, ageTerm, y), "y", "?y * 2"),
                          true),
                arguments(new Assign(new TriplePattern(x, ageTerm, y), "y", "?y * 2"),
                          new Assign(new TriplePattern(x, ageTerm, z), "y", "?y * 2"),
                          false),
                arguments(new Assign(new TriplePattern(x, ageTerm, y), "y", "?y * 2"),
                          new Assign(new TriplePattern(x, ageTerm, y), "y", "?y * 3"),
                          false)
        );
    }

    @ParameterizedTest @MethodSource
    void testEquals(@NonNull Assign a, @NonNull Assign b, boolean expected) {
        assertEquals(expected, a.deepEquals(b));
        assertEquals(expected, b.deepEquals(a));
    }

    static Stream<Arguments> testVarNames() {
        return Stream.of(
                arguments(new Assign(new TriplePattern(x, ageTerm, y), "z", "?y * 2"),
                          asList("x", "y", "z")),
                arguments(new Assign(new TriplePattern(x, ageTerm, y), "y", "?y * 2"),
                          asList("x", "y")),
                arguments(new Assign(new Join(new TriplePattern(x, knowsTerm, y),
                                              new TriplePattern(x, ageTerm, z)),
                                     "y", "?z * 2"),
                          asList("x", "y", "z")),
                arguments(new Assign(Map.of("z", "?y * 2"),
                                     new TriplePattern(x, ageTerm, y)),
                          asList("x", "y", "z"))
        );
    }

    @ParameterizedTest @MethodSource
    void testVarNames(@NonNull Assign a, @NonNull List<String> expected) {
        assertEquals(expected, a.outputVars());
        assertEquals(a.var2expr().keySet(), new HashSet<>(a.assignedVars()));
        for (String innerVar : a.inner().outputVars()) {
            assertEquals(a.inner().outputVars().indexOf(innerVar),
                         a.outputVars().indexOf(innerVar));
        }
    }
}