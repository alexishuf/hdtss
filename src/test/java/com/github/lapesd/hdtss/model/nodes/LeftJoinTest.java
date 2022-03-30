package com.github.lapesd.hdtss.model.nodes;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Set;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.vocab.FOAF.ageTerm;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class LeftJoinTest {
    static Stream<Arguments> testDirectedJoinInputFilterVarNames() {
        return Stream.of(
                arguments(new TriplePattern(x, ageTerm, y),
                        new Filter(new TriplePattern(Alice, ageTerm, z), "?z > 23"),
                        Set.of()),
                arguments(new TriplePattern(x, ageTerm, y),
                        new Filter(new TriplePattern(Alice, ageTerm, z), "?z > ?y"),
                        Set.of()),
                arguments(new TriplePattern(x, ageTerm, y),
                        new Filter(new TriplePattern(Alice, ageTerm, z), "?z > ?w"),
                        Set.of("w")),
                arguments(new TriplePattern(x, ageTerm, y),
                        new Filter(new TriplePattern(Alice, ageTerm, z), "23 > ?w"),
                        Set.of("w")),
                arguments(new Filter(new TriplePattern(x, ageTerm, y), "?y < ?w"),
                        new Filter(new TriplePattern(Alice, ageTerm, z), "?z > ?y"),
                        Set.of("w")),
                // the bind is directional, z from right cannot be assigned to the z from left
                arguments(new Filter(new TriplePattern(x, ageTerm, y), "?y < ?z"),
                        new Filter(new TriplePattern(Alice, ageTerm, z), "?z > ?y"),
                        Set.of("z")),
                arguments(new Filter(new TriplePattern(x, ageTerm, y), "?y < ?z"),
                        new Filter(new TriplePattern(Alice, ageTerm, z), "?z > ?w"),
                        Set.of("z", "w"))
        );
    }

    @ParameterizedTest @MethodSource
    void testDirectedJoinInputFilterVarNames(@NonNull Op left, @NonNull Op right,
                                             @NonNull Set<@NonNull String> expected) {
        assertEquals(expected, new LeftJoin(left, right).inputVars());
    }
}