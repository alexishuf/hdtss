package com.github.lapesd.hdtss.model.nodes;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import com.google.common.collect.Collections2;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.vocab.FOAF.knowsTerm;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class ValuesTest {

    static Stream<Arguments> testEquals() {
        return Stream.of(
                arguments(
                        new Values(
                                new BatchQuerySolutions(List.of("x"),
                                        asList(SolutionRow.of(Alice),
                                               SolutionRow.of(Bob))),
                                new TriplePattern(x, knowsTerm, y)
                        ),
                        new Values(
                                new BatchQuerySolutions(List.of("x"),
                                        asList(SolutionRow.of(Alice),
                                               SolutionRow.of(Bob))),
                                new TriplePattern(x, knowsTerm, y)
                        ),
                        true),
                arguments(
                        new Values(
                                new BatchQuerySolutions(List.of("x"),
                                        asList(SolutionRow.of(Alice),
                                               SolutionRow.of(Bob))),
                                new TriplePattern(x, knowsTerm, y)
                        ),
                        new Values(
                                new BatchQuerySolutions(List.of("x"),
                                        asList(SolutionRow.of(Bob),
                                               SolutionRow.of(Alice))),
                                new TriplePattern(x, knowsTerm, y)
                        ),
                        false),
                arguments(
                        new Values(
                                new BatchQuerySolutions(List.of("x"),
                                        asList(SolutionRow.of(Alice),
                                               SolutionRow.of(Bob))),
                                new TriplePattern(x, knowsTerm, y)
                        ),
                        new Values(
                                new BatchQuerySolutions(List.of("x"),
                                        asList(SolutionRow.of(Alice),
                                               SolutionRow.of(Bob))),
                                new TriplePattern(y, knowsTerm, x)
                        ),
                        false),

                arguments(
                        new Values(
                                new BatchQuerySolutions(asList("x", "y"),
                                        asList(SolutionRow.of(Alice, Charlie),
                                               SolutionRow.of(Bob, Charlie))),
                                new TriplePattern(x, knowsTerm, y)
                        ),
                        new Values(
                                new BatchQuerySolutions(asList("y", "x"),
                                        asList(SolutionRow.of(Charlie, Alice),
                                               SolutionRow.of(Charlie, Bob))),
                                new TriplePattern(x, knowsTerm, y)
                        ),
                        false)
        );
    }

    @ParameterizedTest @MethodSource
    void testEquals(@NonNull Values a, @NonNull Values b, boolean expected) {
        assertEquals(expected, a.deepEquals(b));
        assertEquals(expected, b.deepEquals(a));
    }

    static Stream<Arguments> testBind() {
        return Stream.of(
                // no effect bind
   /* 1 */      arguments(
                        new Values(
                                new BatchQuerySolutions(
                                        List.of("x"),
                                        List.of(SolutionRow.of(Alice))),
                                new TriplePattern(x, knowsTerm, y)),
                        Map.of("z", Charlie),
                        new Values(
                                new BatchQuerySolutions(
                                        List.of("x"),
                                        List.of(SolutionRow.of(Alice))),
                                new TriplePattern(x, knowsTerm, y))
                        ),
                // non-conflicting bind
   /* 2 */      arguments(
                        new Values(
                                new BatchQuerySolutions(
                                        List.of("x"),
                                        List.of(SolutionRow.of(Alice))),
                                new TriplePattern(x, knowsTerm, y)),
                        Map.of("y", Charlie, "z", Alice),
                        new Values(
                                new BatchQuerySolutions(
                                        List.of("x"),
                                        List.of(SolutionRow.of(Alice))),
                                new TriplePattern(x, knowsTerm, Charlie))
                ),
                // bind overlaps with value but affects inner
   /* 3 */      arguments(
                        new Values(
                                new BatchQuerySolutions(
                                        List.of("x"),
                                        List.of(SolutionRow.of(Alice))),
                                new TriplePattern(x, knowsTerm, y)),
                        Map.of("x", Bob, "y", Charlie),
                        new Values(
                                new BatchQuerySolutions(
                                        List.of("x"),
                                        List.of(SolutionRow.of(Alice))),
                                new TriplePattern(x, knowsTerm, Charlie)
                        )
                ),
                //bind overlaps with Values but does not change inner
   /* 4 */      arguments(
                        new Values(
                                new BatchQuerySolutions(
                                        List.of("x"),
                                        List.of(SolutionRow.of(Alice))),
                                new TriplePattern(x, knowsTerm, y)
                        ),
                        Map.of("x", Alice, "z", Charlie),
                        new Values(
                                new BatchQuerySolutions(
                                        List.of("x"),
                                        List.of(SolutionRow.of(Alice))),
                                new TriplePattern(x, knowsTerm, y)
                        )
                )
        );
    }

    @SuppressWarnings("UnstableApiUsage") @ParameterizedTest @MethodSource
    void testBind(@NonNull Values in, @NonNull Map<String, Term> v2t, @NonNull Values expected) {
        Op bound = in.bind(v2t);
        assertTrue(bound.deepEquals(expected));
        assertTrue(expected.deepEquals(bound));

        for (List<String> permutation : Collections2.permutations(v2t.keySet())) {
            Term[] row = new Term[permutation.size()];
            for (int i = 0; i < row.length; i++) row[i] = v2t.get(permutation.get(i));
            Op listBound = in.bind(permutation, row);
            assertTrue(listBound.deepEquals(expected));
            assertTrue(expected.deepEquals(listBound));
        }
    }

    static Stream<Arguments> testVarNames() {
        return Stream.of(
                // vars bound within Values are not exposed as vars
                arguments(
                        new Values(new BatchQuerySolutions(
                                        List.of("x"), List.of(SolutionRow.of(Alice))),
                                   new TriplePattern(x, knowsTerm, y)),
                        List.of("y")),
                // bogus Values does not blow up
                arguments(
                        new Values(new BatchQuerySolutions(
                                        List.of("z"), List.of(SolutionRow.of(Alice))),
                                   new TriplePattern(x, knowsTerm, y)),
                        asList("x", "y")),
                // No issue if Values binds all variables
                arguments(
                        new Values(new BatchQuerySolutions(
                                        List.of("x", "y"), List.of(SolutionRow.of(Alice, Bob))),
                                   new TriplePattern(x, knowsTerm, y)),
                        List.of())
        );
    }

    @ParameterizedTest @MethodSource
    void testVarNames(@NonNull Values in, @NonNull List<String> expected) {
        assertEquals(expected, in.varNames());
    }

}