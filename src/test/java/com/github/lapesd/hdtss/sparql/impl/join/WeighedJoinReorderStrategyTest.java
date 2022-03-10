package com.github.lapesd.hdtss.sparql.impl.join;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.vocab.FOAF.*;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class WeighedJoinReorderStrategyTest {
    @SuppressWarnings("unused") static Stream<Arguments> testProjection() {
        return Stream.of(
                arguments(List.of("x"),
                        List.of(new TriplePattern(x, knowsTerm, Bob)),
                        null),
                arguments(asList("x", "y"),
                        List.of(new TriplePattern(x, knowsTerm, y)),
                        null),
                arguments(asList("x", "y"),
                        asList(new TriplePattern(x, knowsTerm, Bob),
                                new TriplePattern(y, knowsTerm, Bob)),
                        null),
                arguments(asList("x", "y"),
                        asList(new TriplePattern(x, nameTerm, AliceEN),
                                new TriplePattern(x, knowsTerm, y)),
                        null),
                arguments(asList("x", "y"),
                        asList(new TriplePattern(y, nameTerm, bob),
                                new TriplePattern(x, knowsTerm, y)),
                        new int[]{1, 0}),
                arguments(asList("x", "y", "z"),
                        asList(new TriplePattern(y, nameTerm, bob),
                                new TriplePattern(y, ageTerm, z),
                                new TriplePattern(x, knowsTerm, y)),
                        new int[] {2, 0, 1})
        );
    }

    @ParameterizedTest @MethodSource
    void testProjection(List<String> exposed, List<Op> reordered, int[] expected) {
        int[] actual = VarPositionsJoinReorderStrategy.getProjection(exposed, reordered);
        assertArrayEquals(expected, actual);
    }


    @ParameterizedTest @ValueSource(strings = {
            "true  :: ",
            "true  :: 0",
            "false :: 1",
            "true  :: 0, 1",
            "false :: 1, 0",
            "true  :: 0, 1, 2",
            "false :: 0, 2, 1",
    })
    void testIsNoOp(String data) {
        String[] parts = data.split(" +:: +");
        boolean expected = Boolean.parseBoolean(parts[0]);
        long[] input = Arrays.stream((parts.length > 1 ? parts[1] : "").split(" *, *"))
                .filter(s -> !s.isBlank())
                .mapToLong(Integer::parseInt).toArray();
        assertEquals(expected, VarPositionsJoinReorderStrategy.isNoOp(input));
    }
}