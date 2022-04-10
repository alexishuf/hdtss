package com.github.lapesd.hdtss.model;

import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import com.github.lapesd.hdtss.utils.Binding;
import com.github.lapesd.hdtss.vocab.FOAF;
import com.google.common.collect.Collections2;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.model.TermPosition.*;
import static com.github.lapesd.hdtss.model.nodes.TriplePattern.SharedVars.*;
import static com.github.lapesd.hdtss.vocab.FOAF.ageTerm;
import static com.github.lapesd.hdtss.vocab.FOAF.knowsTerm;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@SuppressWarnings("unused")
@Tag("fast")
class TriplePatternTest {
    static @NonNull Stream<@NonNull Arguments> testStream() {
        return Stream.of(
                arguments(new TriplePattern(x, knowsTerm, Alice),
                          asList(x, knowsTerm, Alice)),
                arguments(new TriplePattern(x, knowsTerm, y), asList(x, knowsTerm, y)),
                arguments(new TriplePattern(x, knowsTerm, x), asList(x, knowsTerm, x))
        );
    }

    @ParameterizedTest @MethodSource
    void testStream(@NonNull TriplePattern tp, @NonNull List<@NonNull Term> expected) {
        assertEquals(expected, tp.stream().collect(Collectors.toList()));
    }

    static @NonNull Stream<Arguments> testCollectVarsInfo() {
        String alice = "<"+EX+"Alice>", bob = "\"bob\"@en-US";
        String knows = "<"+FOAF.knows+">", name = "<"+FOAF.name+">";
        String x = "?x", y = "$y", z = "?z.3";
        return Stream.of(
                arguments(new TriplePattern(alice, knows, x),
                          new TriplePattern.VarsInfo(
                                  new String[]{"x"}, new TermPosition[] {OBJ}, NONE)),
                arguments(new TriplePattern(alice, name, bob),
                          new TriplePattern.VarsInfo(new String[0], new TermPosition[0], NONE)),
                arguments(new TriplePattern(x, knows, y),
                          new TriplePattern.VarsInfo(
                                  new String[]{"x", "y"}, new TermPosition[]{SUB, OBJ}, NONE)),
                arguments(new TriplePattern(y, knows, x),
                        new TriplePattern.VarsInfo(
                                new String[]{"y", "x"}, new TermPosition[]{SUB, OBJ}, NONE)),
                arguments(new TriplePattern(x, knows, x),
                          new TriplePattern.VarsInfo(
                                  new String[]{"x"}, new TermPosition[]{SUB}, SO)),
                arguments(new TriplePattern(x, x, bob),
                          new TriplePattern.VarsInfo(
                                  new String[]{"x"}, new TermPosition[]{SUB}, SP)),
                arguments(new TriplePattern(alice, x, x),
                          new TriplePattern.VarsInfo(
                                  new String[]{"x"}, new TermPosition[]{PRE}, PO)),
                arguments(new TriplePattern(y, x, x),
                          new TriplePattern.VarsInfo(
                                  new String[]{"y", "x"},
                                  new TermPosition[]{SUB, PRE}, PO)),
                arguments(new TriplePattern(x, x, y),
                          new TriplePattern.VarsInfo(
                                  new String[]{"x", "y"},
                                  new TermPosition[]{SUB, OBJ}, SP)),
                arguments(new TriplePattern(x, y, x),
                          new TriplePattern.VarsInfo(
                                  new String[]{"x", "y"},
                                  new TermPosition[]{SUB, PRE}, SO)),
                arguments(new TriplePattern(x, x, x),
                          new TriplePattern.VarsInfo(
                                  new String[]{"x"},
                                  new TermPosition[]{SUB}, ALL)),
                arguments(new TriplePattern(x, z, y),
                          new TriplePattern.VarsInfo(
                                  new String[]{"x", "z.3", "y"},
                                  new TermPosition[]{SUB, PRE, OBJ}, NONE)),
                arguments(new TriplePattern(x, y, bob),
                          new TriplePattern.VarsInfo(
                                  new String[]{"x", "y"}, new TermPosition[]{SUB, PRE}, NONE)),
                arguments(new TriplePattern(alice, x, y),
                          new TriplePattern.VarsInfo(
                                  new String[]{"x", "y"}, new TermPosition[]{PRE, OBJ}, NONE)),
                arguments(new TriplePattern(alice, knows, x),
                          new TriplePattern.VarsInfo(
                                  new String[]{"x"}, new TermPosition[]{OBJ}, NONE)),
                arguments(new TriplePattern(alice, x, bob),
                          new TriplePattern.VarsInfo(
                                  new String[]{"x"}, new TermPosition[]{PRE}, NONE))
        );
    }

    @ParameterizedTest @MethodSource
    void testCollectVarsInfo(@NonNull TriplePattern tp,
                             TriplePattern.@NonNull VarsInfo expected) {
        assertEquals(expected, tp.collectVarsInfo());
    }

    static @NonNull Stream<Arguments> testBind() {
        TriplePattern tp1 = new TriplePattern(x, ageTerm, y);
        return Stream.of(
                arguments(tp1, Map.of("x", Alice),
                          new TriplePattern(Alice, ageTerm, y)),
                arguments(tp1, Map.of("x", Alice, "z", i23),
                          new TriplePattern(Alice, ageTerm, y)),
                arguments(tp1, Map.of("z", i23), tp1),
                arguments(tp1, Map.of(), tp1),
                arguments(tp1, Map.of("x", Alice, "y", i23),
                          new TriplePattern(Alice, ageTerm, i23)),
                arguments(new TriplePattern(y, knowsTerm, y), Map.of("y", Bob),
                          new TriplePattern(Bob, knowsTerm, Bob)),
                arguments(new TriplePattern(y, knowsTerm, y), Map.of("x", Alice, "y", Bob),
                          new TriplePattern(Bob, knowsTerm, Bob))
        );
    }

    @SuppressWarnings("UnstableApiUsage") @ParameterizedTest @MethodSource
    void testBind(@NonNull TriplePattern tp, @NonNull Map<@NonNull String, Term> v2t,
                  @NonNull TriplePattern expected) {
        assertEquals(expected, tp.bind(new Binding(v2t)));
        for (List<String> names : Collections2.permutations(v2t.keySet())) {
            Term[] row = new Term[names.size()];
            for (int i = 0; i < row.length; i++) row[i] = v2t.get(names.get(i));
            assertEquals(expected, tp.bind(new Binding(names, row)));
        }
    }

    @ParameterizedTest @ValueSource(strings = {
            "NONE:null:null:null",
            "SP:SUB:PRE:null",
            "SO:SUB:OBJ:null",
            "PO:PRE:OBJ:null",
            "ALL:SUB:PRE:OBJ"
    })
    void testSharedVarsPositions(@NonNull String testData) {
        String[] parts = testData.split(":");
        var sh = TriplePattern.SharedVars.valueOf(parts[0]);
        var first = parts[1].equals("null") ? null : TermPosition.valueOf(parts[1]);
        var second = parts[2].equals("null") ? null : TermPosition.valueOf(parts[2]);
        var third = parts[3].equals("null") ? null : TermPosition.valueOf(parts[3]);
        assertEquals(first, sh.first());
        assertEquals(second, sh.second());
        assertEquals(third, sh.third());
    }
}