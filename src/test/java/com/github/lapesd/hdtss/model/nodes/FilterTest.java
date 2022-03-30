package com.github.lapesd.hdtss.model.nodes;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.vocab.FOAF;
import com.google.common.collect.Collections2;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestUtils.parseTriplePattern;
import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.vocab.FOAF.ageTerm;
import static com.github.lapesd.hdtss.vocab.FOAF.knowsTerm;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class FilterTest {
    static @NonNull Stream<@NonNull Arguments> testBind() {
        TriplePattern tp = parseTriplePattern("?x <" + FOAF.knows + "> $y");
        return Stream.of(
                arguments(new Filter(new TriplePattern(x, knowsTerm, y), "?x = ?y"),
                          Map.of("x", Alice),
                          new Filter(new TriplePattern(Alice, knowsTerm, y), Alice+" = ?y")),
                arguments(new Filter(new TriplePattern(x, knowsTerm, y), "str(?x) > str($y)"),
                          Map.of("y", Alice),
                          new Filter(new TriplePattern(x, knowsTerm, Alice), "str(?x) > str("+Alice+")")),
                arguments(new Filter(new TriplePattern(x, ageTerm, y), "?y > 23"),
                          Map.of("y", i25),
                          new Filter(new TriplePattern(x, ageTerm, i25), i25+" > 23")),
                arguments(new Filter(new TriplePattern(x, ageTerm, y), "?y = ?z"),
                          Map.of("y", i23),
                          new Filter(new TriplePattern(x, ageTerm, i23), i23+" = ?z"))
        );
    }

    @SuppressWarnings("UnstableApiUsage")
    @ParameterizedTest @MethodSource
    void testBind(@NonNull Filter in, @NonNull Map<@NonNull String, Term> v2t,
                  @NonNull Filter expected) {
        Op actual = in.bind(v2t);
        String msg = "\nExpected: " + expected + "\n  Actual: " + actual;
        assertTrue(actual.deepEquals(expected), msg);
        assertTrue(expected.deepEquals(actual), msg);

        for (List<String> varNames : Collections2.permutations(v2t.keySet())) {
            Term[] terms = new Term[varNames.size()];
            for (int i = 0; i < terms.length; i++)
                terms[i] = v2t.get(varNames.get(i));
            Op actual2 = in.bind(varNames, terms);
            String msg2 = "\nExpected: "+expected+"\n  Actual: "+actual2;
            assertTrue(actual2.deepEquals(expected), msg2);
            assertTrue(expected.deepEquals(actual2), msg2);
        }
    }

    static Stream<Arguments> testInputFilterVarNames() {
        return Stream.of(
                arguments(new Filter(new TriplePattern(Alice, ageTerm, x), "?x > 23"),
                          Set.of()),
                arguments(new Filter(new TriplePattern(Alice, ageTerm, x), "?x > ?y"),
                          Set.of("y")),
                arguments(new Filter(new Distinct(new TriplePattern(Alice, ageTerm, x)),
                                     "?x > 23"),
                          Set.of()),
                arguments(new Filter(new Distinct(new TriplePattern(Alice, ageTerm, x)),
                                "?x > ?y"),
                        Set.of("y")),
                arguments(new Filter(new Join(new TriplePattern(Alice, knowsTerm, x),
                                              new Filter(new TriplePattern(x, ageTerm, y), "?y > ?z"),
                                              new TriplePattern(Bob, ageTerm, z)),
                                     "?x > ?z"),
                          Set.of()),
                arguments(new Filter(new Join(new TriplePattern(Alice, knowsTerm, x),
                                              new Filter(new TriplePattern(x, ageTerm, y), "?y > ?w"),
                                              new TriplePattern(Bob, ageTerm, z)),
                                     "?x > ?z"),
                          Set.of("w")),
                arguments(new Filter(new Join(new TriplePattern(Alice, knowsTerm, x),
                                              new Filter(new TriplePattern(x, ageTerm, y), "?y > ?w"),
                                              new TriplePattern(Bob, ageTerm, z)),
                                     "?x > ?t"),
                          Set.of("w", "t"))
        );
    }

    @ParameterizedTest @MethodSource
    void testInputFilterVarNames(@NonNull Filter op, @NonNull Set<@NonNull String> expected) {
        assertEquals(expected, op.inputVars());
    }

    static Stream<@NonNull Arguments> testVarNames() {
        List<Object> empty = List.of();
        List<String> xList = List.of("x");
        List<String> xyList = List.of("x", "y");
        return Stream.of(
                arguments(new Filter(new TriplePattern(Alice, ageTerm, i23), "23 > 25"), empty),
                arguments(new Filter(new TriplePattern(Alice, ageTerm, x), "?x > 23"),   xList),
                arguments(new Filter(new TriplePattern(x, ageTerm, y), "?y > 23"),       xyList),
                arguments(new Filter(new TriplePattern(x, ageTerm, y), "?x > 23"),       xyList),
                arguments(new Filter(new TriplePattern(Alice, ageTerm, x), "?x > ?y"),   xList),
                arguments(new Filter(new TriplePattern(Alice, ageTerm, x), "?y > ?x"),   xList)
        );
    }

    @ParameterizedTest @MethodSource
    void testVarNames(@NonNull Filter op, @NonNull List<@NonNull String> expected) {
        assertEquals(expected, op.varNames());
    }
}