package com.github.lapesd.hdtss.model.nodes;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.utils.Binding;
import com.github.lapesd.hdtss.vocab.XSD;
import com.google.common.collect.Collections2;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static com.github.lapesd.hdtss.vocab.FOAF.ageTerm;
import static com.github.lapesd.hdtss.vocab.FOAF.knowsTerm;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
class MinusTest {
    static Stream<Arguments> testEquals() {
        return Stream.of(
                arguments(new Minus(new TriplePattern(x, knowsTerm, Alice),
                                    new TriplePattern(x, ageTerm, y)),
                          new Minus(new TriplePattern(x, knowsTerm, Alice),
                                    new TriplePattern(x, ageTerm, y)),
                          true),
                arguments(new Minus(new TriplePattern(x, knowsTerm, Alice),
                                    new TriplePattern(x, ageTerm, y)),
                          new Minus(new TriplePattern(x, knowsTerm, Alice),
                                    new TriplePattern(y, ageTerm, x)),
                          false),
                arguments(new Minus(new TriplePattern(x, knowsTerm, Alice),
                                    new TriplePattern(x, ageTerm, y)),
                          new Minus(new TriplePattern(x, knowsTerm, Bob),
                                    new TriplePattern(x, ageTerm, y)),
                          false),
                arguments(new Minus(new TriplePattern(x, knowsTerm, Alice),
                                    new TriplePattern(x, ageTerm, y)),
                          new TriplePattern(x, knowsTerm, Bob),
                          false)
        );
    }

    @ParameterizedTest @MethodSource
    public void testEquals(@NonNull Op a, @NonNull Op b, boolean expected) {
        assertEquals(expected, a.deepEquals(b));
        assertEquals(expected, b.deepEquals(a));
    }

    static Stream<Arguments> testBind() {
        return Stream.of(
        /* 1 */ arguments(new Minus(new TriplePattern(Alice, knowsTerm, x),
                                    new TriplePattern(x, ageTerm, y)),
                          Map.of("x", Bob),
                          new Minus(new TriplePattern(Alice, knowsTerm, Bob),
                                    new TriplePattern(Bob, ageTerm, y))),
        /* 2 */ arguments(new Minus(new TriplePattern(Alice, knowsTerm, x),
                                    new TriplePattern(x, ageTerm, y)),
                          Map.of("x", Bob),
                          new Minus(new TriplePattern(Alice, knowsTerm, Bob),
                                    new TriplePattern(Bob, ageTerm, y))),

        /* 3 */ arguments(new Minus(new Join(new TriplePattern(Alice, knowsTerm, x),
                                             new TriplePattern(Alice, ageTerm, y))       ,
                                    new Filter(new TriplePattern(x, ageTerm, z), "?z = ?y")),
                          Map.of("y", i23),
                          new Minus(new Join(new TriplePattern(Alice, knowsTerm, x),
                                             new TriplePattern(Alice, ageTerm, i23)),
                                     new Filter(new TriplePattern(x, ageTerm, z), "?z = ?y"))),
        /* 4 */ arguments(new Minus(new Join(new TriplePattern(Alice, knowsTerm, x),
                                             new TriplePattern(Alice, ageTerm, y))       ,
                                    new Filter(new TriplePattern(x, ageTerm, z), "?z = ?y")),
                          Map.of("y", i23),
                          new Minus(new Join(new TriplePattern(Alice, knowsTerm, x),
                                             new TriplePattern(Alice, ageTerm, i23)),
                                    new Filter(new TriplePattern(x, ageTerm, z), "?z = ?y"))),

        /* 5 */ arguments(new Minus(new Filter(new TriplePattern(x, ageTerm, y), "?y > 23"),
                                    new Filter(new Join(new TriplePattern(x, knowsTerm, z),
                                                        new TriplePattern(z, ageTerm, w)),
                                               "?w > ?y")),
                          Map.of("y", i25),
                          new Minus(new Filter(new TriplePattern(x, ageTerm, i25),
                                               "\"25\"^^<"+XSD.integer+"> > 23"),
                                    new Filter(new Join(new TriplePattern(x, knowsTerm, z),
                                                        new TriplePattern(z, ageTerm, w)),
                                               "?w > ?y"))),
        /* 6 */ arguments(new Minus(new Filter(new TriplePattern(x, ageTerm, y), "?y > 23"),
                                    new Filter(new Join(new TriplePattern(x, knowsTerm, z),
                                                        new TriplePattern(z, ageTerm, w)),
                                               "?w > ?y")),
                          Map.of("y", i25),
                          new Minus(new Filter(new TriplePattern(x, ageTerm, i25),
                                               i25+" > 23"),
                                    new Filter(new Join(new TriplePattern(x, knowsTerm, z),
                                                        new TriplePattern(z, ageTerm, w)),
                                               "?w > ?y")))
        );
    }

    @SuppressWarnings("UnstableApiUsage") @ParameterizedTest @MethodSource
    public void testBind(@NonNull Minus in, @NonNull Map<String, Term> v2t,
                         @NonNull Minus expected) {
        Op bound = in.bind(new Binding(v2t));
        assertEquals(expected.toString(), bound.toString());
        assertTrue(bound.deepEquals(expected));
        assertTrue(expected.deepEquals(bound));

        for (List<String> permutation : Collections2.permutations(v2t.keySet())) {
            Term[] row = new Term[permutation.size()];
            for (int i = 0; i < row.length; i++) row[i] = v2t.get(permutation.get(i));
            Op listBound = in.bind(new Binding(permutation, row));
            assertEquals(expected.toString(), listBound.toString());
            assertTrue(listBound.deepEquals(expected));
            assertTrue(expected.deepEquals(listBound));
        }
    }

    static Stream<Arguments> testVarNames() {
        return Stream.of(
                arguments(new Minus(new TriplePattern(x, knowsTerm, y),
                                    new TriplePattern(y, ageTerm, z)),
                        asList("x", "y")),
                arguments(new Minus(new TriplePattern(Alice, knowsTerm, Bob),
                                    new TriplePattern(Bob, ageTerm, y)),
                         List.of())
        );
    }

    @ParameterizedTest @MethodSource
    public void testVarNames(@NonNull Minus op, @NonNull List<String> expected) {
        assertEquals(expected, op.outputVars());
    }

    static  Stream<Arguments> testWithChildren() {
        return Stream.of(
                arguments(new Minus(new TriplePattern(x, knowsTerm, y),
                                    new TriplePattern(y, ageTerm, x)),
                          asList(new TriplePattern(Alice, knowsTerm, x),
                                 new TriplePattern(x, ageTerm, y)),
                          new Minus(new TriplePattern(Alice, knowsTerm, x),
                                    new TriplePattern(x, ageTerm, y)))
        );
    }

    @ParameterizedTest @MethodSource
    public void testWithChildren(@NonNull Minus op, @NonNull List<Op> replacements,
                                 @NonNull Minus expected) {
        List<@NonNull String> oldVars = new ArrayList<>(op.outputVars());
        Op replaced = op.withChildren(replacements);
        assertTrue(replaced.deepEquals(expected));
        assertTrue(expected.deepEquals(replaced));
        assertEquals(oldVars, op.outputVars());
    }


    static Stream<Arguments> testInputFilterVarNames() {
        return Stream.of(
                arguments(new Minus(new TriplePattern(Alice, knowsTerm, x),
                                    new TriplePattern(x, ageTerm, y)),
                          Set.of()),
                arguments(new Minus(new TriplePattern(Alice, knowsTerm, x),
                                    new Filter(new TriplePattern(x, ageTerm, y), "?y > 23")),
                          Set.of()),
                // by MINUS semantics, z must be unbound forever, it is not an "input"
                arguments(new Minus(new TriplePattern(Alice, knowsTerm, x),
                                new Filter(new TriplePattern(x, ageTerm, y), "?y > ?z")),
                          Set.of()),
                // by MINUS semantics, y must be unbound forever, it is not an "input"
                arguments(new Minus(new TriplePattern(x, ageTerm, y),
                                    new Filter(new TriplePattern(Bob, ageTerm, z), "?z > ?y")),
                          Set.of()),
                arguments(new Minus(new Filter(new TriplePattern(Alice, ageTerm, x), "?x > ?y"),
                                    new TriplePattern(Bob, ageTerm, x)),
                          Set.of("y")),
                // the bind is direction, y cannot be assigned by the right operand
                arguments(new Minus(new Filter(new TriplePattern(Alice, ageTerm, x), "?x > ?y"),
                                new TriplePattern(Bob, ageTerm, y)),
                          Set.of("y"))
        );
    }

    @ParameterizedTest @MethodSource
    void testInputFilterVarNames(@NonNull Minus op, @NonNull Set<@NonNull String> expected) {
        assertEquals(expected, op.inputVars());
    }
}