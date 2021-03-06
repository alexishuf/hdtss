package com.github.lapesd.hdtss.model.solutions;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.vocab.XSD;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.EX;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@Tag("fast")
abstract class BaseQuerySolutionsTest {
    protected abstract @NonNull QuerySolutions
    createFor(@NonNull List<@NonNull String> names, @NonNull List<Term[]> rows);

    @ParameterizedTest
    @ValueSource(strings = {"",  "x",  "x y",  "y, x1"})
    void testVarNames(String testData) {
        List<String> list = Arrays.stream(testData.split(" "))
                                   .filter(not(String::isEmpty)).collect(toList());
        QuerySolutions solutions = createFor(list, List.of());
        assertEquals(list, solutions.varNames());
    }

    @Test
    void testIsAsk() {
        assertTrue(createFor(List.of(), List.of()).isAsk());
        assertFalse(createFor(List.of("x"), List.of()).isAsk());
        assertFalse(createFor(asList("x", "y"), List.of()).isAsk());
    }

    @Test
    void testAskResult() {
        var negative = createFor(List.of(), List.of());
        var positive = createFor(List.of(), singletonList(new Term[]{XSD.xtrue}));
        assertTrue(negative.isAsk());
        assertTrue(positive.isAsk());

        assertFalse(negative.askResult());
        assertTrue(positive.askResult());
    }

    static @NonNull Term Alice = new Term("<"+ EX +"Alice>");
    static @NonNull Term Bob = new Term("<"+ EX +"Alice>");
    static @NonNull Term alice = new Term("\"alice\"");
    static @NonNull Term roberto = new Term("\"roberto\"@pt");
    static @NonNull Term i23 = new Term("\"23\"^^<"+XSD.integer+">");


    static Stream<Arguments> solutionData() {
        return Stream.of(
                arguments(List.of(), List.of()),
                arguments(List.of(), singletonList(new Term[]{XSD.xtrue})),
                arguments(List.of("x"), List.of()),
                arguments(List.of("x"), singletonList(new Term[]{XSD.xtrue})),
                arguments(List.of("x"), asList(
                        new Term[]{XSD.xtrue},
                        new Term[]{XSD.xfalse}
                )),
                arguments(asList("x", "y"), asList(
                        new Term[]{Alice, alice},
                        new Term[]{Bob, roberto},
                        new Term[]{Bob, i23}
                )),
                arguments(asList("x", "y"), asList(
                        new Term[]{Alice, alice},
                        new Term[]{Bob, roberto},
                        new Term[]{Bob, i23}
                ))
        );
    }

    @ParameterizedTest
    @MethodSource("solutionData")
    void testGetList(@NonNull List<String> varNames, @NonNull List<Term[]> rows) {
        List<List<Term>> expected = new ArrayList<>(rows.size());
        rows.forEach(a -> expected.add(asList(Arrays.copyOf(a, a.length))));
        var solutions = createFor(varNames, rows);
        assertEquals(new ArrayList<>(varNames), solutions.varNames());
        var actual = solutions.list().stream().map(Arrays::asList).collect(toList());
        assertEquals(expected, actual);
    }

    @ParameterizedTest
    @MethodSource("solutionData")
    void testStream(@NonNull List<String> varNames, @NonNull List<Term[]> rows) {
        List<List<Term>> expected = new ArrayList<>(rows.size());
        rows.forEach(a -> expected.add(asList(Arrays.copyOf(a, a.length))));
        var solutions = createFor(varNames, rows);
        var actual = solutions.stream().map(Arrays::asList).collect(toList());
        assertEquals(expected, actual);
    }


    @ParameterizedTest
    @MethodSource("solutionData")
    void testIterate(@NonNull List<String> varNames, @NonNull List<Term[]> rows) {
        List<List<@NonNull Term>> expected = new ArrayList<>(rows.size());
        rows.forEach(a -> expected.add(asList(Arrays.copyOf(a, a.length))));
        var solutions = createFor(varNames, rows);
        List<List<@NonNull Term>> actual = new ArrayList<>();
        for (Term[] r : solutions)
            actual.add(asList(r));
        assertEquals(expected, actual);
    }

    @ParameterizedTest
    @MethodSource("solutionData")
    void testFlux(@NonNull List<String> varNames, @NonNull List<Term[]> rows) {
        List<List<@Nullable Term>> expected = new ArrayList<>(rows.size());
        rows.forEach(a -> expected.add(asList(Arrays.copyOf(a, a.length))));
        var solutions = createFor(varNames, rows);
        var actual = solutions.flux().toStream().map(Arrays::asList).collect(toList());
        assertEquals(expected, actual);
    }

}