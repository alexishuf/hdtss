package com.github.lapesd.hdtss.utils;

import com.github.lapesd.hdtss.model.Term;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.github.lapesd.hdtss.TestVocab.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class BindingTest {
    private record Prototype(@NonNull List<@NonNull String> varNames,
                             Term @NonNull[] terms,
                             @Nullable Predicate<@NonNull String> filter) {
        public @NonNull Binding create() {
            return filter == null ? new Binding(varNames, terms)
                                  : new Binding(varNames, terms).filter(filter);
        }
    }

    static Stream<Arguments> full() {
        return Stream.of(
                arguments(new Prototype(List.of(),    new Term[0],       null)),
                arguments(new Prototype(List.of("x"), new Term[]{Alice}, null)),
                arguments(new Prototype(List.of("x", "y"), new Term[]{Alice, Bob}, null)),
                arguments(new Prototype(List.of("y", "x"), new Term[]{Alice, Bob}, null)),
                arguments(new Prototype(List.of("1", "x"), new Term[]{Alice, Bob}, null)),
                arguments(new Prototype(List.of("y", "x", "1"), new Term[]{Alice, Bob, Charlie}, null))
        );
    }

    static Stream<Arguments> partial() {
        Predicate<String> isEven = s -> s.matches("\\d+") && Integer.parseInt(s) % 2 == 0;
        return Stream.of(
                arguments(new Prototype(List.of(),                      new Term[0],                           isEven), List.of()),
                arguments(new Prototype(List.of("1"),                   new Term[]{Alice},                     isEven), List.of()),
                arguments(new Prototype(List.of("1", "2"),              new Term[]{Alice, Bob},                isEven), List.of(1)),
                arguments(new Prototype(List.of("1", "2", "3"),         new Term[]{Alice, Bob, Charlie},       isEven), List.of(1)),
                arguments(new Prototype(List.of("1", "2", "3", "4"),    new Term[]{Alice, Bob, Charlie, Dave}, isEven), List.of(1, 3)),
                arguments(new Prototype(List.of("2"),      new Term[]{Alice},      isEven), List.of(0)),
                arguments(new Prototype(List.of("2", "4"), new Term[]{Alice, Bob}, isEven), List.of(0, 1)),
                arguments(new Prototype(List.of("2", "3"), new Term[]{Alice, Bob}, isEven), List.of(0))
        );
    }

    @ParameterizedTest @MethodSource("full")
    void testSize(@NonNull Prototype prototype) {
        Binding row = prototype.create();
        assertEquals(prototype.terms.length,               row.size());
        assertEquals(prototype.terms.length == 0, row.isEmpty());
    }

    @ParameterizedTest @MethodSource("full")
    void testIndexOf(@NonNull Prototype prototype) {
        Binding row = prototype.create();
        for (int i = 0; i < prototype.varNames.size(); i++) {
            assertEquals(i, row.indexOf(prototype.varNames.get(i)));
            assertTrue(row.contains(prototype.varNames.get(i)));
        }
    }

    @ParameterizedTest @MethodSource("full")
    void testGetByPosition(@NonNull Prototype prototype) {
        Binding row = prototype.create();
        for (int i = 0; i < prototype.varNames.size(); i++) {
            assertEquals(prototype.varNames.get(i), row.var(i));
            assertEquals(prototype.terms[i], row.get(i));
        }
    }

    @ParameterizedTest @MethodSource("full")
    void testGetByName(@NonNull Prototype prototype) {
        Binding row = prototype.create();
        for (int i = 0; i < prototype.varNames.size(); i++) {
            assertEquals(prototype.terms[i], row.get(prototype.varNames.get(i)));
            assertEquals(prototype.terms[i], row.get(prototype.varNames.get(i), null));
            assertEquals(prototype.terms[i], row.get(prototype.varNames.get(i), Alice));
        }
    }

    @ParameterizedTest @MethodSource("full")
    void testIntersectItself(@NonNull Prototype prototype) {
        Binding row = prototype.create();
        assertEquals(!row.isEmpty(), row.intersects(prototype.varNames));
    }

    @ParameterizedTest @MethodSource("full")
    void testDoesNotIntersectWithEmpty(@NonNull Prototype prototype) {
        Binding row = prototype.create();
        assertFalse(row.intersects(List.of()));
    }

    @ParameterizedTest @MethodSource("full")
    void testFilterWithoutRemovals(@NonNull Prototype prototype) {
        Binding actual = prototype.create().filter(v -> true);
        assertEquals(prototype.varNames.size(), actual.size());
        assertEquals(prototype.varNames.isEmpty(), actual.isEmpty());
        for (int i = 0, varNamesSize = prototype.varNames.size(); i < varNamesSize; i++) {
            assertTrue(actual.contains(prototype.varNames.get(i)));
            assertEquals(prototype.varNames.get(i), actual.var(i));
            assertEquals(i, actual.indexOf(prototype.varNames.get(i)));
            assertEquals(prototype.terms[i], actual.get(i));
            assertEquals(prototype.terms[i], actual.get(prototype.varNames.get(i)));
        }
    }

    @ParameterizedTest @MethodSource("full")
    void testCreateFromMap(@NonNull Prototype prototype) {
        LinkedHashMap<String, Term> map = new LinkedHashMap<>();
        for (int i = 0; i < prototype.varNames.size(); i++)
            map.put(prototype.varNames.get(i), prototype.terms[i]);
        Binding actual = new Binding(map), expected = prototype.create();
        assertBindingEquals(actual, expected);
    }

    private void assertBindingEquals(Binding actual, Binding expected) {
        assertEquals(expected.toString(), actual.toString());
        assertEquals(expected.size(), actual.size());
        assertEquals(expected.isEmpty(), actual.isEmpty());
        assertArrayEquals(expected.vars(), actual.vars());
        assertArrayEquals(expected.terms(), actual.terms());
        for (int i = 0; i < actual.vars().length; i++) {
            assertEquals(expected.var(i), actual.var(i));
            assertEquals(expected.get(i), actual.get(i));
            String name = actual.vars()[i];
            assertEquals(expected.get(name), actual.get(name));
            assertEquals(expected.contains(name), actual.contains(name));
            assertEquals(expected.indexOf(name), actual.indexOf(name));
        }
    }

    @ParameterizedTest @MethodSource("full")
    void testSetTerms(@NonNull Prototype prototype) {
        Binding expected = prototype.create();
        Binding actual = new Binding(prototype.varNames()).setTerms(prototype.terms());
        assertBindingEquals(actual, expected);
    }

    @ParameterizedTest @MethodSource("partial")
    void testPartialSize(@NonNull Prototype prototype, List<Integer> selected) {
        Binding row = prototype.create();
        assertEquals(selected.size(), row.size());
        assertEquals(selected.isEmpty(), row.isEmpty());
    }

    @ParameterizedTest @MethodSource("partial")
    void testIteratePartial(@NonNull Prototype prototype, List<Integer> selected) {
        Binding row = prototype.create();
        for (int i = 0; i < row.size(); i++) {
            assertEquals(prototype.terms[selected.get(i)], row.get(i));
            assertEquals(prototype.varNames.get(selected.get(i)), row.var(i));
        }
    }

    @ParameterizedTest @MethodSource("partial")
    void testFindVar(@NonNull Prototype prototype, List<Integer> selected) {
        Binding row = prototype.create();
        for (int i = 0; i < prototype.varNames.size(); i++) {
            boolean contains = selected.contains(i);
            String varName = prototype.varNames.get(i);
            assertEquals(contains, row.contains(varName));
            assertEquals(contains ? selected.indexOf(i) : -1, row.indexOf(varName));
            assertEquals(contains ? prototype.terms[i] : null,
                         row.get(varName, null));
            if (contains)
                assertEquals(prototype.terms[i], row.get(varName));
            else
                assertThrows(Binding.NoSuchVarException.class, () -> row.get(varName));
        }
    }

    @SuppressWarnings("unused")
    static Stream<Arguments> testUnbound() {
        return Stream.of(
                arguments(List.of(), List.of(), List.of()),
                arguments(List.of(), List.of("x"), List.of("x")),
                arguments(List.of(), List.of("x", "y"), List.of("x", "y")),
                arguments(List.of("z"), List.of("x", "y"), List.of("x", "y")),
                arguments(List.of("x"), List.of("x", "y"), List.of("y")),
                arguments(List.of("y"), List.of("x", "y"), List.of("x")),
                arguments(List.of("z", "x"), List.of("x", "y"), List.of("y")),
                arguments(List.of("z", "y"), List.of("x", "y"), List.of("x"))
        );
    }

    @ParameterizedTest @MethodSource
    void testUnbound(List<String> bindingVars, List<String> inVars, List<String> expected) {
        assertEquals(expected, new Binding(bindingVars).unbound(inVars));

        Binding binding = new Binding(bindingVars);
        Arrays.fill(binding.terms(), Alice);
        assertEquals(expected, binding.unbound(inVars));
    }

    @Test
    void testCopy() {
        var vars = new String[] {"x"};
        var terms = new Term[] {Alice};
        Binding source = new Binding(vars, terms);
        Binding copy = new Binding(source);
        copy.terms()[0] = Bob;
        assertArrayEquals(new Term[]{Alice}, terms);
    }
}