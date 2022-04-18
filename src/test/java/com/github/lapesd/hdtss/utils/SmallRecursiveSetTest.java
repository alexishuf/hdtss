package com.github.lapesd.hdtss.utils;

import com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@Tag("fast")
class SmallRecursiveSetTest {
    static Stream<Arguments> uniqueElements() {
        return Stream.of(
                List.of(),
                List.of("a"),
                List.of("a", "b"),
                List.of("a", "b", "c"),
                List.of("a", "b", "c", "d"),
                IntStream.range(0, 15).mapToObj(String::valueOf).toList(),
                IntStream.range(0, 16).mapToObj(String::valueOf).toList(),
                IntStream.range(0, 73).mapToObj(String::valueOf).toList()
        ).map(Arguments::arguments);
    }

    @ParameterizedTest @MethodSource("uniqueElements")
    void testAddAndIterateAndRemoveAllWithSmallerCapacity(@NonNull List<@NonNull String> elements) {
        var set = new SmallRecursiveSet<String>(4);
        assertEquals(!elements.isEmpty(), set.addAll(elements));
        assertEquals(elements.size(), set.size());
        assertEquals(elements, new ArrayList<>(set));

        assertEquals(!elements.isEmpty(), set.removeAll(elements));
        assertEquals(0, set.size());
        assertEquals(List.of(), new ArrayList<>(set));
    }

    @ParameterizedTest @MethodSource("uniqueElements")
    void testCopyAndIterateThenRemove(@NonNull List<@NonNull String> elements) {
        var set = SmallRecursiveSet.fromDistinct(elements);
        assertEquals(elements.size(), set.size());
        assertEquals(elements, new ArrayList<>(set));
        for (int i = 0; i < elements.size(); i++) {
            assertTrue(set.remove(elements.get(i)), "i="+i);
            assertEquals(elements.subList(i+1, elements.size()), new ArrayList<>(set), "i="+i);
        }
    }

    @ParameterizedTest @MethodSource("uniqueElements")
    void testAddAndIterate(@NonNull List<@NonNull String> elements) {
        var set = new SmallRecursiveSet<String>();
        for (String e : elements)
            assertTrue(set.add(e));
        assertEquals(elements, new ArrayList<>(set));
    }

    @ParameterizedTest @MethodSource("uniqueElements")
    void testAddAndIterateReverse(@NonNull List<@NonNull String> elements) {
        var set = new SmallRecursiveSet<String>();
        for (int i = elements.size()-1; i >= 0; i--)
            assertTrue(set.add(elements.get(i)));
        assertEquals(Lists.reverse(elements), new ArrayList<>(set));
    }

    @ParameterizedTest @MethodSource("uniqueElements")
    void testAddTwiceAndIterate(@NonNull List<@NonNull String> elements) {
        var set = new SmallRecursiveSet<String>();
        for (String e : elements) assertTrue(set.add(e));
        for (int i = elements.size()-1; i >= 0; i--) assertTrue(set.add(elements.get(i)));
        assertEquals(elements, new ArrayList<>(set));
    }

    @ParameterizedTest @MethodSource("uniqueElements")
    void testAddAndRemoveTwice(@NonNull List<@NonNull String> elements) {
        var set = new SmallRecursiveSet<String>();
        //add twice
        for (String e : elements) assertTrue(set.add(e));
        for (int i = elements.size()-1; i >= 0; i--) assertTrue(set.add(elements.get(i)));
        assertEquals(elements, new ArrayList<>(set));

        //remove once, with no effect
        for (String e : elements) assertTrue(set.remove(e));
        assertEquals(elements, new ArrayList<>(set));

        //remove again, clearing the list
        assertEquals(!elements.isEmpty(), set.removeAll(elements));
        assertEquals(List.of(), new ArrayList<>(set));
    }

    @ParameterizedTest @MethodSource("uniqueElements")
    void testAddTwiceAndClear(@NonNull List<@NonNull String> elements) {
        var set = new SmallRecursiveSet<String>();
        //add twice
        assertEquals(!elements.isEmpty(), set.addAll(elements));
        for (int i = elements.size()-1; i >= 0; i--) assertTrue(set.add(elements.get(i)));
        assertEquals(elements, new ArrayList<>(set));

        //clear
        set.clear();
        assertEquals(List.of(), new ArrayList<>(set));

        //still works after clear...
        assertEquals(!elements.isEmpty(), set.addAll(elements));
        assertEquals(elements, new ArrayList<>(set));
    }

    @Test
    void testUnitCapacity() {
        var set = new SmallRecursiveSet<String>(1);
        assertTrue(set.add("a"));
        assertTrue(set.add("b"));
        assertEquals(2, set.size());
        assertEquals(List.of("a", "b"), new ArrayList<>(set));
    }

    @Test
    void testCreateHole() {
        var set = new SmallRecursiveSet<String>();
        assertTrue(set.add("a"));
        assertTrue(set.add("b"));
        assertEquals(List.of("a", "b"), new ArrayList<>(set));

        // create hole
        assertTrue(set.remove("a"));
        assertFalse(set.isEmpty());
        assertEquals(1, set.size());
        assertEquals(List.of("b"), new ArrayList<>(set));

        //fill hole
        assertTrue(set.add("c"));
        assertEquals(2, set.size());
        assertEquals(List.of("c", "b"), new ArrayList<>(set));

        // do not overwrite "b"
        assertTrue(set.add("d"));
        assertEquals(3, set.size());
        assertEquals(List.of("c", "b", "d"), new ArrayList<>(set));
    }

    @Test
    void testTwoHoles() {
        var set = new SmallRecursiveSet<String>();
        assertTrue(set.add("a"));
        assertTrue(set.add("b"));
        assertTrue(set.add("c"));
        assertTrue(set.add("d"));
        assertEquals(4, set.size());
        assertEquals(List.of("a", "b", "c", "d"), new ArrayList<>(set));

        // remove two strings, removing x is a no-op
        assertTrue(set.removeAll(List.of("c", "x", "b")));
        assertEquals(2, set.size());
        assertEquals(List.of("a", "d"), new ArrayList<>(set));

        // recursive add a, no visible effect
        assertTrue(set.add("a"));
        assertEquals(2, set.size());
        assertEquals(List.of("a", "d"), new ArrayList<>(set));

        // recursive remove a, no visible effect
        assertTrue(set.remove("a"));
        assertEquals(2, set.size());
        assertEquals(List.of("a", "d"), new ArrayList<>(set));

        // fill holes
        assertTrue(set.add("1"));
        assertTrue(set.add("2"));
        assertEquals(4, set.size());
        assertEquals(List.of("a", "1", "2", "d"), new ArrayList<>(set));
    }

    @ParameterizedTest @MethodSource("uniqueElements")
    void testContains(@NonNull List<@NonNull String> elements) {
        var set = SmallRecursiveSet.fromDistinct(elements);
        for (String e : elements) {
            assertTrue(set.contains(e));
            assertFalse(set.contains(".."+e));
            assertFalse(set.contains(e+".."));
        }
    }

    @ParameterizedTest @MethodSource("uniqueElements")
    void testContainsAfterHole(@NonNull List<@NonNull String> elements) {
        for (int i = 0; i < elements.size(); i++) {
            var set = SmallRecursiveSet.fromDistinct(elements);
            assertTrue(set.remove(elements.get(i)));
            for (int j = 0; j < elements.size(); j++) {
                assertEquals(j != i, set.contains(elements.get(j)));
                assertEquals(j != i, set.containsAny(List.of(elements.get(j))));
                assertEquals(j != i, set.containsAny(Set .of(elements.get(j))));
                assertFalse(set.contains(".."+elements.get(j)));
                assertFalse(set.contains(elements.get(j)+".."));
            }
        }
    }

    @ParameterizedTest @MethodSource("uniqueElements")
    void testFullIntersection(@NonNull List<@NonNull String> elements) {
        var set = new SmallRecursiveSet<String>();
        assertEquals(!elements.isEmpty(), set.addAll(elements));

        var hashSet = new HashSet<>(elements);
        var treeSet = new TreeSet<>(elements);
        assertEquals(hashSet, set.intersectionSet(elements));
        assertSame(elements.isEmpty() ? Set.of() : hashSet,  set.intersectionSet(hashSet));
        assertSame(elements.isEmpty() ? Set.of() : treeSet,  set.intersectionSet(treeSet));

        assertSame(elements.isEmpty() ? List.of() : elements, set.intersectionList(elements));
        assertEquals(new ArrayList<>(hashSet), set.intersectionList(hashSet));
        assertEquals(new ArrayList<>(treeSet), set.intersectionList(treeSet));
    }

    @ParameterizedTest @MethodSource("uniqueElements")
    void testEmptyIntersection(@NonNull List<@NonNull String> elements) {

        var set = new SmallRecursiveSet<String>();
        assertEquals(!elements.isEmpty(), set.addAll(elements));

        List<String> alternative = elements.stream().map(s -> ".." + s).toList();

        assertSame(Set.of(), set.intersectionSet(List.of()));
        assertSame(Set.of(), set.intersectionSet(Set.of()));
        assertSame(Set.of(), set.intersectionSet(alternative));
        assertSame(Set.of(), set.intersectionSet(new HashSet<>(alternative)));
        assertSame(Set.of(), set.intersectionSet(new TreeSet<>(alternative)));

        assertSame(List.of(), set.intersectionList(List.of()));
        assertSame(List.of(), set.intersectionList(Set.of()));
        assertSame(List.of(), set.intersectionList(alternative));
        assertSame(List.of(), set.intersectionList(new HashSet<>(alternative)));
        assertSame(List.of(), set.intersectionList(new TreeSet<>(alternative)));
    }

    private void assertSubsetEquals(@NonNull Set<@NonNull String> expected,
                                    @NonNull Collection<@NonNull String> actual) {
        assertEquals(expected.size(), actual.size());
        assertEquals(expected, new HashSet<>(actual));
    }

    @ParameterizedTest @MethodSource("uniqueElements")
    void testPartialIntersection(@NonNull List<@NonNull String> elements) {
        var set = new SmallRecursiveSet<String>();
        assertEquals(!elements.isEmpty(), set.addAll(elements));

        for (int i = 0; i < elements.size(); i++) {
            List<@NonNull String> subList = elements.subList(0, i);
            Set<@NonNull String> subSet = new HashSet<>(subList);
            assertSubsetEquals(subSet, set.intersectionSet(subList));
            assertSubsetEquals(subSet, set.intersectionSet(subSet));
            assertSubsetEquals(subSet, set.intersectionSet(new TreeSet<>(subList)));
            assertSubsetEquals(subSet, set.intersectionSet(Lists.reverse(subList)));

            assertSubsetEquals(subSet, set.intersectionList(subList));
            assertSubsetEquals(subSet, set.intersectionList(subSet));
            assertSubsetEquals(subSet, set.intersectionList(new TreeSet<>(subList)));
            assertSubsetEquals(subSet, set.intersectionList(Lists.reverse(subList)));
        }
    }

    @ParameterizedTest @MethodSource("uniqueElements")
    void testIntersectionToleratesNull(@NonNull List<@NonNull String> elements) {
        var set = SmallRecursiveSet.fromDistinct(elements);
        ArrayList<String> list = new ArrayList<>();
        list.add(null);
        assertSame(Set.of(), set.intersectionSet(list));
        assertSame(List.of(), set.intersectionList(list));
    }

    @ParameterizedTest @MethodSource("uniqueElements")
    void testContainsAnyToleratesNull(@NonNull List<@NonNull String> elements) {
        var set = SmallRecursiveSet.fromDistinct(elements);
        ArrayList<String> list = new ArrayList<>();
        list.add(null);
        assertFalse(set.containsAny(list));
    }

    @ParameterizedTest @MethodSource("uniqueElements")
    void testFromDistinct(@NonNull List<@NonNull String> elements) {
        var set = SmallRecursiveSet.fromDistinct(elements);
        assertEquals(elements.size(), set.size());
        assertEquals(elements.isEmpty(), set.isEmpty());
        assertEquals(elements, new ArrayList<>(set));
        assertEquals(elements.toString(), set.toString());
    }

    @ParameterizedTest @MethodSource("uniqueElements")
    void testFromDistinctAddAgainAndRemoveTwice(@NonNull List<@NonNull String> elements) {
        var set = SmallRecursiveSet.fromDistinct(elements);
        assertEquals(elements, new ArrayList<>(set));

        //increment
        assertEquals(!elements.isEmpty(), set.addAll(elements));
        assertEquals(elements, new ArrayList<>(set));

        //decrement
        assertEquals(!elements.isEmpty(), set.removeAll(elements));
        assertEquals(elements, new ArrayList<>(set));

        //effective remove, backwards
        for (int i = elements.size()-1; i >= 0; i--) {
            assertTrue(set.remove(elements.get(i)), "i="+i);
            assertEquals(elements.subList(0, i), new ArrayList<>(set), "i="+i);
        }
    }


    @ParameterizedTest @MethodSource("uniqueElements")
    void testFromDistinctAndAddMore(@NonNull List<@NonNull String> elements) {
        var set = SmallRecursiveSet.fromDistinct(elements);
        assertTrue(set.add("novel"));

        assertFalse(set.isEmpty());
        assertEquals(elements.size()+1, set.size());
        assertEquals(Stream.concat(elements.stream(), Stream.of("novel")).toList(),
                     new ArrayList<>(set));
    }

}