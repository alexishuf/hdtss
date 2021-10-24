package com.github.lapesd.hdtss.utils;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@SuppressWarnings("unused")
@Tag("fast")
class BitsetOpsTest  {
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4, 32, 63, 64, 65, 127, 128, 129, 191, 192, 193})
    void testSetClearAndGet(int size) {
        long[] bs = BitsetOps.createBitset(size);
        //fill from 0 -> size
        for (int i = 0; i < size; i++) {
            BitsetOps.set(bs, i);
            for (int j = 0; j <= i; j++)
                assertTrue(BitsetOps.get(bs, j), "i="+i+", j="+j);
            for (int j = i+1; j < size; j++)
                assertFalse(BitsetOps.get(bs, j), "i="+i+", j="+j);
            assertEquals(i+1, BitsetOps.cardinality(bs));
        }
        //clear from 0 -> size
        for (int i = 0; i < size; i++) {
            BitsetOps.clear(bs, i);
            for (int j = 0; j <= i; j++)
                assertFalse(BitsetOps.get(bs, j), "i="+i+", j="+j);
            for (int j = i+1; j < size; j++)
                assertTrue(BitsetOps.get(bs, j), "i="+i+", j="+j);
            assertEquals(size-i-1, BitsetOps.cardinality(bs));
        }
        //fill from size -> 0
        for (int i = size-1; i >= 0; i--) {
            BitsetOps.set(bs, i);
            for (int j = i; j < size; j++)
                assertTrue(BitsetOps.get(bs, j),  "i="+i+", j="+j);
            for (int j = i-1; j >= 0; j--)
                assertFalse(BitsetOps.get(bs, j),  "i="+i+", j="+j);
            assertEquals(size-i, BitsetOps.cardinality(bs));
        }
        //clear from size -> 0
        for (int i = size-1; i >= 0; i--) {
            BitsetOps.clear(bs, i);
            for (int j = i; j < size; j++)
                assertFalse(BitsetOps.get(bs, j),  "i="+i+", j="+j);
            for (int j = 0; j < i; j++)
                assertTrue(BitsetOps.get(bs, j), "i="+i+", j="+j);
            assertEquals(i, BitsetOps.cardinality(bs));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 4, 63, 64, 65, 127, 128, 129})
    void testNextSet(int size) {
        long[] bs = BitsetOps.createBitset(size);
        for (int i = 0; i < size; i += 2)
            BitsetOps.set(bs, i);
        List<Integer> actual = new ArrayList<>();
        for (int i = BitsetOps.nextSet(bs, 0); i >= 0 ; i = BitsetOps.nextSet(bs, i+1))
            actual.add(i);
        var expected = IntStream.range(0, size).filter(i -> (i&1) == 0).boxed().collect(toList());
        assertEquals(expected, actual);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 4, 63, 64, 65, 127, 128, 129})
    void testNextClear(int size) {
        long[] bs = BitsetOps.createBitset(size);
        for (int i = 0; i < size; i += 2)
            BitsetOps.set(bs, i);
        List<Integer> actual = new ArrayList<>();
        for (int i = BitsetOps.nextClear(bs, 0); i < size ; i = BitsetOps.nextClear(bs, i+1))
            actual.add(i);
        var expected = IntStream.range(0, size).filter(i -> (i&1) == 1).boxed().collect(toList());
        assertEquals(expected, actual);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4, 63, 64, 65, 127, 128, 129})
    void testCardinality(int size) {
        long[] bs = BitsetOps.createBitset(size);
        for (int i = 0; i < size; i++) {
            BitsetOps.set(bs, i);
            assertEquals(i+1, BitsetOps.cardinality(bs), "i="+i);
        }
        int expected = size;
        for (int i = 0; i < size; i += 2) {
            BitsetOps.clear(bs, i);
            assertEquals(--expected, BitsetOps.cardinality(bs));
        }
        for (int i = 1; i < size; i += 2) {
            BitsetOps.clear(bs, i);
            assertEquals(--expected, BitsetOps.cardinality(bs));
        }
        assertEquals(0, BitsetOps.cardinality(bs));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4, 63, 64, 65, 127, 128, 129})
    void testClearAll(int size) {
        long[] bs = BitsetOps.createBitset(size);
        for (int i = 0; i < size; i++)
            BitsetOps.set(bs, i);
        assertEquals(size, BitsetOps.cardinality(bs));
        BitsetOps.clearAll(bs);
        assertEquals(0, BitsetOps.cardinality(bs));

    }

    static @NonNull Stream<Arguments> testIntersect() {
        return Stream.of(
                arguments(new long[]{0xaa55aa55},
                          new long[]{0x55555555},
                          new long[]{0x00550055}),
                arguments(new long[]{0x000f, 0x00f0, 0x0f00},
                          new long[]{0xfff1, 0xff2f, 0xf3ff},
                          new long[]{0x0001, 0x0020, 0x0300}),
                arguments(new long[]{0x1, 0x2},
                          new long[]{0x3},
                          new long[]{0x1, 0x0}),
                arguments(new long[]{0x3},
                          new long[]{0x1, 0x3},
                          new long[]{0x1, 0x0})
        );
    }

    @ParameterizedTest @MethodSource
    void testIntersect(long[] l, long[] r, long[] ex) {
        long[] lCopy = Arrays.copyOf(l, l.length);
        long[] rCopy = Arrays.copyOf(r, r.length);
        assertArrayEquals(ex, BitsetOps.intersection(l, r));
        assertArrayEquals(lCopy, l);
        assertArrayEquals(rCopy, r);
    }

    static @NonNull Stream<Arguments> testMinus() {
        return Stream.of(
                arguments(new long[]{0x3}, new long[]{0x1}, new long[]{0x2}),
                arguments(new long[]{0x03, 0x30},
                          new long[]{0x01, 0x20},
                          new long[]{0x02, 0x10}),
                arguments(new long[]{0x03},
                          new long[]{0x01, 0x20},
                          new long[]{0x02, 0x00}),
                arguments(new long[]{0x03, 0x30},
                          new long[]{0x01},
                          new long[]{0x02, 0x30})
        );
    }

    @ParameterizedTest @MethodSource
    void testMinus(long[] l, long[] r, long[] ex) {
        long[] lCopy = Arrays.copyOf(l, l.length);
        long[] rCopy = Arrays.copyOf(r, r.length);
        assertArrayEquals(ex, BitsetOps.minus(l, r));
        assertArrayEquals(lCopy, l);
        assertArrayEquals(rCopy, r);
    }

    static @NonNull Stream<Arguments> testUnion() {
        return Stream.of(
                arguments(new long[]{0x1}, new long[]{0x2}, new long[]{0x3}),
                arguments(new long[]{0x01, 0x40},
                          new long[]{0x02, 0x20},
                          new long[]{0x03, 0x60}),
                arguments(new long[]{0x01, 0x40},
                          new long[]{0x02},
                          new long[]{0x03, 0x40}),
                arguments(new long[]{0x01},
                          new long[]{0x02, 0x20},
                          new long[]{0x03, 0x20})
        );
    }

    @ParameterizedTest @MethodSource
    void testUnion(long[] l, long[] r, long[] ex) {
        long[] lCopy = Arrays.copyOf(l, l.length);
        long[] rCopy = Arrays.copyOf(r, r.length);
        assertArrayEquals(ex, BitsetOps.union(l, r));
        assertArrayEquals(lCopy, l);
        assertArrayEquals(rCopy, r);
    }

    static @NonNull Stream<Arguments> testComplement() {
        return Stream.of(
                arguments(new long[]{0x1}, new long[]{~0x1}),
                arguments(new long[]{0x1, ~0x2}, new long[]{~0x1, 0x2})
        );
    }

    @ParameterizedTest @MethodSource
    void testComplement(long[] bs, long[] ex) {
        long[] copy = Arrays.copyOf(bs, bs.length);
        assertArrayEquals(ex, BitsetOps.complement(bs));
        assertArrayEquals(copy, bs);
    }

    static @NonNull Stream<Arguments> testSymmetricDiff() {
        return Stream.of(
                arguments(new long[]{0xff0}, new long[]{0x0ff}, new long[]{0xf0f}),
                arguments(new long[]{0x110, 0x011},
                          new long[]{0x011},
                          new long[]{0x101, 0x011}),
                arguments(new long[]{0x011},
                          new long[]{0x110, 0x011},
                          new long[]{0x101, 0x011})
        );
    }

    @ParameterizedTest @MethodSource
    void testSymmetricDiff(long[] l, long[] r, long[] ex) {
        long[] lCopy = Arrays.copyOf(l, l.length);
        long[] rCopy = Arrays.copyOf(r, r.length);
        assertArrayEquals(ex, BitsetOps.symmetricDiff(l, r));
        assertArrayEquals(lCopy, l);
        assertArrayEquals(rCopy, r);
    }
}