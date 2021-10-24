package com.github.lapesd.hdtss.utils;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.IntStream;

public class BitsetOps {
    public static long @NonNull [] createBitset(int size) {
        return new long[1 + ((size-1) >> 6)];
    }

    /**
     * Set the i-th bit in the bitset bs.
     *
     * @param bs the bitset
     * @param i the index of the bit to set to {@code true}
     * @throws ArrayIndexOutOfBoundsException if {@code i <0 || i >= 64*bs.length}
     */
    public static void set(long @NonNull [] bs, int i) {
        bs[i>>6] |=   0x1L << (i & 63);
    }

    /**
     * Set all bits in the bitset to {@code true}.
     *
     * @param bs bitset to set
     */
    public static void setAll(long @NonNull [] bs) {
        Arrays.fill(bs, ~0L);
    }

    /**
     * Set the i-th bit in bs to {@code false}.
     *
     * @param bs the bitset
     * @param i the bit to clear
     * @throws ArrayIndexOutOfBoundsException if {@code i <0 || i >= 64*bs.length}
     */
    public static void clear(long @NonNull [] bs, int i) {
        bs[i>>6] &= ~(0x1L << (i & 63));
    }

    /**
     * Set all bits to zero.
     *
     * @param bs the bitset to set.
     */
    public static void clearAll(long @NonNull [] bs) {
        Arrays.fill(bs, 0);
    }

    /**
     * Get the value of the i-th bit in the bitset.
     *
     * @param bs the bitset
     * @param i the index of the bit to read
     * @return true iff the i-th bit in the bitset is set
     * @throws ArrayIndexOutOfBoundsException if {@code i <0 || i >= 64*bs.length}
     */
    public static boolean get(long @NonNull[] bs, int i) {
        return (bs[i>>6] & 0x1L << (i & 63)) != 0;
    }

    /**
     * Get the first bit at an index {@code i >= from} such that {@code get(bs, i) == true}.
     *
     * @param bs the bitset
     * @param from the minimum index to find a set bit
     * @return some index {@code i >= from} whose bit is set or -1 if there is no such {@code i}.
     */
    public static int nextSet(long @NonNull[] bs, int from) {
        if (from < 0)
            return -1;
        int wIdx = from>>6;
        if (wIdx >= bs.length)
            return -1;

        long word = bs[wIdx] & (~0L << from);
        while (true) {
            if (word != 0)
                return (wIdx<<6) + Long.numberOfTrailingZeros(word);
            else if (++wIdx == bs.length)
                return -1;
            word = bs[wIdx];
        }
    }

    /**
     * Get the first bit at or after {@code from} that is not set.
     *
     * @param bs the bitset where to look up the clear bit
     * @param from the minimum index of the clear bit
     * @return The lower {@code i >= from} such that {@code !get(bs, i)} or bs.length if there
     *         is no such i
     */
    public static int nextClear(long @NonNull [] bs, int from) {
        if (from < 0)
            return bs.length << 6;
        int wIdx = from >> 6;
        if (wIdx >= bs.length)
            return bs.length << 6;
        long word = ~bs[wIdx] & (~0L << from);
        while (true) {
            if (word != 0)
                return (wIdx << 6) + Long.numberOfTrailingZeros(word);
            else if (++wIdx == bs.length)
                return bs.length << 6;
            word = ~bs[wIdx];
        }
    }

    /**
     * Get the number of set bits in the bitset
     *
     * @param bs the bitset
     * @return the number of 1-bits.
     */
    public static int cardinality(long @NonNull [] bs) {
        int sum = 0;
        for (long word : bs) {
            sum += Long.bitCount(word);
        }
        return sum;
    }

    /**
     * Clear all bits in lhs that are not set in rhs.
     *
     * See {@link java.util.Collection#retainAll(Collection)}.
     *
     * @param lhs left-hand bitset to mutated
     * @param rhs right-hand bitset to be read
     */
    public static void retainAll(long @NonNull [] lhs, long @NonNull [] rhs) {
        int common = Math.min(lhs.length, rhs.length);
        for (int i = 0; i < common; i++) lhs[i] &= rhs[i];
        Arrays.fill(lhs, common, lhs.length, 0);
    }

    /**
     * Create a new bitset with the intersection of the given two.
     *
     * @param lhs left-hand bitset to be read (can be swapped with rhs, as operation is commutative)
     * @param rhs right-hand bitset to be read
     * @return a new bitset with the intersection of {@code lhs} and {@code rhs}. Its size will
     *         be {@code Math.max(lhs.length, rhs.length)}
     */
    public static long @NonNull [] intersection(long @NonNull [] lhs, long @NonNull [] rhs) {
        long[] copy = Arrays.copyOf(lhs, Math.max(lhs.length, rhs.length));
        retainAll(copy, rhs);
        return copy;
    }

    /**
     * Add all 1-bits from rhs to lhs.
     *
     * If rhs has 1-bits after lhs.length*64, those will not be added to lhs and an
     * {@link AssertionError} will be thrown if asserts are enabled.
     *
     * @param lhs the bitset to mutate
     * @param rhs the bitset to read
     */
    public static void addAll(long @NonNull [] lhs, long @NonNull [] rhs) {
        int common = Math.min(lhs.length, rhs.length);
        for (int i = 0; i < common; i++) lhs[i] |= rhs[i];
        assert IntStream.range(common, rhs.length).allMatch(i -> rhs[i] == 0)
                : "rhs has 1-bits past lhs.length";
    }

    /**
     * Create a new bitset with the union of the given two.
     *
     * @param lhs left-hand bitset to be read (can be swapped with rhs, as operation is commutative)
     * @param rhs right-hand bitset to be read
     * @return a new bitset with the union of {@code lhs} and {@code rhs}. Its size will
     *         be {@code Math.max(lhs.length, rhs.length)}
     */
    public static long @NonNull [] union(long @NonNull [] lhs, long @NonNull [] rhs) {
        long[] copy = Arrays.copyOf(lhs, Math.max(lhs.length, rhs.length));
        addAll(copy, rhs);
        return copy;
    }

    /**
     * Clear all 1-bits from lhs that are also set in rhs.
     *
     * This corresponds to an {@code l &= ~r}
     *
     * @param lhs the bitset to be mutated
     * @param rhs the bitset with 1-bits to remove from lhs
     */
    public static void removeAll(long @NonNull [] lhs, long @NonNull [] rhs) {
        for (int i = 0, words = Math.min(lhs.length, rhs.length); i < words; i++) lhs[i] &= ~rhs[i];
    }

    /**
     * Create a new bitset which is a copy lhs, with the 1-bits in rhs cleared.
     *
     * @param lhs left-hand bitset whose copy will have bits cleared
     * @param rhs right-hand bitset with 1-bits to remove from lhs copy.
     * @return a new copy of lhs with1-bits from rhs removed. Its size will be
     *         {@code Math.max(lhs.length, rhs.length)}
     */
    public static long @NonNull [] minus(long @NonNull [] lhs, long @NonNull [] rhs) {
        long[] copy = Arrays.copyOf(lhs, Math.max(lhs.length, rhs.length));
        removeAll(copy, rhs);
        return copy;
    }

    /**
     * Remove from lhs its intersection  with rhs and add all 1-bits from rhs that are 0 in lhs.
     *
     * This is equivalent to a xor operation.
     *
     * If rhs has 1-bits after lhs.length*64, those will not be added to lhs and an
     * {@link AssertionError} will be thrown if asserts are enabled.
     *
     * @param lhs the bitset to be mutated
     * @param rhs the bitset to be read.
     */
    public static void toSymmetricDiff(long @NonNull[] lhs, long @NonNull [] rhs) {
        int common = Math.min(lhs.length, rhs.length);
        for (int i = 0; i < common; i++) lhs[i] ^= rhs[i];
        assert IntStream.range(common, rhs.length).allMatch(i -> rhs[i] != 0)
                : "rhs has 1-bits pasth lhs.length";
    }

    /**
     * Create a new bitset with the symmetric difference (xor) of the given two.
     *
     * @param lhs left-hand bitset to be read (can be swapped with rhs, as operation is commutative)
     * @param rhs right-hand bitset to be read
     * @return a new bitset with the symmetric difference of {@code lhs} and {@code rhs}.
     *         Its size will be {@code Math.max(lhs.length, rhs.length)}
     */
    public static long @NonNull [] symmetricDiff(long @NonNull [] lhs, long @NonNull [] rhs) {
        long[] copy = Arrays.copyOf(lhs, Math.max(lhs.length, rhs.length));
        toSymmetricDiff(copy, rhs);
        return copy;
    }

    /**
     * Invert all bits in the given bitset
     * @param bs the bitset to mutate
     */
    public static void flip(long @NonNull [] bs) {
        for (int i = 0; i < bs.length; i++) bs[i] = ~bs[i];
    }

    /**
     * Create a copy of the given bitset with all bits flipped.
     *
     * @param bs the bitset to copy and flip.
     * @return a new bitset with all bits from {@code bs} but flipped.
     */
    public static long @NonNull [] complement(long @NonNull [] bs) {
        long[] copy = Arrays.copyOf(bs, bs.length);
        flip(copy);
        return copy;
    }
}
