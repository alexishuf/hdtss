package com.github.lapesd.hdtss.model;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.List;


public final class Row {
    public static final @NonNull Term @NonNull [] EMPTY = new Term[0];
    public static final List<@NonNull Term @NonNull []> SINGLE_EMPTY;

    static {
        List<?> l = List.of((Object) EMPTY);
        //noinspection unchecked
        SINGLE_EMPTY = (List<Term[]>) l;
    }

    private final Term @NonNull [] terms;
    private int hash = 0;

    public Row(Term @NonNull [] terms) { this.terms = terms; }

    public static @Nullable Term @NonNull [] raw(@Nullable Term... terms) {
        return terms == null ? EMPTY : terms;
    }

    public Term @NonNull [] terms() {
        return terms;
    }

    @Override public boolean equals(Object o) {
        return o == this || (o instanceof Row rhs && Arrays.equals(terms, rhs.terms));
    }

    @Override public int hashCode() {
        if (hash == 0)
            hash = Arrays.hashCode(terms);
        return hash;
    }

    @Override public String toString() {
        return Arrays.toString(terms);
    }
}
