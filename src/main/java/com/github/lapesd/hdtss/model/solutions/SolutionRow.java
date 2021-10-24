package com.github.lapesd.hdtss.model.solutions;

import com.github.lapesd.hdtss.model.Term;
import io.micronaut.core.annotation.Introspected;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;


@Introspected
public record SolutionRow(Term @lombok.NonNull @NonNull [] terms) {
    public static final @NonNull SolutionRow EMPTY = new SolutionRow(new Term[0]);

    public static @NonNull SolutionRow of(Term... terms) {
        return new SolutionRow(terms);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SolutionRow that)) return false;
        return Arrays.equals(terms, that.terms);
    }

    @Override public int hashCode() {
        return Arrays.hashCode(terms);
    }

    @Override public @NonNull String toString() {
        return Arrays.toString(terms);
    }
}
