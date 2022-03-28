package com.github.lapesd.hdtss.sparql.impl.minus;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Minus;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Predicate;

public interface MinusStrategy {
    /**
     * Create a {@link Predicate} that accepts receives row instances from to the
     * {@link Minus#outer()} node and return whether they should be accepted as solutions
     * to the {@link Minus} operator.
     *
     * @param minus the {@link Minus} operator being evalauted
     * @return A {@link Predicate} that accepts only {@link Minus#outer()} solutions that
     *        are not eliminated by {@link Minus#inner()}.
     */
    @NonNull Predicate<@Nullable Term @NonNull[]> createFilter(@NonNull Minus minus);
}
