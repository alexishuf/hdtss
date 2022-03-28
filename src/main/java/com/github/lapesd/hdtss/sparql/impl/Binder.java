package com.github.lapesd.hdtss.sparql.impl;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Op;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@ToString @Accessors(fluent = true)
public final class Binder {
    @Getter private final @NonNull Op right;
    @Getter private final @NonNull List<@NonNull String> sharedVars;
    @Getter private final int @NonNull[] leftIndices;

    public Binder(@NonNull Op left, @NonNull Op right) {
        this(right, left.varNames(), right.varNames());
    }

    public Binder(@NonNull Op right, @NonNull List<@NonNull String> leftVars,
                  @NonNull List<@NonNull String> rightVars) {
        this.right = right;
        int cap = Math.min(leftVars.size(), rightVars.size());
        this.sharedVars = new ArrayList<>(cap);
        int[] indices = new int[cap];
        int n = 0;
        for (String name : rightVars) {
            int i = leftVars.indexOf(name);
            if (i >= 0) {
                indices[n++] = i;
                sharedVars.add(name);
            }
        }
        leftIndices = n == cap ? indices : Arrays.copyOf(indices, n);
    }

    public Op bind(@Nullable Term @NonNull[] leftRow) {
        return right.bind(sharedVars, ExecutorUtils.project(leftIndices, leftRow));
    }
}
