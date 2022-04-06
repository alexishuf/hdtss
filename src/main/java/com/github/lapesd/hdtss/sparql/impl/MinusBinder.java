package com.github.lapesd.hdtss.sparql.impl;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Minus;
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
public final class MinusBinder {
    @Getter private final @NonNull Op right;
    @Getter private final @NonNull List<@NonNull String> sharedVars;
    @Getter private final int @NonNull[] leftIndices;

    public MinusBinder(@NonNull Minus minus) {
        this.right = minus.children().get(1);
        List<@NonNull String> leftVars = minus.main().outputVars();
        this.sharedVars = new ArrayList<>(leftVars.size());
        int[] indices = new int[leftVars.size()];
        int n = 0;
        for (String name : right.outputVars()) {
            int i = leftVars.indexOf(name);
            if (i >= 0) {
                indices[n++] = i;
                sharedVars.add(name);
            }
        }
        this.leftIndices = n == indices.length ? indices : Arrays.copyOf(indices, n);
    }

    public Op bind(@Nullable Term @NonNull[] leftRow) {
        return right.bind(sharedVars, ExecutorUtils.project(leftIndices, leftRow));
    }
}
