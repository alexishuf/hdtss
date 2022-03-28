package com.github.lapesd.hdtss.sparql.impl;

import com.github.lapesd.hdtss.model.Term;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class ExecutorUtils {
    public static Term @NonNull[]
    project(int @Nullable [] indices, @Nullable Term @NonNull[] row) {
        if (indices == null)
            return row;
        var result = new Term[indices.length];
        for (int i = 0; i < indices.length; i++) {
            int idx = indices[i];
            result[i] = idx < 0 ? null : row[idx];
        }
        return result;
    }

    public static int @NonNull[] findIndices(@NonNull List<@NonNull String> subset,
                                             @NonNull List<@NonNull String> superSet) {
        int[] indices = new int[subset.size()];
        for (int i = 0; i < indices.length; i++)
            indices[i] = superSet.indexOf(subset.get(i));
        return indices;
    }
}
