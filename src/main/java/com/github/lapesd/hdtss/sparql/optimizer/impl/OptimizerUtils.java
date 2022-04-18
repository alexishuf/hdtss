package com.github.lapesd.hdtss.sparql.optimizer.impl;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.sparql.optimizer.Optimizer;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;

public class OptimizerUtils {
    public static @NonNull Op optimizeChildren(@NonNull Op op,
                                               @NonNull Optimizer optimizer) {
        List<@NonNull Op> replacements = null;
        @NonNull List<@NonNull Op> children = op.children();
        for (int i = 0, size = children.size(); i < size; i++) {
            Op child = children.get(i);
            Op replacement = optimizer.optimize(child);
            if (replacements != null) {
                replacements.add(replacement);
            } else if (replacement != child) {
                replacements = new ArrayList<>(size);
                for (int j = 0; j < i; j++)
                    replacements.add(children.get(j));
                replacements.add(replacement);
            }
        }
        return replacements == null ? op : op.withChildren(replacements);
    }
}
