package com.github.lapesd.hdtss.model.nodes;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Set;

public class WeakDistinct extends AbstractOp {
    public WeakDistinct(@NonNull Op inner) {
        super(List.of(inner));
    }

    public @NonNull Op inner() { return children.get(0); }

    @Override public @NonNull Type type() { return Type.WEAK_DISTINCT; }

    @Override public @NonNull List<@NonNull String> outputVars() {
        return children.get(0).outputVars();
    }

    @Override public @NonNull Set<@NonNull String> inputVars() {
        return children.get(0).inputVars();
    }

    @Override public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        return new WeakDistinct(OpUtils.single(replacements));
    }
}
