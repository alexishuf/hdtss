package com.github.lapesd.hdtss.model.nodes;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

public final class Distinct extends AbstractOp {

    public Distinct(@NonNull Op inner) {
        super(List.of(inner));
    }

    @Override public @NonNull Type type() { return Type.DISTINCT; }

    @Override public @NonNull List<@NonNull String> outputVars() {
        return children.get(0).outputVars();
    }

    @Override
    public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        return new Distinct(OpUtils.single(replacements));
    }
}
