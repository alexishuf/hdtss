package com.github.lapesd.hdtss.model.nodes;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

public class Ask extends AbstractOp {
    public Ask(@NonNull Op child) {
        super(List.of(child));
    }

    public @NonNull Op inner() {
        return children.get(0);
    }

    @Override public @NonNull Type type() {
        return Type.ASK;
    }

    @Override public @NonNull List<@NonNull String> varNames() {
        return List.of();
    }

    @Override public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        return new Ask(OpUtils.single(replacements));
    }
}
