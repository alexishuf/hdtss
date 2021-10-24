package com.github.lapesd.hdtss.model.nodes;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

public final class LeftJoin extends Join {
    public LeftJoin(@NonNull List<@NonNull Op> operands) { super(operands); }
    public LeftJoin(@NonNull Op... operands) { super(operands); }

    @Override public @NonNull Type type() {
        return Type.LEFT_JOIN;
    }

    @Override
    public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        return switch (replacements.size()) {
            case 0 -> IdentityNode.INSTANCE;
            case 1 -> replacements.get(0);
            default -> new LeftJoin(replacements);
        };
    }

    @Override public boolean isBGP() {
        return false;
    }
}
