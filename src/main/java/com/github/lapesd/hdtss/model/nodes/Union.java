package com.github.lapesd.hdtss.model.nodes;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.List;

public final class Union extends AbstractOp {
    public Union(@NonNull List<Op> operands) {
        super(operands);
    }

    public Union(@NonNull Op... operands) {
        this(Arrays.asList(operands));
    }

    public static @NonNull Op of(@NonNull Op... operands) {
        return of(Arrays.asList(operands));
    }

    public static @NonNull Op of(@NonNull List<Op> operands) {
        var flat = OpUtils.flattenOrCopy(Union.class, operands);
        return OpUtils.maybeSingle(flat).orElseGet(() -> new Union(flat));
    }

    @Override public @NonNull Type type() {
        return Type.UNION;
    }

    @Override
    public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        replacements = OpUtils.flattenOrCopy(Union.class, replacements);
        return switch (replacements.size()) {
            case 0 -> IdentityNode.INSTANCE;
            case 1 -> replacements.get(0);
            default -> new Union(replacements);
        };
    }
}
