package com.github.lapesd.hdtss.model.nodes;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Join extends AbstractOp {
    public Join(@NonNull List<@NonNull Op> operands) {
        super(operands);
    }

    public Join(@NonNull Op... operands) {
        this(Arrays.asList(operands));
    }

    public static @NonNull Op of(@NonNull Op... operands) {
        return of(Arrays.asList(operands));
    }

    public static @NonNull Op of(@NonNull List<@NonNull Op> operands) {
        var flat = OpUtils.flattenOrCopy(Join.class, operands);
        return OpUtils.maybeSingle(flat).orElseGet(() -> new Join(flat));
    }

    @Override public @NonNull Type type() {
        return Type.JOIN;
    }

    @Override public @NonNull Set<@NonNull String> inputVars() {
        Set<@NonNull String> result = null;
        outer:
        for (int i = 0, size = children.size(); i < size; i++) {
            for (String candidate : children.get(i).inputVars()) {
                for (int j = 0; j < size; j++) {
                    if (j != i && children.get(j).varNames().contains(candidate))
                        continue outer;
                }
                (result == null ? result = new HashSet<>() : result).add(candidate);
            }
        }
        return result == null ? Set.of() : result;
    }

    @Override
    public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        replacements = OpUtils.flattenOrCopy(Join.class, replacements);
        return switch (replacements.size()) {
            case 0 -> IdentityNode.INSTANCE;
            case 1 -> replacements.get(0);
            default -> new Join(replacements);
        };
    }

    public boolean isBGP() {
        for (Op o : children) {
            if (o instanceof TriplePattern) continue;
            if (o instanceof Filter f && f.inner() instanceof TriplePattern) continue;
            return false;
        }
        return true;
    }
}
