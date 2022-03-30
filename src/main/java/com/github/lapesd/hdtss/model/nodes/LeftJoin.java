package com.github.lapesd.hdtss.model.nodes;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class LeftJoin extends Join {
    public LeftJoin(@NonNull List<@NonNull Op> operands) { super(operands); }
    public LeftJoin(@NonNull Op... operands) { super(operands); }

    @Override public @NonNull Type type() {
        return Type.LEFT_JOIN;
    }

    @Override public @NonNull Set<@NonNull String> inputVars() {
        if (inputVarNames == null) {
            @NonNull Op left = children.get(0);
            Set<@NonNull String> inputs = null, leftInputs = left.inputVars();
            List<@NonNull String> leftOutputs = left.varNames();
            for (String candidate : children.get(1).inputVars()) {
                if (!leftOutputs.contains(candidate))
                    (inputs == null ? inputs = new HashSet<>() : inputs).add(candidate);
            }
            if (inputs == null) inputs = leftInputs;
            else                inputs.addAll(leftInputs);
            inputVarNames = inputs;
        }
        return inputVarNames;
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
