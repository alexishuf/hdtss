package com.github.lapesd.hdtss.model.nodes;

import com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.utils.Binding;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

@Accessors(fluent = true)
public final class Values extends AbstractOp {
    private final @Getter @NonNull BatchQuerySolutions values;

    public Values(@NonNull QuerySolutions values, @NonNull Op child) {
        super(List.of(child));
        this.values = new BatchQuerySolutions(values);
    }

    public @NonNull Op inner() {
        return children.get(0);
    }

    @Override public @NonNull Type type() {
        return Type.VALUES;
    }

    @Override public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        return new Values(values, OpUtils.single(replacements));
    }

    @Override public @NonNull List<@NonNull String> outputVars() {
        if (varNames == null)
            super.outputVars().removeIf(values.varNames()::contains);
        return varNames;
    }

    @Override public @NonNull Op bind(@NonNull Binding binding) {
        var valuesVars = values.varNames();
        binding = binding.filter(v -> !valuesVars.contains(v));
        Op inner = children.get(0), bInner = inner.bind(binding);
        return bInner == inner ? this : new Values(this.values, bInner);
    }

    @Override public boolean deepEquals(@NonNull Op other) {
        if (!(other instanceof Values rhs)) return false;
        return rhs.values.deepEquals(values) && rhs.inner().deepEquals(inner());
    }
}
