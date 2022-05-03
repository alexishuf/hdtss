package com.github.lapesd.hdtss.sparql.impl.values;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.sparql.OpExecutor;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.utils.Binding;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

abstract class ValuesExecutor implements OpExecutor {
    protected static final @NonNull Set<Op.Type> SUPPORTED_TYPES = Set.of(Op.Type.VALUES);
    protected final @NonNull OpExecutorDispatcher dispatcher;

    public ValuesExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }


    @Override public @NonNull Set<Op.Type> supportedTypes() {
        return SUPPORTED_TYPES;
    }

    protected @NonNull Binding augment(@NonNull List<@NonNull String> valuesVars,
                                       @Nullable Binding binding) {
        if (binding == null)
            return new Binding(valuesVars);
        List<@NonNull String> allVars = new ArrayList<>(valuesVars.size() + binding.size());
        allVars.addAll(valuesVars);
        for (int i = 0, size = binding.size(); i < size; i++) {
            String name = binding.var(i);
            if (!allVars.contains(name)) allVars.add(name);
        }
        Binding augmented = new Binding(allVars);
        Term[] augmentedTerms = augmented.terms();
        for (int i = allVars.size(), size = augmented.size(); i < size; i++)
            augmentedTerms[i] = binding.get(augmented.var(i));
        return augmented;
    }
}
