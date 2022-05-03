package com.github.lapesd.hdtss.sparql.impl.exists;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.sparql.OpExecutor;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.utils.Binding;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

abstract class ExistsExecutor implements OpExecutor {
    private final @NonNull Set<Op.Type> SUPPORTED_TYPES = Set.of(Op.Type.EXISTS);
    protected final @NonNull OpExecutorDispatcher dispatcher;

    public ExistsExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override public @NonNull Set<Op.Type> supportedTypes() {
        return SUPPORTED_TYPES;
    }

    protected final @NonNull Binding createTemplate(@NonNull Op main, @NonNull Binding parent) {
        List<@NonNull String> offeredVars = main.outputVars();
        List<String> rightBindingVars = new ArrayList<>(offeredVars.size() + parent.size());
        for (String name : offeredVars) {
            if (!parent.contains(name)) rightBindingVars.add(name);
        }
        int parentStart = rightBindingVars.size();
        rightBindingVars.addAll(Arrays.asList(parent.vars()));
        Binding template = new Binding(rightBindingVars);
        for (int i = 0, size = parent.size(); i < size; i++)
            template.terms()[parentStart+i] = parent.get(i);
        return template;
    }

    protected final @NonNull Binding fillTemplate(@NonNull Binding template,
                                                  @Nullable Term @NonNull [] row) {
        System.arraycopy(row, 0, template.terms(), 0, row.length);
        return template;
    }
}
