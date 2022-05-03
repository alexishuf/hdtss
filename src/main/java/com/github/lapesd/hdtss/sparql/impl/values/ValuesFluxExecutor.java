package com.github.lapesd.hdtss.sparql.impl.values;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.Values;
import com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.FluxQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import com.github.lapesd.hdtss.utils.Binding;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@Singleton
@Named("values")
@RequiresOperatorFlow(values = {"REACTIVE"})
public class ValuesFluxExecutor extends ValuesExecutor {
    @Inject
    public ValuesFluxExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        Op inner = node.children().get(0);
        BatchQuerySolutions values = ((Values) node).values();
        @NonNull String @NonNull[] valuesVars = values.varNames().toArray(String[]::new);
        var flux = values.flux().map(t -> new Binding(valuesVars, t))
                .flatMap(b -> dispatcher.execute(inner.bind(b)).flux());
        return new FluxQuerySolutions(node.outputVars(), flux);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node, @Nullable Binding binding) {
        if (binding == null || binding.isEmpty())
            return execute(node);
        Op inner = node.children().get(0);
        BatchQuerySolutions values = ((Values) node).values();
        Binding augmented = augment(values.varNames(), binding);
        var flux = values.flux().map(row -> {
            Binding copy = new Binding(augmented);
            System.arraycopy(row, 0, copy.terms(), 0, row.length);
            return copy;
        }).flatMap(b -> dispatcher.execute(inner, b).flux());
        return new FluxQuerySolutions(node.outputVars(), flux);
    }
}
