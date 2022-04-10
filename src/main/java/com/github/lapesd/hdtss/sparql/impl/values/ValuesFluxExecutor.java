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
}
