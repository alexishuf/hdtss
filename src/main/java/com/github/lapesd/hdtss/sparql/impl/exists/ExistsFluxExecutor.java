package com.github.lapesd.hdtss.sparql.impl.exists;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.FluxQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

@Singleton
@Named("exists")
@RequiresOperatorFlow(values = {"REACTIVE", "HEAVY_REACTIVE"})
public class ExistsFluxExecutor extends ExistsExecutor {
    @Inject
    public ExistsFluxExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        Op outer = node.children().get(0), inner = node.children().get(1);
        var outerVars = outer.varNames();
        return new FluxQuerySolutions(node.varNames(), dispatcher.execute(outer).flux()
                .filter(r -> dispatcher.execute(inner.bind(outerVars, r)).askResult()));
    }
}
