package com.github.lapesd.hdtss.sparql.impl.exists;

import com.github.lapesd.hdtss.model.nodes.IdentityNode;
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
@Named("notExists")
@RequiresOperatorFlow(operator = "exists", values = {"REACTIVE", "HEAVY_REACTIVE"})
public class NotExistsFluxExecutor extends NotExistsExecutor {
    @Inject
    public NotExistsFluxExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        Op outer = node.children().get(0), inner = node.children().get(1);
        var outerVars = outer.varNames();
        QuerySolutions outerSols = dispatcher.execute(outer);
        if (IdentityNode.is(inner))
            return outerSols;
        return new FluxQuerySolutions(node.varNames(), outerSols.flux()
                .filter(r -> !dispatcher.execute(inner.bind(outerVars, r.terms())).askResult()));
    }
}
