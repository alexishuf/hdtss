package com.github.lapesd.hdtss.sparql.impl.exists;

import com.github.lapesd.hdtss.model.nodes.Exists;
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
@Named("exists")
@RequiresOperatorFlow(values = {"REACTIVE", "HEAVY_REACTIVE"})
public class ExistsFluxExecutor extends ExistsExecutor {
    @Inject
    public ExistsFluxExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        Exists exists = (Exists) node;
        Op main = exists.main(), filter = exists.filter();
        if (IdentityNode.is(filter))
            return dispatcher.execute(main);
        boolean negate = exists.negate();
        var outerVars = main.outputVars();
        return new FluxQuerySolutions(node.outputVars(), dispatcher.execute(main).flux()
                .filter(r -> negate ^ dispatcher.execute(filter.bind(outerVars, r)).askResult()));
    }
}
