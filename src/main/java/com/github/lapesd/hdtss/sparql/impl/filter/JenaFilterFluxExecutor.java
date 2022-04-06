package com.github.lapesd.hdtss.sparql.impl.filter;

import com.github.lapesd.hdtss.model.nodes.Filter;
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
@Named("filter")
@RequiresOperatorFlow(values = {"REACTIVE", "HEAVY_REACTIVE"})
public class JenaFilterFluxExecutor extends JenaFilterExecutor {

    @Inject
    public JenaFilterFluxExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        Filter filter = (Filter) node;
        return new FluxQuerySolutions(node.outputVars(),
                dispatcher.execute(filter.inner()).flux().filter(new Evaluator(filter)));
    }
}
