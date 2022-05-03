package com.github.lapesd.hdtss.sparql.impl.filter;

import com.github.lapesd.hdtss.model.nodes.Filter;
import com.github.lapesd.hdtss.model.nodes.Op;
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
@Named("filter")
@RequiresOperatorFlow(values = {"REACTIVE", "HEAVY_REACTIVE"})
public class JenaFilterFluxExecutor extends JenaFilterExecutor {

    @Inject
    public JenaFilterFluxExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node, @Nullable Binding binding) {
        Filter filterNode = (Filter) node;
        Evaluator evaluator = createEvaluator(filterNode, binding);
        var flux = dispatcher.execute(node.children().get(0), binding).flux().filter(evaluator);
        return new FluxQuerySolutions(evaluator.inVars(), flux);
    }
}
