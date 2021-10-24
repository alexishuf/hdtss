package com.github.lapesd.hdtss.sparql.impl.distinct;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.FluxQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.DistinctStrategy;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

@Singleton
@Named("distinct")
@RequiresOperatorFlow(values = {"REACTIVE"})
public class DistinctFluxExecutor extends DistinctExecutor {
    @Inject
    public DistinctFluxExecutor(@NonNull OpExecutorDispatcher dispatcher,
                                @NonNull DistinctStrategy distinctStrategy) {
        super(dispatcher, distinctStrategy);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        var set = distinctStrategy.createSet();
        return new FluxQuerySolutions(node.varNames(),
                dispatcher.execute(node.children().get(0)).flux().filter(set::add));
    }
}
