package com.github.lapesd.hdtss.sparql.impl.distinct;

import com.github.lapesd.hdtss.model.Row;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.FluxQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.DistinctStrategy;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import com.github.lapesd.hdtss.utils.Binding;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@Singleton
@Named("distinct")
@RequiresOperatorFlow(values = {"REACTIVE"})
public class DistinctFluxExecutor extends DistinctExecutor {
    @Inject
    public DistinctFluxExecutor(@NonNull OpExecutorDispatcher dispatcher,
                                @NonNull DistinctStrategy distinctStrategy) {
        super(dispatcher, distinctStrategy);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node, @Nullable Binding binding) {
        var set = distinctStrategy.createSet();
        var flux = dispatcher.execute(node.children().get(0), binding)
                             .flux().filter(r -> set.add(new Row(r)));
        var outVars = binding == null ? node.outputVars() : binding.unbound(node.outputVars());
        return new FluxQuerySolutions(outVars, flux);
    }
}
