package com.github.lapesd.hdtss.sparql.impl.limit;

import com.github.lapesd.hdtss.model.nodes.Limit;
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
@Named("limit")
@RequiresOperatorFlow(values = {"REACTIVE"})
public class LimitFluxExecutor extends LimitExecutor {
    @Inject
    public LimitFluxExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op op) {
        Op inner = op.children().get(0);
        long limit = ((Limit) op).limit();
        return new FluxQuerySolutions(op.outputVars(), dispatcher.execute(inner).flux().take(limit));
    }
}
