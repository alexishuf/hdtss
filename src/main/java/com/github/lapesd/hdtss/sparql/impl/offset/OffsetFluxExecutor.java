package com.github.lapesd.hdtss.sparql.impl.offset;

import com.github.lapesd.hdtss.model.nodes.Offset;
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
@Named("offset")
@RequiresOperatorFlow(values = {"REACTIVE"})
public class OffsetFluxExecutor extends OffsetExecutor {
    @Inject
    public OffsetFluxExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        Op inner = node.children().get(0);
        long n = ((Offset) node).offset();
        return new FluxQuerySolutions(node.outputVars(), dispatcher.execute(inner).flux().skip(n));
    }
}
