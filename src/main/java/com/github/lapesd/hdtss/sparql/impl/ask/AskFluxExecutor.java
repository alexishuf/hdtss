package com.github.lapesd.hdtss.sparql.impl.ask;

import com.github.lapesd.hdtss.model.Row;
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
@Named("ask")
@RequiresOperatorFlow(values = {"REACTIVE"})
public class AskFluxExecutor extends AskExecutor {
    @Inject public AskFluxExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        var flux = dispatcher.execute(node.children().get(0)).flux()
                             .take(1).map(r -> Row.EMPTY);
        return new FluxQuerySolutions(node.outputVars(), flux);
    }

}
