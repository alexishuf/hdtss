package com.github.lapesd.hdtss.sparql.impl.assign;

import com.github.lapesd.hdtss.model.nodes.Assign;
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
@Named("assign")
@RequiresOperatorFlow(values = {"REACTIVE", "HEAVY_REACTIVE"})
public class JenaAssignFluxExecutor extends JenaAssignExecutor {
    @Inject
    public JenaAssignFluxExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        Evaluator evaluator = new Evaluator((Assign) node);
        return new FluxQuerySolutions(node.outputVars(),
                dispatcher.execute(node.children().get(0)).flux().map(evaluator));
    }
}
