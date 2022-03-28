package com.github.lapesd.hdtss.sparql.impl.project;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.FluxQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.ExecutorUtils;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

@Singleton
@Named("project")
@RequiresOperatorFlow(values = {"REACTIVE"})
public class ProjectFluxExecutor extends ProjectExecutor {
    @Inject
    public ProjectFluxExecutor(@NonNull OpExecutorDispatcher dispatcher) { super(dispatcher); }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        Op inner = node.children().get(0);
        int[] indices = ExecutorUtils.findIndices(node.varNames(), inner.varNames());
        return new FluxQuerySolutions(node.varNames(),
                dispatcher.execute(inner).flux().map(r -> ExecutorUtils.project(indices, r)));
    }
}
