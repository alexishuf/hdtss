package com.github.lapesd.hdtss.sparql.impl.project;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.FluxQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.ExecutorUtils;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import com.github.lapesd.hdtss.utils.Binding;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static com.github.lapesd.hdtss.sparql.impl.ExecutorUtils.project;

@Singleton
@Named("project")
@RequiresOperatorFlow(values = {"REACTIVE"})
public class ProjectFluxExecutor extends ProjectExecutor {
    @Inject
    public ProjectFluxExecutor(@NonNull OpExecutorDispatcher dispatcher) { super(dispatcher); }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node, @Nullable Binding binding) {
        Op inner = node.children().get(0);
        List<@NonNull String> outVars = node.outputVars(), innerVars = inner.outputVars();
        if (binding != null) {
            outVars = binding.unbound(outVars);
            innerVars = binding.unbound(innerVars);
        }
        int[] indices = ExecutorUtils.findIndices(outVars, innerVars);
        var flux = dispatcher.execute(inner, binding).flux().map(r -> project(indices, r));
        return new FluxQuerySolutions(outVars, flux);
    }
}
