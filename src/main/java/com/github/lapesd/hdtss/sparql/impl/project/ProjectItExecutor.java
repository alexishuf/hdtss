package com.github.lapesd.hdtss.sparql.impl.project;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.IteratorQuerySolutions;
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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

@Singleton
@Named("project")
@RequiresOperatorFlow(values = {"ITERATOR", "HEAVY_REACTIVE", "HDT_REACTIVE"})
public class ProjectItExecutor extends ProjectExecutor {

    @Inject
    public ProjectItExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node, @Nullable Binding binding) {
        Op inner = node.children().get(0);
        List<@NonNull String> outVars = node.outputVars(), innerVars = inner.outputVars();
        if (binding != null) {
            outVars = binding.unbound(outVars);
            innerVars = binding.unbound(innerVars);
        }
        int[] indices = ExecutorUtils.findIndices(outVars, innerVars);
        var it = dispatcher.execute(inner, binding).iterator();
        return new IteratorQuerySolutions(outVars, new Iterator<>() {
            @Override public boolean hasNext() {return it.hasNext();}
            @Override public @Nullable Term @NonNull[] next() {
                if (!it.hasNext())
                    throw new NoSuchElementException();
                return ExecutorUtils.project(indices, it.next());
            }
        });
    }

}
