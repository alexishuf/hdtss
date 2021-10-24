package com.github.lapesd.hdtss.sparql.impl.project;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.IteratorQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Iterator;
import java.util.NoSuchElementException;

@Singleton
@Named("project")
@RequiresOperatorFlow(values = {"ITERATOR", "HEAVY_REACTIVE", "HDT_REACTIVE"})
public class ProjectItExecutor extends ProjectExecutor {

    @Inject
    public ProjectItExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        var inner = dispatcher.execute(node.children().get(0)).iterator();
        int[] indices = findIndices(node);
        return new IteratorQuerySolutions(node.varNames(), new Iterator<>() {
            @Override public boolean hasNext() {return inner.hasNext();}
            @Override public @NonNull SolutionRow next() {
                if (!inner.hasNext())
                    throw new NoSuchElementException();
                return project(inner.next(), indices);
            }
        });
    }

}
