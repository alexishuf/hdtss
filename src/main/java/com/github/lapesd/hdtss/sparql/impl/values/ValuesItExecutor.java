package com.github.lapesd.hdtss.sparql.impl.values;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.Values;
import com.github.lapesd.hdtss.model.solutions.IteratorQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

@Singleton
@Named("values")
@RequiresOperatorFlow(values = {"ITERATOR", "HEAVY_REACTIVE", "HDT_REACTIVE"})
public class ValuesItExecutor extends ValuesExecutor {
    @Inject
    public ValuesItExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        Values valuesNode = (Values) node;
        var values = valuesNode.values();
        var inner = valuesNode.inner();
        return new IteratorQuerySolutions(node.varNames(), new Iterator<>() {
            private final @NonNull Iterator<@NonNull SolutionRow> valuesIt = values.iterator();
            private @NonNull Iterator<SolutionRow> it = Collections.emptyIterator();

            @Override public boolean hasNext() {
                while (!it.hasNext() && valuesIt.hasNext()) {
                    Op bound = inner.bind(values.varNames(), valuesIt.next().terms());
                    it = dispatcher.execute(bound).iterator();
                }
                return it.hasNext();
            }

            @Override public @NonNull SolutionRow next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                return it.next();
            }
        });
    }
}
