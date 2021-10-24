package com.github.lapesd.hdtss.sparql.impl.filter;

import com.github.lapesd.hdtss.model.nodes.Filter;
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
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.NoSuchElementException;

@Singleton
@Named("filter")
@RequiresOperatorFlow(values = {"ITERATOR", "HDT_REACTIVE"})
public class JenaFilterItExecutor extends JenaFilterExecutor {
    @Inject
    public JenaFilterItExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        Filter filter = (Filter) node;
        Evaluator evaluator = new Evaluator(filter);
        var inner = dispatcher.execute(filter.inner()).iterator();
        return new IteratorQuerySolutions(node.varNames(), new Iterator<>() {
            private @Nullable SolutionRow next = null;

            @Override public boolean hasNext() {
                while (next == null && inner.hasNext()) {
                    SolutionRow candidate = inner.next();
                    if (evaluator.test(candidate))
                        next = candidate;
                }
                return next != null;
            }

            @Override public SolutionRow next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                SolutionRow row = this.next;
                this.next = null;
                return row;
            }
        });
    }
}
