package com.github.lapesd.hdtss.sparql.impl.distinct;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.IteratorQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import com.github.lapesd.hdtss.sparql.DistinctStrategy;
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
@Named("distinct")
@RequiresOperatorFlow(values = {"ITERATOR", "HEAVY_REACTIVE", "HDT_REACTIVE"})
public class DistinctItExecutor extends DistinctExecutor {

    @Inject
    public DistinctItExecutor(@NonNull OpExecutorDispatcher dispatcher,
                              @NonNull DistinctStrategy distinctStrategy) {
        super(dispatcher, distinctStrategy);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        var inner = dispatcher.execute(node.children().get(0)).iterator();
        var set = distinctStrategy.createSet();
        return new IteratorQuerySolutions(node.varNames(), new Iterator<>() {
            private @Nullable SolutionRow next;

            @Override public boolean hasNext() {
                while (next == null && inner.hasNext()) {
                    SolutionRow candidate = inner.next();
                    if (set.add(candidate))
                        next = candidate;
                }
                return next != null;
            }

            @Override public @NonNull SolutionRow next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                SolutionRow row = this.next;
                assert row != null;
                this.next = null;
                return row;
            }
        });
    }
}
