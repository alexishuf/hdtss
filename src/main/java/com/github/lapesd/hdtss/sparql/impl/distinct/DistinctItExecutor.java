package com.github.lapesd.hdtss.sparql.impl.distinct;

import com.github.lapesd.hdtss.model.Row;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.IteratorQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.DistinctStrategy;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
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
        return new IteratorQuerySolutions(node.outputVars(), new Iterator<>() {
            private @Nullable Term @Nullable[] next;

            @EnsuresNonNullIf(expression = "this.next", result = true)
            @Override public boolean hasNext() {
                while (next == null && inner.hasNext()) {
                    var candidate = inner.next();
                    if (set.add(new Row(candidate)))
                        next = candidate;
                }
                return next != null;
            }

            @Override public @Nullable Term @NonNull[] next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                var row = this.next;
                this.next = null;
                assert row != null;
                return row;
            }
        });
    }
}
