package com.github.lapesd.hdtss.sparql.impl.limit;

import com.github.lapesd.hdtss.model.nodes.Limit;
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

@RequiresOperatorFlow(values = {"ITERATOR", "HEAVY_REACTIVE", "HDT_REACTIVE"})
@Singleton
@Named("limit")
public class LimitItExecutor extends LimitExecutor {
    @Inject
    public LimitItExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        long limit = ((Limit) node).limit();
        QuerySolutions sols = dispatcher.execute(node.children().get(0));
        var it = sols.iterator();
        return new IteratorQuerySolutions(sols.varNames(), new Iterator<>() {
            private int count = 0;

            @Override public boolean hasNext() {
                return it.hasNext() && count < limit;
            }

            @Override public SolutionRow next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                ++count;
                return it.next();
            }
        });
    }
}
