package com.github.lapesd.hdtss.sparql.impl.exists;

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
@Named("exists")
@RequiresOperatorFlow(values = {"ITERATOR", "HDT_REACTIVE"})
public class ExistsItExecutor extends ExistsExecutor {
    @Inject
    public ExistsItExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        Op outer = node.children().get(0), inner = node.children().get(1);
        var outerVars = outer.varNames();
        var it = dispatcher.execute(outer).iterator();
        return new IteratorQuerySolutions(node.varNames(), new Iterator<>() {
            private @Nullable SolutionRow next;

            @Override public boolean hasNext() {
                while (next == null && it.hasNext()) {
                    SolutionRow outerRow = it.next();
                    if (dispatcher.execute(inner.bind(outerVars, outerRow.terms())).askResult())
                        this.next = outerRow;
                }
                return next != null;
            }

            @Override public @NonNull SolutionRow next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                assert this.next != null;
                SolutionRow next = this.next;
                this.next = null;
                return next;
            }
        });
    }
}
