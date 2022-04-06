package com.github.lapesd.hdtss.sparql.impl.union;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.IteratorQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

@Singleton
@Named("union")
@RequiresOperatorFlow(values = {"ITERATOR", "HEAVY_REACTIVE", "HDT_REACTIVE"})
public class UnionItExecutor extends UnionExecutor {
    @Inject
    public UnionItExecutor(@NonNull OpExecutorDispatcher dispatcher) {super(dispatcher);}

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        if (node.type() != Op.Type.UNION)
            throw new IllegalArgumentException("node is not a Union");
        List<@NonNull String> exposedVars = node.outputVars();
        return new IteratorQuerySolutions(node.outputVars(), new Iterator<>() {
            private final Iterator<@NonNull Op> nodeIt = node.children().iterator();
            private final TermsOrder order = new TermsOrder(exposedVars);
            private Iterator<@Nullable Term @NonNull[]> solutionIt;
            private @Nullable Term @Nullable[] next;

            @EnsuresNonNullIf(expression = "this.next", result = true)
            @Override public boolean hasNext() {
                while (next == null) {
                    if (solutionIt == null || !solutionIt.hasNext()) {
                        if (!nodeIt.hasNext())
                            return false;
                        Op nextNode = nodeIt.next();
                        QuerySolutions solutions = dispatcher.execute(nextNode);
                        order.reset(solutions.varNames());
                        solutionIt = solutions.iterator();
                    }
                    if (solutionIt.hasNext())
                        next = solutionIt.next();
                }
                return true;
            }

            @Override public @Nullable Term @NonNull[] next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                var row = this.next;
                assert row != null;
                this.next = null;
                return order.apply(row);
            }
        });
    }


}
