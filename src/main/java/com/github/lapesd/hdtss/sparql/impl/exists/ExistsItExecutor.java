package com.github.lapesd.hdtss.sparql.impl.exists;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Exists;
import com.github.lapesd.hdtss.model.nodes.IdentityNode;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.IteratorQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import com.github.lapesd.hdtss.utils.Binding;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
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
        Exists exists = (Exists) node;
        Op main = exists.main(), filter = exists.filter();
        if (IdentityNode.is(filter))
            return dispatcher.execute(main);
        boolean negate = exists.negate();
        var vars = main.outputVars();
        var it = dispatcher.execute(main).iterator();
        return new IteratorQuerySolutions(vars, new Iterator<>() {
            private @Nullable Term @Nullable[] next;
            private final @NonNull Binding binding = new Binding(vars.toArray(String[]::new));

            @EnsuresNonNullIf(expression = "this.next", result = true)
            @Override public boolean hasNext() {
                while (next == null && it.hasNext()) {
                    var outerRow = it.next();
                    Op bound = filter.bind(binding.setTerms(outerRow));
                    if (negate ^ dispatcher.execute(bound).askResult())
                        this.next = outerRow;
                }
                return next != null;
            }

            @Override public @Nullable Term @NonNull[] next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                var next = this.next;
                this.next = null;
                assert next != null;
                return next;
            }
        });
    }
}
