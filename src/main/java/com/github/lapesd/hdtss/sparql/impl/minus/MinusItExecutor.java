package com.github.lapesd.hdtss.sparql.impl.minus;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Minus;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.IteratorQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;


@Singleton
@Named("minus")
@RequiresOperatorFlow(values = {"ITERATOR", "HDT_REACTIVE"})
public class MinusItExecutor extends MinusExecutor {

    @Inject public MinusItExecutor(@NonNull OpExecutorDispatcher dispatcher,
                                   @NonNull MinusStrategy strategy) {
        super(dispatcher, strategy);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        Predicate<@Nullable Term @NonNull[]> filter = strategy.createFilter((Minus) node);
        var it = dispatcher.execute(node.children().get(0)).iterator();
        return new IteratorQuerySolutions(node.varNames(), new Iterator<>() {
            private @Nullable Term @Nullable[] next = null;

            @Override public boolean hasNext() {
                while (next == null && it.hasNext()) {
                    var candidate = it.next();
                    if (filter.test(candidate))
                        next = candidate;
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
