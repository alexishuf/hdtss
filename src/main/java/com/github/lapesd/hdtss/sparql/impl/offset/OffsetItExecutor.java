package com.github.lapesd.hdtss.sparql.impl.offset;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Offset;
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

@Singleton
@Named("offset")
@RequiresOperatorFlow(values = {"ITERATOR", "HEAVY_REACTIVE", "HDT_REACTIVE"})
public class OffsetItExecutor extends OffsetExecutor {

    @Inject
    public OffsetItExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        long offset = ((Offset) node).offset();
        var it = dispatcher.execute(node.children().get(0)).iterator();
        return new IteratorQuerySolutions(node.varNames(), new Iterator<>() {
            private int count = 0;

            @Override public boolean hasNext() {
                while (count < offset && it.hasNext()) {
                    it.next();
                    ++count;
                }
                return it.hasNext();
            }

            @Override public @Nullable Term @NonNull[] next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                return it.next();
            }
        });
    }
}
