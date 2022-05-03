package com.github.lapesd.hdtss.sparql.impl.slice;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.Slice;
import com.github.lapesd.hdtss.model.solutions.IteratorQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import com.github.lapesd.hdtss.utils.Binding;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.NoSuchElementException;

@Singleton
@Named("slice")
@RequiresOperatorFlow(values = {"ITERATOR", "HEAVY_REACTIVE", "HDT_REACTIVE"})
public class SliceItExecutor extends SliceExecutor {

    @Inject
    public SliceItExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node, @Nullable Binding binding) {
        Slice slice = (Slice) node;
        long offset = slice.offset(), end = offset + slice.limit();
        var it = dispatcher.execute(slice.inner(), binding).iterator();
        var outVars = binding == null ? slice.outputVars() : binding.unbound(slice.outputVars());
        return new IteratorQuerySolutions(outVars, new Iterator<>() {
            private long cursor = 0;

            @Override public boolean hasNext() {
                while (cursor < offset && it.hasNext()) {
                    it.next();
                    ++cursor;
                }
                return cursor != end && it.hasNext();
            }

            @Override public @Nullable Term @NonNull[] next() {
                if (!hasNext()) throw new NoSuchElementException();
                ++cursor;
                return it.next();
            }
        });
    }
}
