package com.github.lapesd.hdtss.sparql.impl.values;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.Values;
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

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

@Singleton
@Named("values")
@RequiresOperatorFlow(values = {"ITERATOR", "HEAVY_REACTIVE", "HDT_REACTIVE"})
public class ValuesItExecutor extends ValuesExecutor {
    @Inject
    public ValuesItExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node,
                                                     @Nullable Binding outerBinding) {
        Values valuesNode = (Values) node;
        var values = valuesNode.values();
        var inner = valuesNode.inner();
        return new IteratorQuerySolutions(node.outputVars(), new Iterator<>() {
            private final @NonNull Iterator<@Nullable Term @NonNull[]> valuesIt = values.iterator();
            private @NonNull Iterator<@Nullable Term @NonNull[]> it = Collections.emptyIterator();
            private final @NonNull Binding binding = augment(values.varNames(), outerBinding);

            @Override public boolean hasNext() {
                while (!it.hasNext() && valuesIt.hasNext()) {
                    var row = valuesIt.next();
                    if (outerBinding == null) {
                        binding.setTerms(row);
                    } else {
                        System.arraycopy(row, 0, binding.terms(), 0, row.length);
                    }
                    it = dispatcher.execute(inner, binding).iterator();
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
