package com.github.lapesd.hdtss.sparql.impl.join;

import com.github.lapesd.hdtss.model.Row;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.IteratorQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import com.github.lapesd.hdtss.utils.Binding;
import com.github.lapesd.hdtss.utils.BitsetOps;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static com.github.lapesd.hdtss.utils.BitsetOps.*;
import static java.lang.System.arraycopy;
import static java.util.Arrays.copyOf;
import static java.util.Collections.emptyIterator;

@Singleton
@Named("join")
@RequiresOperatorFlow(values = {"ITERATOR", "HDT_REACTIVE"})
@Requires(property = "sparql.join.strategy", value = "BIND", defaultValue = "BIND")
public class BindJoinItExecutor extends BindJoinExecutor {

    @Inject
    public BindJoinItExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        super(dispatcher);
    }

    private class State implements Iterator<@Nullable Term []> {
        @Nullable State prev;
        @NonNull Binding binding;
        final int @NonNull[] indices;
        @NonNull Op op;
        @NonNull Iterator<@Nullable Term []> it;
        boolean isOptional, hasValue, open = true;

        public State(@Nullable State prev, @NonNull Op op, boolean isOptional) {
            if (prev != null && prev.isOptional)
                throw new IllegalArgumentException("optional left operand not supported");
            this.prev = prev;
            var myVars = op.outputVars();
            int myVarsSize = myVars.size();
            this.indices = new int[myVarsSize];
            if (prev == null) {
                binding = new Binding(myVars.toArray(String[]::new));
                for (int i = 0; i < indices.length; i++) indices[i] = i;
            } else {
                Binding prevBinding = prev.binding;
                long[] novel = createBitset(myVarsSize);
                for (int i = 0; i < myVarsSize; i++) {
                    if (!prevBinding.contains(myVars.get(i)))
                        BitsetOps.set(novel, i);
                }
                int nNovel = cardinality(novel), nVars = prevBinding.size(), nIndices = 0;
                if (nNovel == 0) {
                    this.binding = prevBinding;
                } else {
                    String[] vars = copyOf(prevBinding.vars(), nVars + nNovel);
                    for (int i = nextSet(novel, 0); i >= 0; i = nextSet(novel, i + 1)) {
                        vars[nVars++] = myVars.get(i);
                        indices[nIndices++] = i;
                    }
                    this.binding = new Binding(vars);
                }
            }
            this.op = op;
            this.isOptional = isOptional;
            this.it = prev == null ? dispatcher.execute(op).iterator() : emptyIterator();
        }

        private void merge(@Nullable Term @NonNull[] myRow) {
            if (prev == null) {
                binding.setTerms(myRow);
            } else if (prev.binding != binding) {
                assert !prev.isOptional;
                Term[] prevTerms = prev.binding.terms(), accTerms = binding.terms();
                arraycopy(prevTerms, 0, accTerms, 0, prevTerms.length);
                if (myRow.length > 0) {
                    assert prev.binding.size()+myRow.length == binding.size();
                    arraycopy(myRow, 0, accTerms, prevTerms.length, myRow.length);
                } else {
                    Arrays.fill(accTerms, prevTerms.length, accTerms.length, null);
                }
            }
        }

        @EnsuresNonNullIf(expression = "this.next", result = true)
        @Override public boolean hasNext() {
            while (open && !hasValue) {
                if (it.hasNext()) {
                    merge(it.next());
                    hasValue = true;
                } else if (prev != null && prev.hasNext()) {
                    Op bound = op.bind(prev.binding.setTerms(prev.next()));
                    it = dispatcher.execute(bound).iterator();
                    if (!it.hasNext() && isOptional) {
                        it = Row.SINGLE_EMPTY.iterator();
                    }
                } else {
                    open = false;
                }
            }
            return open;
        }

        @Override public @Nullable Term @NonNull[] next() {
            if (!hasNext())
                throw new NoSuchElementException();
            @Nullable Term[] terms = binding.terms();
            hasValue = false;
            return terms;
        }
    }

    @Override
    protected @NonNull QuerySolutions execute(boolean isLeft, @NonNull List<@NonNull Op> operands,
                                              @NonNull List<String> varNames) {
        return new IteratorQuerySolutions(varNames, new Iterator<>() {
            private State last;

            private void init() {
                if (last == null) {
                    for (Op op : operands)
                        last = new State(last, op, last != null && isLeft);
                }
            }

            @Override public boolean hasNext()  {
                init();
                return last.hasNext();
            }

            @Override public @Nullable Term @NonNull[] next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                @Nullable Term[] raw = last.next();
                return copyOf(raw, raw.length);
            }
        });
    }
}
