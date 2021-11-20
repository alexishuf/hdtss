package com.github.lapesd.hdtss.sparql.impl.join;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.IteratorQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

import static java.lang.System.arraycopy;
import static java.util.Collections.emptyIterator;

@Singleton
@Named("join")
@RequiresOperatorFlow(values = {"ITERATOR", "HDT_REACTIVE"})
@Requires(property = "sparql.join.strategy", value = "BIND", defaultValue = "BIND")
public class BindJoinItExecutor extends BindJoinExecutor {

    @Inject
    public BindJoinItExecutor(@NonNull OpExecutorDispatcher dispatcher,
                              @NonNull JoinReorderStrategy reorderStrategy) {
        super(dispatcher, reorderStrategy);
    }

    private class State implements Iterator<SolutionRow> {
        @Nullable State prev;
        @NonNull List<@NonNull String> accVars;
        int[] indices;
        Term @NonNull [] accTerms;
        @NonNull Op op;
        @NonNull Iterator<SolutionRow> it;
        boolean isOptional;
        @Nullable SolutionRow next;

        public State(@Nullable State prev, @NonNull Op op, boolean isOptional) {
            if (prev != null && prev.isOptional)
                throw new IllegalArgumentException("optional left operand not supported");
            this.prev = prev;
            var myVars = op.varNames();
            if (prev == null) {
                accVars = myVars;
                indices = new int[myVars.size()];
                for (int i = 0; i < indices.length; i++)
                    indices[i] = i;
            } else {
                var previousAcc = prev.accVars;
                accVars = new ArrayList<>(previousAcc);
                int myVarsSize = myVars.size(), nIndices = 0;
                indices = new int[myVarsSize];
                for (int i = 0, iAfterBound = 0; i < myVarsSize; i++) {
                    String v = myVars.get(i);
                    if (!accVars.contains(v)) {
                        accVars.add(v);
                        indices[nIndices++] = iAfterBound++;
                    }
                }
                indices = Arrays.copyOf(indices, nIndices);
                accVars = accVars.size() == previousAcc.size() ? previousAcc : accVars;
            }
            this.accTerms = new Term[accVars.size()];
            this.op = op;
            this.isOptional = isOptional;
            this.it = prev == null ? dispatcher.execute(op).iterator() : emptyIterator();
        }

        private void merge(@NonNull SolutionRow myRow) {
            Term[] myTerms = myRow.terms();
            if (prev == null) {
                next = myRow;
                arraycopy(myTerms, 0, accTerms, 0, accTerms.length);
            } else {
                assert !prev.isOptional;
                assert prev.accTerms.length+indices.length == accTerms.length;
                Term[] prevTerms = prev.accTerms;
                int prevLen = prevTerms.length;
                arraycopy(prevTerms, 0, accTerms, 0, prevLen);
                if (myTerms.length == 0) {
                    Arrays.fill(accTerms, prevLen, accTerms.length, null);
                } else {
                    assert prevLen+myTerms.length == accTerms.length;
                    for (int i = prevLen; i < accTerms.length; i++)
                        accTerms[i] = myTerms[indices[i - prevLen]];
                }
                next = new SolutionRow(Arrays.copyOf(accTerms, accTerms.length));
            }
        }

        @Override public boolean hasNext() {
            while (next == null) {
                if (it.hasNext()) {
                    merge(it.next());
                } else if (prev != null && prev.hasNext()) {
                    SolutionRow leftResult = prev.next();
                    Op bound = op.bind(prev.accVars, leftResult.terms());
                    it = dispatcher.execute(bound).iterator();
                    if (!it.hasNext() && isOptional) {
                        it = Collections.singletonList(SolutionRow.EMPTY).iterator();
                    }
                } else {
                    return false;
                }
            }
            return true;
        }

        @Override public @NonNull SolutionRow next() {
            if (!hasNext())
                throw new NoSuchElementException();
            SolutionRow next = this.next;
            assert next != null;
            this.next = null;
            return next;
        }
    }

    @Override
    protected @NonNull QuerySolutions execute(boolean isLeft, @NonNull List<@NonNull Op> operands,
                                              @NonNull List<String> varNames,
                                              int @Nullable[] projection) {
        final int nOperands = operands.size();
        List<State> states = new ArrayList<>(nOperands);
        return new IteratorQuerySolutions(varNames, new Iterator<>() {
            private State last;

            private void init() {
                if (!states.isEmpty())
                    return;
                assert last == null;
                for (Op op : operands)
                    states.add(last = new State(last, op, last != null && isLeft));
            }

            @Override public boolean hasNext()  {
                init();
                return last.hasNext();
            }

            @Override public @NonNull SolutionRow next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                SolutionRow next = last.next();
                if (projection != null) {
                    Term[] unordered = next.terms();
                    Term[] ordered = new Term[unordered.length];
                    for (int i = 0; i < ordered.length; i++) ordered[i] = unordered[projection[i]];
                    next = new SolutionRow(ordered);
                }
                return next;
            }
        });
    }
}
