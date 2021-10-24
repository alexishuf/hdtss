package com.github.lapesd.hdtss.sparql.impl.minus;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Minus;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

@Requires(property = "sparql.minus.strategy", value = "BIND", defaultValue = "BIND")
public class BindMinusStrategy implements MinusStrategy {
    protected final @NonNull OpExecutorDispatcher dispatcher;

    @Inject
    public BindMinusStrategy(@NonNull OpExecutorDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    private static record State(@NonNull OpExecutorDispatcher dispatcher,
                                @NonNull Op inner,
                                @NonNull List<String> sharedVars,
                                int @NonNull [] outerIndices) implements Predicate<SolutionRow> {

        @Override public boolean test(SolutionRow row) {
            Term[] terms = row.terms(), projected = new Term[outerIndices.length];
            for (int i = 0; i < outerIndices.length; i++) projected[i] = terms[outerIndices[i]];
            Op bound = inner.bind(sharedVars, projected, Op.BindType.ONLY_TRIPLES);
            return !dispatcher.execute(bound).askResult();
        }
    }

    @Override public @NonNull Predicate<SolutionRow> createFilter(@NonNull Minus minus) {
        var outerVars = minus.outer().varNames();
        List<String> sharedVars = new ArrayList<>(outerVars.size());
        int[] outerIndices = new int[outerVars.size()];
        int nShared = 0;
        for (String innerVar : minus.inner().varNames()) {
            int i = outerVars.indexOf(innerVar);
            if (i >= 0) {
                sharedVars.add(innerVar);
                outerIndices[nShared++] = i;
            }
        }
        outerIndices = Arrays.copyOf(outerIndices, nShared);
        return new State(dispatcher, minus.inner(), sharedVars, outerIndices);
    }
}
