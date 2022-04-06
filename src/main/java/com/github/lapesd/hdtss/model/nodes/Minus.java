package com.github.lapesd.hdtss.model.nodes;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.utils.BitsetOps;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.*;

import static com.github.lapesd.hdtss.utils.BitsetOps.nextSet;
import static java.util.Arrays.asList;

public class Minus extends AbstractOp {
    public Minus(@NonNull Op main, @NonNull Op filter) {
        super(asList(main, filter));
    }

    public @NonNull Op main() {
        return children.get(0);
    }

    public @NonNull Op filter() {
        return children.get(1);
    }

    @Override public @NonNull Type type() {
        return Type.MINUS;
    }

    @Override public @NonNull List<@NonNull String> outputVars() {
        return main().outputVars();
    }

    @Override public @NonNull Set<@NonNull String> inputVars() {
        return children.get(0).inputVars();
    }

    @Override public @NonNull Op bind(@NonNull List<String> varNames, Term @NonNull [] row) {
        Op main = children.get(0), filter = children.get(1);
        Op bMain = main.bind(varNames, row);

        Set<@NonNull String> inputVars = filter.inputVars();
        long[] subset = BitsetOps.createBitset(row.length);
        for (int i = 0, size = varNames.size(); i < size; i++) {
            if (!inputVars.contains(varNames.get(i)))
                BitsetOps.set(subset, i);
        }
        int subsetSize = BitsetOps.cardinality(subset);
        if (subsetSize != row.length) {
            List<String> varNamesSubset = new ArrayList<>();
            Term[] rowSubset = new Term[subsetSize];
            for (int i = nextSet(subset, 0); i >= 0; i = nextSet(subset, i+1) ) {
                rowSubset[varNames.size()] = row[i];
                varNamesSubset.add(varNames.get(i));
            }
            varNames = varNamesSubset;
            row = rowSubset;
        }
        Op bFilter = filter.bind(varNames, row);

        return bFilter != filter || bMain != main ? new Minus(bMain, bFilter) : this;
    }

    @Override public @NonNull Op bind(@NonNull Map<String, Term> var2term) {
        Op main = children.get(0);
        Op bMain = main.bind(var2term), filter = children.get(1);
        boolean needsProjection = false;
        Set<@NonNull String> filterInputs = filter.inputVars();
        for (String var : filterInputs) {
            if (var2term.containsKey(var)) {
                needsProjection = true;
                break;
            }
        }
        if (needsProjection) {
            Map<String, Term> subset = new HashMap<>((int)Math.max(4, var2term.size()/0.75+1));
            for (Map.Entry<String, Term> e : var2term.entrySet()) {
                if (!filterInputs.contains(e.getKey()))
                    subset.put(e.getKey(), e.getValue());
            }
            var2term = subset;
        }
        Op bFilter = filter.bind(var2term);
        return bMain != main || bFilter != filter ? new Minus(bMain, bFilter) : this;
    }

    @Override public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        if (replacements.size() != 2)
            throw new IllegalArgumentException("MINUS must have an outer node and an inner node");
        return new Minus(replacements.get(0), replacements.get(1));
    }

    @Override public boolean deepEquals(@NonNull Op other) {
        return other instanceof Minus m
                && main().deepEquals(m.main())
                && filter().deepEquals(m.filter());
    }

    @Override public @NonNull String toString() {
        return "Minus("+ main()+", "+ filter()+")";
    }
}
