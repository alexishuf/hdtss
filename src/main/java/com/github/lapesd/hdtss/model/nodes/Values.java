package com.github.lapesd.hdtss.model.nodes;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.impl.ExecutorUtils;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Accessors(fluent = true)
public class Values extends AbstractOp {
    private final @Getter @NonNull BatchQuerySolutions values;

    public Values(@NonNull QuerySolutions values, @NonNull Op child) {
        super(List.of(child));
        this.values = new BatchQuerySolutions(values);
    }

    public @NonNull Op inner() {
        return children.get(0);
    }

    @Override public @NonNull Type type() {
        return Type.VALUES;
    }

    @Override public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        return new Values(values, OpUtils.single(replacements));
    }

    @Override public @NonNull List<@NonNull String> outputVars() {
        if (varNames == null)
            super.outputVars().removeIf(values.varNames()::contains);
        return varNames;
    }

    @Override public @NonNull Op bind(@NonNull List<String> varNames, Term @NonNull [] row) {
        var inner = children.get(0);
        var forbidden = values.varNames();
        var useful = inner.outputVars();
        int varNamesSize = varNames.size();
        int[] indices = new int[varNamesSize];
        int newLen = 0;
        for (int i = 0; i < varNamesSize; i++) {
            String name = varNames.get(i);
            if (!forbidden.contains(name) && useful.contains(name))
                indices[newLen++] = i;
        }
        if (newLen == 0) {
            return this;
        } else if (newLen == varNamesSize) {
            return new Values(values, inner.bind(varNames, row));
        } else {
            Term[] newRow = ExecutorUtils.project(indices, row);
            List<@NonNull String> newVars = new ArrayList<>(newLen);
            for (int i = 0; i < newLen; i++)
                newVars.add(varNames.get(indices[i]));
            return new Values(values, inner.bind(newVars, newRow));
        }
    }

    @Override public @NonNull Op bind(@NonNull Map<String, Term> var2term) {
        Op inner = children.get(0);
        if (inner.outputVars().stream().noneMatch(var2term::containsKey)) {
            return this; // no work
        } else if (values.varNames().stream().anyMatch(var2term::containsKey)) {
            var filtered = new HashMap<>(var2term);
            values.varNames().forEach(filtered::remove);
            return new Values(values, inner.bind(filtered));
        } else {
            return new Values(values, inner.bind(var2term));
        }
    }

    @Override public boolean deepEquals(@NonNull Op other) {
        if (!(other instanceof Values rhs)) return false;
        return rhs.values.deepEquals(values) && rhs.inner().deepEquals(inner());
    }
}
