package com.github.lapesd.hdtss.model.nodes;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Exists extends AbstractOp {
    public Exists(@NonNull Op outer, @NonNull Op inner) {
        super(List.of(outer, inner));
    }

    public @NonNull Op main() {
        return children.get(0);
    }

    public @NonNull Op filter() {
        return children.get(1);
    }

    @Override public @NonNull Type type() {
        return Type.EXISTS;
    }

    @Override public @NonNull List<@NonNull String> varNames() {
        return main().varNames();
    }

    @Override public @NonNull Set<@NonNull String> inputVars() {
        if (inputVarNames == null) {
            Op filter = children.get(1);
            Set<String> inputs = new HashSet<>(filter.varNames());
            inputs.addAll(filter.inputVars());
            for (String offer : children.get(0).varNames()) inputs.remove(offer);
            inputVarNames = inputs;
        }
        return inputVarNames;
    }

    @Override public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        int size = replacements.size();
        if (size != 2)
            throw new IllegalArgumentException("FILTER EXISTS requires exactly two child nodes");
        return new Exists(replacements.get(0), replacements.get(1));
    }

    @Override public boolean deepEquals(@NonNull Op other) {
        if (!(other instanceof Exists e)) return false;
        return e.main().deepEquals(main()) && e.filter().deepEquals(filter());
    }

    @Override public @NonNull String toString() {
        return "Exists("+ main()+", "+ filter()+")";
    }
}
