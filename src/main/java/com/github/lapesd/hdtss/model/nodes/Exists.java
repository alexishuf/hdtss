package com.github.lapesd.hdtss.model.nodes;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Exists extends AbstractOp {
    private final boolean negate;

    protected Exists(@NonNull Op outer, @NonNull Op inner, boolean negate) {
        super(List.of(outer, inner));
        this.negate = negate;
    }

    /** Create a {@code main FILTER EXISTS filter} expression */
    public static @NonNull Exists create(@NonNull Op main, @NonNull Op filter) {
        return new Exists(main, filter, false);
    }

    /** Create a {@code main FILTER NOT EXISTS filter} expression */
    public static @NonNull Exists not(@NonNull Op main, @NonNull Op filter) {
        return new Exists(main, filter, true);
    }

    /** Whether this is a {@code NOT EXISTS} instead of {@code EXISTS} */
    public boolean negate() { return negate; }

    public @NonNull Op main() {
        return children.get(0);
    }

    public @NonNull Op filter() {
        return children.get(1);
    }

    @Override public @NonNull Type type() {
        return Type.EXISTS;
    }

    @Override public @NonNull List<@NonNull String> outputVars() {
        return main().outputVars();
    }

    @Override public @NonNull Set<@NonNull String> inputVars() {
        if (inputVarNames == null) {
            Op filter = children.get(1);
            Set<String> inputs = new HashSet<>(filter.outputVars());
            inputs.addAll(filter.inputVars());
            for (String offer : children.get(0).outputVars()) inputs.remove(offer);
            inputVarNames = inputs;
        }
        return inputVarNames;
    }

    @Override public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        int size = replacements.size();
        if (size != 2)
            throw new IllegalArgumentException("FILTER EXISTS requires exactly two child nodes");
        return new Exists(replacements.get(0), replacements.get(1), negate);
    }

    @Override public boolean deepEquals(@NonNull Op other) {
        if (!(other instanceof Exists e)) return false;
        return negate == e.negate && e.main().deepEquals(main()) && e.filter().deepEquals(filter());
    }

    @Override public @NonNull String toString() {
        return "Exists("+ main()+", "+ filter()+")";
    }
}
