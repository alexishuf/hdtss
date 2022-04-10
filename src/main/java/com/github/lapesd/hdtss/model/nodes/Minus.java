package com.github.lapesd.hdtss.model.nodes;

import com.github.lapesd.hdtss.utils.Binding;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;

public final class Minus extends AbstractOp {
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

    @Override public @NonNull Op bind(@NonNull Binding binding) {
        Op main = children.get(0), filter = children.get(1);
        var filterInputs = filter.inputVars();
        Binding bindingSubset = binding.filter(v -> !filterInputs.contains(v));
        Op bMain = main.bind(binding), bFilter = filter.bind(bindingSubset);
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
