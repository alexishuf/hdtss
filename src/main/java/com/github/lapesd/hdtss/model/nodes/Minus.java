package com.github.lapesd.hdtss.model.nodes;

import com.github.lapesd.hdtss.model.Term;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

public class Minus extends AbstractOp {
    public Minus(@NonNull Op outer, @NonNull Op inner) {
        super(asList(outer, inner));
    }

    public @NonNull Op outer() {
        return children.get(0);
    }

    public @NonNull Op inner() {
        return children.get(1);
    }

    @Override public @NonNull Type type() {
        return Type.MINUS;
    }

    @Override public @NonNull List<@NonNull String> varNames() {
        return outer().varNames();
    }

    @Override public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        if (replacements.size() != 2)
            throw new IllegalArgumentException("MINUS must have an outer node and an inner node");
        return new Minus(replacements.get(0), replacements.get(1));
    }

    @Override
    public @NonNull Op bind(@NonNull List<String> varNames, Term @NonNull [] row,
                            @NonNull BindType bindType) {
        if (row.length != varNames.size())
            throw new IllegalArgumentException("varNames.size() != row.length");
        else if (row.length == 0)
            return this;
        Op boundOuter = outer().bind(varNames, row, bindType);
        Op boundInner = inner().bind(varNames, row, BindType.ONLY_TRIPLES);
        return new Minus(boundOuter, boundInner);
    }

    @Override
    public @NonNull Op bind(@NonNull Map<String, Term> var2term,
                            @NonNull BindType bindType) {
        if (var2term.isEmpty())
            return this;
        Op boundOuter = outer().bind(var2term, bindType);
        Op boundInner = inner().bind(var2term, BindType.ONLY_TRIPLES);
        return new Minus(boundOuter, boundInner);
    }

    @Override public boolean deepEquals(@NonNull Op other) {
        return other instanceof Minus m
                && outer().deepEquals(m.outer())
                && inner().deepEquals(m.inner());
    }

    @Override public @NonNull String toString() {
        return "Minus("+outer()+", "+inner()+")";
    }
}
