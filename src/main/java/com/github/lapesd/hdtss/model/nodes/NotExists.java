package com.github.lapesd.hdtss.model.nodes;

import com.github.lapesd.hdtss.model.Term;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.Map;

public class NotExists extends AbstractOp {
    public NotExists(@NonNull Op outer, @NonNull Op inner) {
        super(List.of(outer, inner));
    }

    public @NonNull Op outer() {
        return children.get(0);
    }

    public @NonNull Op inner() {
        return children.get(1);
    }

    @Override public @NonNull Type type() {
        return Type.NOT_EXISTS;
    }

    @Override public @NonNull List<@NonNull String> varNames() {
        return outer().varNames();
    }

    @Override public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        int size = replacements.size();
        if (size != 2)
            throw new IllegalArgumentException("NOT EXISTS requires exactly 2 child nodes");
        return new NotExists(replacements.get(0), replacements.get(1));
    }

    @Override
    public @NonNull Op bind(@NonNull List<String> varNames, Term @NonNull [] row,
                            @NonNull BindType bindType) {
        if (row.length != varNames.size())
            throw new IllegalArgumentException("row.length != varNames.size()");
        else if (row.length == 0)
            return this;
        Op boundOuter = outer().bind(varNames, row, bindType);
        Op boundInner = bindType == BindType.ONLY_TRIPLES ? inner()
                      : inner().bind(varNames, row, bindType);
        return new NotExists(boundOuter, boundInner);
    }

    @Override
    public @NonNull Op bind(@NonNull Map<String, Term> var2term, @NonNull BindType bindType) {
        if (var2term.isEmpty())
            return this;
        Op boundOuter = outer().bind(var2term, bindType);
        Op boundInner = bindType == BindType.ONLY_TRIPLES ? inner()
                      : inner().bind(var2term, bindType);
        return new NotExists(boundOuter, boundInner);
    }

    @Override public boolean deepEquals(@NonNull Op other) {
        if (!(other instanceof NotExists ne)) return false;
        return outer().deepEquals(ne.outer()) && inner().deepEquals(ne.inner());
    }

    @Override public @NonNull String toString() {
        return "NotExists("+outer()+", "+inner()+")";
    }
}
