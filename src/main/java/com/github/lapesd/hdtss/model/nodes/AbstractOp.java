package com.github.lapesd.hdtss.model.nodes;

import com.github.lapesd.hdtss.model.Term;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.stream.Collectors;

abstract class AbstractOp implements Op {
    protected final @NonNull List<@NonNull Op> children;
    protected @Nullable List<@NonNull String> varNames = null;

    public AbstractOp(@NonNull List<@NonNull Op> children) {
        this.children = children;
    }

    @Override public @NonNull List<@NonNull Op> children() {
        return children;
    }

    @Override public @NonNull List<@NonNull String> varNames() {
        if (varNames == null) {
            Set<String> set = new LinkedHashSet<>();
            for (Op child : children)
                set.addAll(child.varNames());
            varNames = new ArrayList<>(set);
        }
        return varNames;
    }

    @Override public @NonNull Op bind(@NonNull List<String> varNames, Term @NonNull [] row,
                                      @NonNull BindType bindType) {
        if (varNames.isEmpty())
            return this;
        List<@NonNull Op> list = new ArrayList<>(children().size());
        for (Op child : children())
            list.add(child.bind(varNames, row));
        return withChildren(list);
    }

    @Override public @NonNull Op bind(@NonNull Map<String, Term> var2term,
                                      @NonNull BindType bindType) {
        if (var2term.isEmpty())
            return this;
        List<@NonNull Op> list = new ArrayList<>(children().size());
        for (Op child : children())
            list.add(child.bind(var2term));
        return withChildren(list);
    }

    @Override public @NonNull String toString() {
        String list = children.stream().map(Objects::toString).collect(Collectors.joining(", "));
        return getClass().getSimpleName()+"("+list+")";
    }
}
