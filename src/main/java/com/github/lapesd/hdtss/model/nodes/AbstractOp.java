package com.github.lapesd.hdtss.model.nodes;

import com.github.lapesd.hdtss.utils.Binding;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.stream.Collectors;

abstract class AbstractOp implements Op {
    protected final @NonNull List<@NonNull Op> children;
    protected @Nullable List<@NonNull String> varNames = null;
    protected @Nullable Set<@NonNull String> inputVarNames = null;

    public AbstractOp(@NonNull List<@NonNull Op> children) {
        this.children = children;
    }

    @Override public @NonNull List<@NonNull Op> children() {
        return children;
    }

    @Override public @NonNull List<@NonNull String> outputVars() {
        if (varNames == null) {
            Set<String> set = new LinkedHashSet<>();
            for (Op child : children)
                set.addAll(child.outputVars());
            varNames = new ArrayList<>(set);
        }
        return varNames;
    }

    @Override public @NonNull Set<@NonNull String> inputVars() {
        if (inputVarNames == null) {
            Set<@NonNull String> union = null;
            for (Op child : children()) {
                Set<@NonNull String> set = child.inputVars();
                if (!set.isEmpty())
                    (union == null ? union = new HashSet<>() : union).addAll(set);
            }
            inputVarNames = union == null ? Set.of() : union;
        }
        return inputVarNames;
    }

    @Override public boolean deepEquals(@NonNull Op other) {
        if (!getClass().equals(other.getClass()))
            return false;
        List<@NonNull Op> ac = children(), bc = other.children();
        int size = ac.size();
        if (bc.size() != size)
            return false;
        for (int i = 0; i < size; i++) {
            if (!ac.get(i).deepEquals(bc.get(i)))
                return false;
        }
        return true;
    }

    @Override public @NonNull Op bind(@NonNull Binding binding) {
        List<@NonNull Op> list = new ArrayList<>(children().size());
        boolean change = false;
        for (Op child : children()) {
            Op replacement = child.bind(binding);
            change |= replacement != child;
            list.add(replacement);
        }
        return change ? withChildren(list) : this;
    }

    @Override public @NonNull String toString() {
        String list = children.stream().map(Objects::toString).collect(Collectors.joining(", "));
        return getClass().getSimpleName()+"("+list+")";
    }
}
