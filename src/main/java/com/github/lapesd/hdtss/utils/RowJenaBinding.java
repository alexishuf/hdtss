package com.github.lapesd.hdtss.utils;

import com.github.lapesd.hdtss.model.Row;
import com.github.lapesd.hdtss.model.Term;
import lombok.Setter;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class RowJenaBinding implements Binding {
    private final @NonNull Map<@NonNull Var, @NonNull Integer> var2idx;
    private @Setter @Nullable Term @NonNull[] row = Row.EMPTY;

    public RowJenaBinding(@NonNull List<@NonNull String> varNames) {
        this.var2idx= new LinkedHashMap<>();
        for (int i = 0, size = varNames.size(); i < size; i++)
            this.var2idx.put(Var.alloc(varNames.get(i)), i);
    }

    @Override public @NonNull Iterator<Var> vars() {
        return var2idx.keySet().iterator();
    }

    @Override public boolean contains(Var var) {
        return var2idx.containsKey(var);
    }

    @Override public Node get(Var var) {
        int i = var2idx.getOrDefault(var, -1);
        return i < 0 ? null : JenaUtils.toNode(row[i]);
    }

    @Override public int size() {
        int size = 0;
        for (Term value : row) {
            if (value != null) ++size;
        }
        return size;
    }

    @Override public boolean isEmpty() {
        return size() == 0;
    }
}
