package com.github.lapesd.hdtss.model.nodes;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.utils.BindUtils;
import com.github.lapesd.hdtss.utils.ExprUtils;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

@Accessors(fluent = true)
public class Assign extends AbstractOp {
    private final @Getter @NonNull Map<String, String> var2expr;
    private @Nullable List<String> assignedVars;

    public Assign(@NonNull Map<String, String> var2expr, @NonNull Op child) {
        super(List.of(child));
        this.var2expr = Map.copyOf(var2expr);
    }

    public Assign(@NonNull Op child, String... var2expr) {
        super(List.of(child));
        if (var2expr.length % 2 != 0) {
            var msg = "var2expr must have an even length (var, expr, var, expr, ...)*";
            throw new IllegalArgumentException(msg);
        }
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < var2expr.length; i += 2)
            map.put(var2expr[i], var2expr[i + 1]);
        this.var2expr = map;
    }

    public static @NonNull Assign
    withAssignments(@NonNull Op inner, @NonNull Map<@NonNull String, @NonNull String> var2expr) {
        if (inner instanceof Assign a) {
            inner = a.inner();
            var map = new LinkedHashMap<>(a.var2expr);
            map.putAll(var2expr);
            var2expr = map;
        }
        return new Assign(var2expr, inner);
    }

    public static @NonNull Assign withAssignments(@NonNull Op inner, @NonNull String... var2expr) {
        if (var2expr.length % 2 != 0)
            throw new IllegalArgumentException("var2expr must be of even length (var, expr, ...)");
        Map<@NonNull String, @NonNull String> map = new LinkedHashMap<>();
        for (int i = 0; i < var2expr.length; i += 2)
            map.put(var2expr[i], var2expr[i + 1]);
        return withAssignments(inner, map);
    }

    public @NonNull List<@NonNull String> assignedVars() {
        if (assignedVars == null)
            assignedVars = new ArrayList<>(var2expr.keySet());
        return assignedVars;
    }

    public @NonNull Op inner() {
        return children.get(0);
    }

    @Override public @NonNull List<@NonNull String> varNames() {
        if (varNames == null) {
            var childVars = children.get(0).varNames();
            ArrayList<@NonNull String> list = new ArrayList<>(childVars.size() + var2expr.size());
            list.addAll(childVars);
            for (String name : var2expr.keySet()) {
                if (!list.contains(name))
                    list.add(name);
            }
            varNames = list;
        }
        return varNames;
    }

    @Override public @NonNull Type type() {
        return Type.ASSIGN;
    }

    @Override public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        return new Assign(var2expr, OpUtils.single(replacements));
    }

    @Override
    public @NonNull Op bind(@NonNull List<String> varNames, Term @NonNull [] row,
                            @NonNull BindType bindType) {
        return BindUtils.bindWithMap(this, varNames, row, bindType);
    }

    @Override
    public @NonNull Op bind(@NonNull Map<String, Term> var2term, @NonNull BindType bindType) {
        if (var2term.isEmpty()) return this;
        Op boundInner = inner().bind(var2term, bindType);
        if (bindType == BindType.ONLY_TRIPLES)
            return new Assign(var2expr, boundInner);
        HashMap<String, String> boundExprs = new HashMap<>();
        for (Map.Entry<String, String> e : var2expr.entrySet()) {
            String var = e.getKey();
            if (var2term.containsKey(var))
                continue;
            boundExprs.put(var, ExprUtils.bindExpr(e.getValue(), var2term));
        }
        return new Assign(boundExprs, boundInner);
    }

    @Override public boolean deepEquals(@NonNull Op other) {
        if (!(other instanceof Assign a)) return false;
        return a.var2expr.equals(var2expr) && inner().deepEquals(a.inner());
    }
}
