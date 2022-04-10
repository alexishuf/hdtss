package com.github.lapesd.hdtss.model.nodes;

import com.github.lapesd.hdtss.utils.Binding;
import com.github.lapesd.hdtss.utils.ExprUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.compile;

public final class Filter extends AbstractOp {
    /* --- --- --- constants --- --- --- */
    private static final Pattern WRAPPER_RX = compile("(?i)^\\s*FILTER\\((.*)\\)\\s*$");

    /* --- --- --- instance fields and constructor --- --- --- */
    private final @NonNull List<@NonNull String> filters;
    private @Nullable Set<String> filtersVarNames;

    public Filter(@NonNull Op inner,
                  @NonNull List<@NonNull String> filters) {
        super(List.of(inner));
        this.filters = filters;
        assert filters.stream().noneMatch(WRAPPER_RX.asPredicate())
                : "Filter expressions must not be wrapped with FILTER()";
    }

    public Filter(@NonNull Op inner, String... filters) {
        this(inner, Arrays.asList(filters));
    }

    public static @NonNull Filter withFilters(@NonNull Op inner, @NonNull String... filters) {
        return withFilters(inner, Arrays.asList(filters));
    }
    public static @NonNull Filter withFilters(@NonNull Op inner,
                                              @NonNull List<@NonNull String> filters) {
        List<String> list;
        if (inner instanceof Filter o) {
            inner = o.inner();
            (list = new ArrayList<>(o.filters.size() + filters.size())).addAll(o.filters);
        } else {
            list = new ArrayList<>(filters.size());
        }
        list.addAll(filters);
        return new Filter(inner, list);
    }

    public @NonNull Op inner() {
        return children.get(0);
    }

    public @NonNull List<@NonNull String> filters() {
        return filters;
    }

    /**
     * Get the set of variables mentioned in filter expressions.
     *
     * @return a non-null, possibly empty set of non-null and non-empty variable names
     *         (not including leading '?' and '$').
     */
    public @NonNull Set<@NonNull String> filtersVarNames() {
        if (filtersVarNames == null) {
            Set<@NonNull String> set = new HashSet<>();
            for (String filter : filters)
                ExprUtils.findVarNames(filter, set);
            filtersVarNames = Collections.unmodifiableSet(set);
        }
        return filtersVarNames;
    }

    /* --- --- --- Method implementations --- --- --- */

    @Override public @NonNull Type type() {
        return Type.FILTER;
    }

    @Override public @NonNull List<@NonNull String> outputVars() {
        return children.get(0).outputVars();
    }

    @Override public @NonNull Set<@NonNull String> inputVars() {
        Op child = children.get(0);
        List<@NonNull String> childOutputs = child.outputVars();
        Set<@NonNull String> childInputs = child.inputVars();
        Set<@NonNull String> union = null;
        for (String var : filtersVarNames()) {
            if (!childOutputs.contains(var))
                (union == null ? union = new HashSet<>() : union).add(var);
        }
        if (union == null)
            return childInputs;
        union.addAll(childInputs);
        return union;
    }

    @Override public @NonNull Op bind(@NonNull Binding binding) {
        if (!binding.intersects(outputVars()) && !binding.intersects(inputVars()))
            return this;
        List<@NonNull String> boundFilters = new ArrayList<>(filters.size());
        for (String filter : filters)
            boundFilters.add(ExprUtils.bindExpr(filter, binding));
        return new Filter(inner().bind(binding), boundFilters);
    }

    @Override
    public @NonNull Op withChildren(@NonNull List<@NonNull Op> replacements) {
        Op replacement = OpUtils.single(replacements);
        return replacement instanceof Filter f ? flatten(f) : new Filter(replacement, filters);
    }

    @Override public boolean deepEquals(@NonNull Op other) {
        if (!(other instanceof Filter rhs)) return false;
        return filters.equals(rhs.filters) && children.get(0).deepEquals(rhs.children.get(0));
    }

    @Override public @NonNull String toString() {
        return "Filter"+filters+"("+inner()+")";
    }

    @NonNull Filter flatten(@NonNull Filter child) {
        if (child.inner() instanceof Filter f)
            child = child.flatten(f);
        List<String> filterUnion = new ArrayList<>(filters.size() + child.filters.size());
        filterUnion.addAll(filters);
        filterUnion.addAll(child.filters);
        return new Filter(child.inner(), filterUnion);
    }

}
