package com.github.lapesd.hdtss.sparql.impl.filter;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Filter;
import com.github.lapesd.hdtss.model.nodes.Op.Type;
import com.github.lapesd.hdtss.sparql.OpExecutor;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.utils.Binding;
import com.github.lapesd.hdtss.utils.ExprUtils;
import com.github.lapesd.hdtss.utils.JenaExprEvaluator;
import com.github.lapesd.hdtss.utils.JenaUtils;
import org.apache.jena.sparql.expr.Expr;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

abstract class JenaFilterExecutor implements OpExecutor {
    private static final @NonNull Set<Type> SUPPORTED_TYPES = Set.of(Type.FILTER);
    protected final @NonNull OpExecutorDispatcher dispatcher;

    public JenaFilterExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        this.dispatcher = dispatcher;
        JenaUtils.init();
    }

    @Override public @NonNull Set<Type> supportedTypes() {
        return SUPPORTED_TYPES;
    }

    protected final @NonNull Evaluator
    createEvaluator(@NonNull Filter filterNode, @Nullable Binding binding) {
        if (binding == null)
            return new Evaluator(filterNode.inner().outputVars(), filterNode.filters());
        var filterVars = filterNode.filtersVarNames();
        List<String> filters = filterNode.filters();
        List<String> inVars = binding.unbound(filterNode.inner().outputVars());
        for (int i = 0, size = binding.size(); i < size; i++) {
            if (filterVars.contains(binding.var(i))) {
                List<String> boundFilters = new ArrayList<>(filters.size());
                for (String filter : filters)
                    boundFilters.add(ExprUtils.bindExpr(filter, binding));
                filters = boundFilters;
                break;
            }
        }
        return new Evaluator(inVars, filters);
    }

    protected static class Evaluator implements Predicate<@Nullable Term @NonNull[]> {
        private final @NonNull List<@NonNull String> inVars;
        private final @NonNull JenaExprEvaluator evaluator;
        private final @NonNull List<Expr> parsedFilters;

        public Evaluator(@NonNull List<String> inVars, @NonNull List<@NonNull String> filters) {
            this.inVars = inVars;
            this.evaluator = new JenaExprEvaluator(inVars);
            this.parsedFilters = JenaUtils.parseFilters(filters);
        }

        public @NonNull List<@NonNull String> inVars() { return inVars; }

        @Override public boolean test(@Nullable Term @NonNull[] row) {
            evaluator.setInput(row);
            for (Expr filter : parsedFilters) {
                if (!evaluator.test(filter))
                    return false;
            }
            return true;
        }
    }
}
