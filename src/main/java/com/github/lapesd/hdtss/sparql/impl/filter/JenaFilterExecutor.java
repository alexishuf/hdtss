package com.github.lapesd.hdtss.sparql.impl.filter;

import com.github.lapesd.hdtss.model.nodes.Filter;
import com.github.lapesd.hdtss.model.nodes.Op.Type;
import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import com.github.lapesd.hdtss.sparql.OpExecutor;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.utils.JenaExprEvaluator;
import com.github.lapesd.hdtss.utils.JenaUtils;
import org.apache.jena.sparql.expr.Expr;
import org.checkerframework.checker.nullness.qual.NonNull;

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

    protected static class Evaluator implements Predicate<@NonNull SolutionRow> {
        private final @NonNull JenaExprEvaluator evaluator;
        private final @NonNull List<Expr> parsedFilters;

        public Evaluator(@NonNull Filter filter) {
            this.evaluator = new JenaExprEvaluator(filter.inner().varNames());
            this.parsedFilters = JenaUtils.parseFilters(filter.filters());
        }

        @Override public boolean test(@NonNull SolutionRow row) {
            evaluator.setInput(row);
            for (Expr filter : parsedFilters) {
                if (!evaluator.test(filter))
                    return false;
            }
            return true;
        }
    }
}
