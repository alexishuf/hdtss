package com.github.lapesd.hdtss.sparql.optimizer.impl;

import com.github.lapesd.hdtss.data.query.CardinalityEstimator;
import com.github.lapesd.hdtss.model.nodes.Filter;
import com.github.lapesd.hdtss.model.nodes.Join;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.sparql.optimizer.Optimizer;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.annotation.Order;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

@Singleton
@Order(100)
@Requires(property = "sparql.optimizer.join", pattern = "(?i)t(rue)?|1|on", defaultValue = "true")
@Requires(property = "sparql.optimizer.filter-join-penalty",
          pattern = "\\d*\\.?[1-9]\\d*\\.?\\d*", defaultValue = "5")
@Requires(property = "sparql.optimizer.filter", pattern = "(?i)t(rue)?|1|on")
public class FilterAwareJoinOrderOptimizer implements Optimizer {
    private final @NonNull MyHelper helper;

    @Inject public
    FilterAwareJoinOrderOptimizer(CardinalityEstimator estimator,
                                  @Property(name = "sparql.optimizer.join.filter-penalty",
                                            defaultValue = "5") double penalty) {
        helper = new MyHelper(estimator, penalty/100);
    }

    @Override public @NonNull Op optimize(@NonNull Op op) {
        Op.Type type = op.type();
        return switch (type) {
            case FILTER -> {
                boolean pushed = helper.pushFilterVars(((Filter) op).filtersVarNames());
                Op optimized = OptimizerUtils.optimizeChildren(op, this);
                if (pushed) helper.filterVarsStack.pop();
                yield  optimized;
            }
            case EXISTS, MINUS -> {
                Op l = op.children().get(0), r = op.children().get(1);
                boolean pushed;
                if (type == Op.Type.MINUS)
                    pushed = helper.pushFilterVars(r.outputVars());
                else
                    pushed = helper.pushFilterVars(r.inputVars(), r.outputVars());
                Op lOptimized = optimize(l);
                if (pushed) helper.filterVarsStack.pop();
                ArrayDeque<Collection<String>> oldStack = helper.filterVarsStack;
                helper.filterVarsStack = new ArrayDeque<>();
                Op rOptimized = optimize(r);
                helper.filterVarsStack = oldStack;
                if (lOptimized != l || rOptimized != r)
                    yield op.withChildren(List.of(lOptimized, rOptimized));
                yield op;
            }
            case JOIN -> helper.reorder((Join)op);
            default   -> OptimizerUtils.optimizeChildren(op, this);
        };
    }

    private static class MyHelper extends JoinOrderHelper {
        private final double penaltyRatio;
        private @NonNull ArrayDeque<Collection<String>> filterVarsStack = new ArrayDeque<>();
        private @Nullable Collection<String> activeFilterVars = null;

        public MyHelper(@NonNull CardinalityEstimator estimator, double penaltyRatio) {
            super(estimator);
            this.penaltyRatio = penaltyRatio;
        }

        public boolean pushFilterVars(@NonNull Collection<String> vars) {
            boolean done = !vars.isEmpty();
            if (done) filterVarsStack.push(vars);
            return done;
        }
        public boolean pushFilterVars(@NonNull Collection<String> set,
                                      @NonNull Collection<String> additional) {
            if (set.isEmpty() && additional.isEmpty())
                return false;
            if (set.isEmpty()) {
                filterVarsStack.push(additional);
            } else if (additional.isEmpty()) {
                filterVarsStack.push(set);
            } else {
                int capacity = (int) Math.max(4, (set.size()+additional.size())/0.75f + 1);
                HashSet<String> union = new HashSet<>(capacity);
                union.addAll(additional);
                union.addAll(set);
                filterVarsStack.push(union);
            }
            return true;
        }

        @Override public @NonNull Op reorder(@NonNull Join join) {
            Collection<String> oldActiveFilterVars = this.activeFilterVars;
            List<@NonNull String> offer = join.outputVars();
            for (Collection<String> vars : filterVarsStack) {
                if (offer.containsAll(vars)) {
                    this.activeFilterVars = vars;
                    break;
                }
            }
            Op optimized = super.reorder(join);
            activeFilterVars = oldActiveFilterVars;
            return optimized;
        }

        @Override protected long estimate(@NonNull Op op) {
            long cost = super.estimate(op);
            if (activeFilterVars != null) {
                for (String v : op.outputVars()) {
                    if (activeFilterVars.contains(v)) return cost;
                }
                cost += (long) Math.max(1, cost * penaltyRatio);
            }
            return cost;
        }
    }
}
