package com.github.lapesd.hdtss.sparql.optimizer.impl;

import com.github.lapesd.hdtss.model.nodes.Filter;
import com.github.lapesd.hdtss.model.nodes.Join;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.sparql.optimizer.Optimizer;
import com.github.lapesd.hdtss.utils.Binding;
import com.github.lapesd.hdtss.utils.SmallRecursiveSet;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.annotation.Order;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;

import static com.github.lapesd.hdtss.sparql.optimizer.impl.OptimizerUtils.optimizeChildren;

@Singleton
@Order(200)
@Requires(property = "sparql.optimizer.filter", pattern = "(?i)t(rue)?|1|on",
          defaultValue = "false")
public class FilterPusher implements Optimizer {
    @Override public @NonNull Op optimize(@NonNull Op op) {
        if (op instanceof Filter filter) {
            Op child = filter.children().get(0);
            Op replacement = new Pusher(filter).optimize(child);
            return replacement == child ? op : replacement;
        } else {
            return optimizeChildren(op, this);
        }
    }

    @Override public @NonNull Op optimize(@NonNull Op op, @NonNull Binding ignore) {
        return optimize(op);
    }

    private static final class Pusher implements Optimizer {
        private final @NonNull Filter filter;
        private int depth = 0;

        public Pusher(@NonNull Filter filter) { this.filter = filter; }

        @Override public @NonNull Op optimize(@NonNull Op op, @NonNull Binding ignored) {
            return optimize(op);
        }

        @Override public @NonNull Op optimize(@NonNull Op op) {
            var filterVars = filter.filtersVarNames();
            if (!op.outputVars().containsAll(filterVars))
                return op; // cannot push
            ++depth;
            Op replacement = switch (op.type()) {
                case ASK,TRIPLE,IDENTITY -> op;
                case MINUS, EXISTS, FILTER, SLICE, DISTINCT, PROJECT, ASSIGN, WEAK_DISTINCT, VALUES -> optimize(op.children().get(0));
                case UNION -> optimizeChildren(op, this);
                case JOIN, LEFT_JOIN -> {
                    var children = op.children();
                    int size = children.size();
                    if (size == 2)
                        yield optimizeChildren(op, this);
                    var pending = SmallRecursiveSet.fromDistinct(filterVars);
                    boolean change = false;
                    int first = 0;
                    for (int i = 0; i < size; i++) {
                        if (!pending.removeAll(children.get(i).outputVars())) {
                            // no contribution -> non-contiguous subset, start new candidate subset
                            pending.clear();
                            pending.addAll(filterVars);
                            first = i+1;
                        } else if (pending.isEmpty()) { // subset satisfies filter
                            if (first == 0 && i == size-1) { //needs all operands
                                yield op;
                            } else if (first < i) { // found a subset > 1
                                change = true;
                                int subsetSize = i - first + 1;
                                List<Op> subset = new ArrayList<>(subsetSize);
                                for (int j = first; j <= i; j++)
                                    subset.add(children.get(j));
                                List<Op> joinOperands = new ArrayList<>(size-subsetSize+1);
                                for (int j = 0; j < first; j++)
                                    joinOperands.add(children.get(j));
                                joinOperands.add(filter.withChildren(List.of(new Join(subset))));
                                for (int j = i+1; j < size; j++)
                                    joinOperands.add(children.get(j));
                                size = (children = joinOperands).size();
                                i = first;
                            } else { //only a single operands needs wrapping
                                if (!change) { //only copy children once
                                    children = new ArrayList<>(children);
                                    change = true;
                                }
                                children.set(i, filter.withChildren(List.of(children.get(i))));
                            }
                            first = i+1; // start new candidate subset
                        }
                    }
                    yield change ? op.withChildren(children) : op;
                }
            };
            --depth;
            if (replacement == op && depth > 0)
                replacement = filter.withChildren(List.of(op));
            return replacement;
        }
    }
}
