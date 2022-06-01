package com.github.lapesd.hdtss.sparql.optimizer.impl;

import com.github.lapesd.hdtss.model.nodes.Assign;
import com.github.lapesd.hdtss.model.nodes.Filter;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.Project;
import com.github.lapesd.hdtss.sparql.optimizer.Optimizer;
import com.github.lapesd.hdtss.utils.Binding;
import com.github.lapesd.hdtss.utils.SmallRecursiveSet;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.annotation.Order;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.github.lapesd.hdtss.sparql.optimizer.impl.OptimizerUtils.optimizeChildren;

@Order(300)
@Singleton
@Requires(property = "sparql.optimizer.projection", pattern = "(?i)t(rue)?|1|on",
          defaultValue = "true")
public class ProjectionPusher implements Optimizer {
    @Override public @NonNull Op optimize(@NonNull Op op) {
        return switch (op.type()) {
            case PROJECT, ASK -> {
                Op child = op.children().get(0);
                Op opt = new Active(op.outputVars()).optimize(child);
                if (opt.type() == Op.Type.PROJECT)
                    opt = op.children().get(0);
                yield opt == child ? op : op.withChildren(List.of(opt));
            }
            case EXISTS, MINUS -> {
                List<@NonNull Op> children = op.children();
                Active active = new Active(new SmallRecursiveSet<>());
                Op right = children.get(1), opt = optimizeChildren(right, active);
                if (opt.type() == Op.Type.PROJECT)
                    opt = opt.children().get(0);
                yield opt == right ? op : op.withChildren(List.of(children.get(0), opt));
            }
            case DISTINCT,WEAK_DISTINCT,FILTER,SLICE,VALUES -> optimizeChildren(op, this);
            default -> op;
        };
    }

    @Override public @NonNull Op optimize(@NonNull Op op, @NonNull Binding binding) {
        return optimize(op);
    }

    private static final class Active implements Optimizer {
        private final @NonNull SmallRecursiveSet<String> useful;

        private Active(@NonNull Collection<String> useful) {
            this.useful = SmallRecursiveSet.fromDistinct(useful);
        }

        @Override public @NonNull Op optimize(@NonNull Op op, @NonNull Binding ignored) {
            return optimize(op);
        }

        @Override public @NonNull Op optimize(@NonNull Op op) {
            Op replacement = switch (op.type()) {
                case ASSIGN -> {
                    var assignedVars = ((Assign) op).assignedVars();
                    useful.addAll(assignedVars);
                    Op opt = optimizeChildren(op, this);
                    useful.removeAll(assignedVars);
                    yield opt;
                }
                case FILTER -> {
                    var filterVars = ((Filter) op).filtersVarNames();
                    useful.addAll(filterVars);
                    Op result = optimizeChildren(op, this);
                    useful.removeAll(filterVars);
                    yield result;
                }
                case JOIN,LEFT_JOIN,EXISTS,MINUS -> {
                    Op[] children = op.children().toArray(Op[]::new);
                    for (Op child : children) {
                        useful.addAll(child.outputVars());
                        useful.addAll(child.inputVars());
                    }
                    boolean change = false;
                    for (int i = 0; i < children.length; i++) {
                        Op child = children[i];
                        useful.removeAll(child.outputVars());
                        useful.removeAll(child.inputVars());
                        Op childReplacement = optimize(children[i]);
                        change |= childReplacement != child;
                        children[i] = childReplacement;
                        useful.addAll(child.outputVars());
                        useful.addAll(child.inputVars());
                    }
                    yield change ? op.withChildren(Arrays.asList(children)) : op;
                }
                case ASK -> new Active(new SmallRecursiveSet<>()).optimize(op);
                case TRIPLE -> op;
                case IDENTITY,DISTINCT,WEAK_DISTINCT,UNION,VALUES,SLICE,PROJECT
                        -> optimizeChildren(op, this);
            };
            List<@NonNull String> offer = replacement.outputVars();
            List<String> used = useful.intersectionList(offer);
            if (used.size() < offer.size())
                return new Project(used, replacement);
            return replacement;
        }
    }
}
