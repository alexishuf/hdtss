package com.github.lapesd.hdtss.sparql.optimizer.impl;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.WeakDistinct;
import com.github.lapesd.hdtss.sparql.optimizer.Optimizer;
import com.github.lapesd.hdtss.utils.Binding;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.annotation.Order;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;

@Singleton
@Requires(property = "sparql.optimizer.distinct", pattern = "(?i)t(rue)?|1|on",
          defaultValue = "true")
@Order(400)
public class DistinctPusher implements Optimizer {
    private static final Optimizer ACTIVE = new Optimizer() {
        private static final int TARGETS = (1 << Op.Type.JOIN.ordinal())
                                         | (1 << Op.Type.LEFT_JOIN.ordinal())
                                         | (1 << Op.Type.FILTER.ordinal());
        @Override public @NonNull Op optimize(@NonNull Op op) {
            if (((1 << op.type().ordinal()) & TARGETS) != 0) {
                List<@NonNull Op> replacements = new ArrayList<>(op.children());
                for (int i = 0, size = replacements.size(); i < size; i++)
                    replacements.set(i, new WeakDistinct(replacements.get(i)));
                return op.withChildren(replacements);
            } else {
                return OptimizerUtils.optimizeChildren(op, this);
            }
        }

        @Override public @NonNull Op optimize(@NonNull Op op, @NonNull Binding ignored) {
            return optimize(op);
        }
    };

    @Override public @NonNull Op optimize(@NonNull Op op) {
        return switch (op.type()) {
            case ASK, DISTINCT -> OptimizerUtils.optimizeChildren(op, ACTIVE);
            case MINUS, EXISTS -> {
                List<@NonNull Op> children = op.children();
                Op right = children.get(1), opt = ACTIVE.optimize(right);
                yield opt == right ? op : op.withChildren(List.of(children.get(0), opt));
            }
            default -> OptimizerUtils.optimizeChildren(op, this);
        };
    }

    @Override public @NonNull Op optimize(@NonNull Op op, @NonNull Binding ignored) {
        return optimize(op);
    }
}
