package com.github.lapesd.hdtss.sparql.optimizer.impl;

import com.github.lapesd.hdtss.data.query.CardinalityEstimator;
import com.github.lapesd.hdtss.model.nodes.Join;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.Project;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import com.github.lapesd.hdtss.utils.Binding;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.*;

class JoinOrderHelper {
    private final @NonNull CardinalityEstimator estimator;

    public JoinOrderHelper(@NonNull CardinalityEstimator estimator) {
        this.estimator = estimator;
    }

    public @NonNull Op reorder(@NonNull Join join, @NonNull Binding binding) {
        List<@NonNull Op> ops = join.children();
        int size = ops.size();
        if (size < 2)
            return join;
        long[] weights = new long[size];
        for (int i = 0; i < size; i++)
            weights[i] = (estimate(ops.get(i), binding) << 32) | i;
        Arrays.sort(weights);
        avoidProducts(join, ops, weights);
        if (isNoOp(weights))
            return join;
        ArrayList<@NonNull Op> reordered = new ArrayList<>(size);
        for (long weight : weights)
            reordered.add(ops.get((int)weight));
        Op optimized = new Join(reordered);
        if (!optimized.outputVars().equals(join.outputVars()))
            optimized = new Project(join.outputVars(), optimized);
        return optimized;
    }

    protected long estimate(@NonNull Op op, @NonNull Binding binding) {
        return switch (op.type()) {
            case TRIPLE -> estimator.estimate((TriplePattern) op.bind(binding));
            case MINUS,EXISTS -> estimate(op.children().get(0), binding);
            default -> {
                int sum = 0;
                for (Op child : op.children())
                    sum += estimate(child, binding);
                yield sum;
            }
        };
    }

    static boolean isNoOp(long[] weights) {
        boolean noOp = true;
        for (int i = 0; noOp && i < weights.length; i++)
            noOp = (int) weights[i] == i;
        return noOp;
    }

    private static void avoidProducts(@NonNull Join join, List<@NonNull Op> ops, long[] weights) {
        Set<String> vars = new HashSet<>(join.outputVars().size()*2);
        vars.addAll(ops.get((int) weights[0]).outputVars());
        for (int i = 1; i < weights.length; i++) {
            int idx = firstIntersecting(vars, ops, weights, i);
            if (idx > i) {
                long tmp = weights[idx];
                System.arraycopy(weights, i, weights, i+1, idx-i);
                weights[i] = tmp;
            }
            vars.addAll(ops.get((int) weights[i]).outputVars());
        }
    }

    private static int firstIntersecting(@NonNull Set<String> vars, @NonNull List<@NonNull Op> ops,
                                         long[] weights, int startIdx) {
        for (int i = startIdx; i < weights.length; i++) {
            for (String var : ops.get((int) weights[i]).outputVars()) {
                if (vars.contains(var))
                    return i;
            }
        }
        return -1;
    }
}
