package com.github.lapesd.hdtss.sparql.impl.join;

import com.github.lapesd.hdtss.model.TermPosition;
import com.github.lapesd.hdtss.model.nodes.Join;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

@Singleton
@Requires(property = "sparql.join.reorder", value = "VARS_POS", defaultValue = "VARS_POS")
public class VarPositionsJoinReorderStrategy implements JoinReorderStrategy {

    @Override public @Nullable JoinReorder reorder(@NonNull Join join) {
        List<@NonNull Op> ops = join.children();
        int size = ops.size();
        if (size < 2)
            return null;
        long[] weights = new long[size];
        for (int i = 0; i < size; i++)
            weights[i] = ((long)estimate(ops.get(i)) << 32) | i;
        Arrays.sort(weights);
        avoidProducts(join, ops, weights);
        if (isNoOp(weights))
            return null;
        ArrayList<@NonNull Op> reordered = new ArrayList<>(size);
        for (long weight : weights)
            reordered.add(ops.get((int)weight));
        return new JoinReorder(reordered, getProjection(join.varNames(), reordered));
    }

    private void avoidProducts(@NonNull Join join, List<@NonNull Op> ops, long[] weights) {
        Set<String> vars = new HashSet<>(join.varNames().size()*2);
        vars.addAll(ops.get((int) weights[0]).varNames());
        for (int i = 1; i < weights.length; i++) {
            int idx = firstIntersecting(vars, ops, weights, i);
            if (idx > i) {
                long tmp = weights[idx];
                System.arraycopy(weights, i, weights, i+1, idx-i);
                weights[i] = tmp;
            }
            vars.addAll(ops.get((int) weights[i]).varNames());
        }
    }

    private int firstIntersecting(@NonNull Set<String> vars, @NonNull List<@NonNull Op> ops,
                                  long[] weights, int startIdx) {
        for (int i = startIdx; i < weights.length; i++) {
            for (String var : ops.get((int) weights[i]).varNames()) {
                if (vars.contains(var))
                    return i;
            }
        }
        return -1;
    }

    static int @Nullable [] getProjection(@NonNull List<@NonNull String> exposedVars,
                                          @NonNull List<@NonNull Op> reordered) {
        List<String> effVars = new ArrayList<>(exposedVars.size());
        for (Op op : reordered) {
            for (String var : op.varNames()) {
                if (!effVars.contains(var))
                    effVars.add(var);
            }
        }
        assert effVars.size() == exposedVars.size();
        int nVars = exposedVars.size();
        int[] projection = new int[exposedVars.size()];
        boolean trivial = true;
        for (int i = 0; i < nVars; i++) {
            projection[i] = effVars.indexOf(exposedVars.get(i));
            trivial &= projection[i] == i;
        }
        assert Arrays.stream(projection).allMatch(i -> i >= 0);
        return trivial ? null : projection;
    }

    static boolean isNoOp(long[] weights) {
        boolean noOp = true;
        for (int i = 0; noOp && i < weights.length; i++)
            noOp = (int) weights[i] == i;
        return noOp;
    }

    static int estimate(@NonNull Op op) {
        if (op instanceof TriplePattern tp) {
            var positions = tp.collectVarsInfo().positions();
            return switch (positions.length) {
                case 0 -> 0;
                case 1 -> switch (positions[0]) {
                    case SUB -> 1000;
                    case PRE -> 10;
                    case OBJ -> 20;
                };
                case 2 -> switch (positions[0]) {
                    case SUB -> switch (positions[1]) {
                        case PRE -> 2000 ;
                        case OBJ -> 10000;
                        default -> throw new IllegalArgumentException("unordered collectVarsInfo");
                    };
                    case PRE -> {
                        if (positions[1] != TermPosition.OBJ)
                            throw new IllegalArgumentException("unordered collectVarsInfo");
                        yield 100;
                    }
                    default -> throw new IllegalArgumentException("unordered collectVarsInfo()");
                };
                default -> 100000;
            };
        } else {
            int sum = 0;
            for (Op child : op.children())
                sum += estimate(child);
            return sum;
        }
    }
}
