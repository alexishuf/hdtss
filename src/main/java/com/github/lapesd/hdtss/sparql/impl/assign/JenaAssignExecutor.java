package com.github.lapesd.hdtss.sparql.impl.assign;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Assign;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.sparql.OpExecutor;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.utils.JenaExprEvaluator;
import com.github.lapesd.hdtss.utils.JenaUtils;
import org.apache.jena.sparql.expr.Expr;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

abstract class JenaAssignExecutor implements OpExecutor {
    private static final @NonNull Set<Op.Type> SUPPORTED_TYPES = Set.of(Op.Type.ASSIGN);
    protected final @NonNull OpExecutorDispatcher dispatcher;

    public JenaAssignExecutor(@NonNull OpExecutorDispatcher dispatcher) {
        this.dispatcher = dispatcher;
        JenaUtils.init();
    }

    @Override public @NonNull Set<Op.Type> supportedTypes() {
        return SUPPORTED_TYPES;
    }

    protected static class Evaluator
            implements Function<@Nullable Term @NonNull[], @Nullable Term @NonNull[]> {
        private final @NonNull JenaExprEvaluator evaluator;
        private final @NonNull Map<String, Expr> var2expr;
        private final @NonNull List<String> outVars;

        public Evaluator(@NonNull Assign assign) {
            this.evaluator = new JenaExprEvaluator(assign.inner().outputVars());
            this.var2expr = new HashMap<>();
            for (Map.Entry<String, String> e : assign.var2expr().entrySet())
                this.var2expr.put(e.getKey(), JenaUtils.parseFilter(e.getValue()));
            this.outVars = assign.outputVars();
            assert assign.outputVars().stream().filter(v -> !var2expr.containsKey(v))
                    .allMatch(v -> outVars.indexOf(v) == assign.inner().outputVars().indexOf(v))
                    : "Assign is rearranging variables from its input Op";
        }

        @Override public @Nullable Term @NonNull[] apply(@Nullable Term @NonNull[] inputs) {
            evaluator.setInput(inputs);
            Term[] outputs = new Term[outVars.size()];
            for (int i = 0; i < outputs.length; i++) {
                var expr = var2expr.getOrDefault(outVars.get(i), null);
                outputs[i] = expr == null ? inputs[i] : evaluator.apply(expr);
            }
            return outputs;
        }
    }

}
