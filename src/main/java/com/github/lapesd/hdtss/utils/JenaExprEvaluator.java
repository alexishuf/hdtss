package com.github.lapesd.hdtss.utils;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.function.FunctionEnvBase;
import org.apache.jena.sparql.util.Context;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public class JenaExprEvaluator implements
        Function<@NonNull Expr, @NonNull Term>,
        Predicate<@NonNull Expr> {
    private final @NonNull DatasetGraph dsg = DatasetFactory.create().asDatasetGraph();
    private final @NonNull Context jCtx = Context.setupContextForDataset(ARQ.getContext(), dsg);
    protected final @NonNull FunctionEnvBase fnEnv = new FunctionEnvBase(jCtx, dsg.getDefaultGraph(), dsg);
    protected final @NonNull RowJenaBinding tmpBinding;

    public JenaExprEvaluator(@NonNull List<String> inputVars) {
        this.tmpBinding = new RowJenaBinding(inputVars);
    }

    public @NonNull JenaExprEvaluator setInput(@NonNull SolutionRow row) {
        tmpBinding.setRow(row);
        return this;
    }

    public @NonNull Term apply(@NonNull Expr expr, @NonNull SolutionRow row) {
        return setInput(row).apply(expr);
    }

    public boolean test(@NonNull Expr expr, @NonNull SolutionRow row) {
        return setInput(row).test(expr);
    }

    @Override public @NonNull Term apply(@NonNull Expr expr) {
        return JenaUtils.fromNode(expr.eval(tmpBinding, fnEnv).asNode());
    }

    @Override public boolean test(@NonNull Expr expr) {
        return expr.isSatisfied(tmpBinding, fnEnv);
    }
}
