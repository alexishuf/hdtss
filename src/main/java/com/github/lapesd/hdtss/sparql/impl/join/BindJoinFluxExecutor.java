package com.github.lapesd.hdtss.sparql.impl.join;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.FluxQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

@Singleton
@Named("join")
@RequiresOperatorFlow(values = {"REACTIVE", "HEAVY_REACTIVE"})
@Requires(property = "sparql.join.strategy", value = "BIND", defaultValue = "BIND")
public class BindJoinFluxExecutor extends BindJoinItExecutor {
    @Inject
    public BindJoinFluxExecutor(@NonNull OpExecutorDispatcher dispatcher,
                                @NonNull JoinReorderStrategy reorderStrategy) {
        super(dispatcher, reorderStrategy);
    }

    @Override
    protected @NonNull QuerySolutions execute(boolean isLeft, @NonNull List<@NonNull Op> operands,
                                              @NonNull List<@NonNull String> varNames) {
        QuerySolutions its = super.execute(isLeft, operands, varNames);
        return new FluxQuerySolutions(its.varNames(), its.flux());
    }
}
