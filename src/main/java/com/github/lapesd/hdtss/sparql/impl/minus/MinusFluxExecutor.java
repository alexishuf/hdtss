package com.github.lapesd.hdtss.sparql.impl.minus;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Minus;
import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.model.solutions.FluxQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.conditional.RequiresOperatorFlow;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Predicate;

@Singleton
@Named("minus")
@RequiresOperatorFlow(values = {"REACTIVE", "HEAVY_REACTIVE"})
public class MinusFluxExecutor extends MinusExecutor {
    @Inject public MinusFluxExecutor(@NonNull OpExecutorDispatcher dispatcher,
                             @NonNull MinusStrategy strategy) {
        super(dispatcher, strategy);
    }

    @Override public @NonNull QuerySolutions execute(@NonNull Op node) {
        Predicate<@Nullable Term @NonNull[]> filter = strategy.createFilter((Minus) node);
        return new FluxQuerySolutions(node.outputVars(),
                dispatcher.execute(node.children().get(0)).flux().filter(filter));
    }
}
