package com.github.lapesd.hdtss.sparql.optimizer.impl;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.sparql.optimizer.Optimizer;
import com.github.lapesd.hdtss.sparql.optimizer.OptimizerRunner;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.ArrayList;
import java.util.List;

@Singleton
@Requires(property = "sparql.optimizer.runner", value = "ALL", defaultValue = "ALL")
@Slf4j
public class AllOptimizerRunner implements OptimizerRunner {
    private final @NonNull List<Optimizer> optimizers;
    private final boolean debug;

    @Inject public AllOptimizerRunner(@NonNull List<Optimizer> optimizers) {
        this.optimizers = optimizers;
        this.debug = log.isDebugEnabled();
    }

    @Override public @NonNull Op optimize(@NonNull Op input) {
        Op op = input;
        if (!debug) {
            for (Optimizer optimizer : optimizers)
                op = optimizer.optimize(op);
        } else {
            List<String> applied = new ArrayList<>(optimizers.size());
            for (Optimizer optimizer : optimizers) {
                long start = System.nanoTime();
                Op optimized = optimizer.optimize(op);
                if (optimized != op) {
                    double ms = (System.nanoTime() - start) / 1000000.0;
                    String name = optimizer.getClass().getSimpleName();
                    applied.add(String.format("%s (%.3f ms)", name, ms));
                    op = optimized;
                }
            }
            if (!applied.isEmpty())
                log.debug("Applied {} to\n    {}\n, yielding\n    {}", applied, input, op);
        }
        return op;
    }
}
