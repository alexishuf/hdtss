package com.github.lapesd.hdtss.sparql.optimizer.impl;

import com.github.lapesd.hdtss.model.nodes.Op;
import com.github.lapesd.hdtss.sparql.optimizer.OptimizerRunner;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

@Singleton
@Requires(property = "sparql.optimizer.runner", value = "NONE")
public class NoneOptimizerRunner implements OptimizerRunner {
    @Override public @NonNull Op optimize(@NonNull Op op) {
        return op;
    }
}
