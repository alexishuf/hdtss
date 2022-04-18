package com.github.lapesd.hdtss.sparql.optimizer;

import com.github.lapesd.hdtss.model.nodes.Op;
import org.checkerframework.checker.nullness.qual.NonNull;

public interface OptimizerRunner {
    @NonNull Op optimize(@NonNull Op op);
}
