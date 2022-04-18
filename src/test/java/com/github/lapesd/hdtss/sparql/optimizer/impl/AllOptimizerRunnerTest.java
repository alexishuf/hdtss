package com.github.lapesd.hdtss.sparql.optimizer.impl;

import com.github.lapesd.hdtss.sparql.optimizer.Optimizer;
import com.github.lapesd.hdtss.sparql.optimizer.OptimizerRunner;
import io.micronaut.context.ApplicationContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("fast")
class AllOptimizerRunnerTest {
    @Test
    void testEnabledByDefault() {
        try (var ctx = ApplicationContext.builder().args("-hdt.estimator=PATTERN").start()) {
            assertTrue(ctx.getBean(OptimizerRunner.class) instanceof AllOptimizerRunner);
            Collection<Optimizer> optimizers = ctx.getBeansOfType(Optimizer.class);
            assertFalse(optimizers.isEmpty());
        }
    }

}