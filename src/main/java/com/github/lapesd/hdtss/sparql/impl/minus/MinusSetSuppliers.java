package com.github.lapesd.hdtss.sparql.impl.minus;

import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

public class MinusSetSuppliers {
    @Bean @Singleton @Named("minusSet")
    @Requires(property = "sparql.minus.set", value = "HASH", defaultValue = "HASH")
    public @NonNull Supplier<Set<SolutionRow>> hashSet() { return HashSet::new; }

    @Bean @Singleton @Named("minusSet")
    @Requires(property = "sparql.minus.set", value = "TREE")
    public @NonNull Supplier<Set<SolutionRow>> treeSet() { return TreeSet::new; }
}
