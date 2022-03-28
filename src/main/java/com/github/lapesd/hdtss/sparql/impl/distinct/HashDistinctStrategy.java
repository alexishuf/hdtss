package com.github.lapesd.hdtss.sparql.impl.distinct;

import com.github.lapesd.hdtss.model.Row;
import com.github.lapesd.hdtss.sparql.DistinctStrategy;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.HashSet;

@Singleton
@Requires(property = "sparql.distinct.strategy", value = "HASH", defaultValue = "HASH")
public class HashDistinctStrategy implements DistinctStrategy {
    @Override public @NonNull Collection<Row> createSet() {
        return new HashSet<>();
    }
}
