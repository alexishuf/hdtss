package com.github.lapesd.hdtss.sparql;

import com.github.lapesd.hdtss.model.Row;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;

public interface DistinctStrategy {
    @NonNull Collection<@NonNull Row> createSet();
}
