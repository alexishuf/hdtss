package com.github.lapesd.hdtss.sparql;

import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;

public interface DistinctStrategy {
    @NonNull Collection<SolutionRow> createSet();
}
