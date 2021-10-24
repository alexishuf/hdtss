package com.github.lapesd.hdtss.sparql.impl.distinct;

import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import com.github.lapesd.hdtss.sparql.DistinctStrategy;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.LinkedHashSet;

@Singleton
@Requires(property = "sparql.distinct.strategy", value = "WINDOW")
public record WindowDistinctStrategy(
        @Property(name = "sparql.distinct.window", defaultValue = "8192")
        int windowSize
) implements DistinctStrategy {


    @Override public @NonNull Collection<SolutionRow> createSet() {
        return new LinkedHashSet<>() {
            @Override public boolean add(SolutionRow solutionRow) {
                if (size() >= windowSize) {
                    var it = iterator();
                    it.next();
                    it.remove();
                }
                return super.add(solutionRow);
            }
        };
    }
}
