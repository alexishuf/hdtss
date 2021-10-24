package com.github.lapesd.hdtss.sparql.impl.join;

import com.github.lapesd.hdtss.model.nodes.Join;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;

@Singleton
@Requires(property = "sparql.join.reorder", value = "NONE", defaultValue = "NONE")
public class NoJoinReorderStrategy implements JoinReorderStrategy {
    @Override public @NonNull Join reorder(@NonNull Join join) {
        return join;
    }
}
