package com.github.lapesd.hdtss.sparql.impl.minus;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Minus;
import com.github.lapesd.hdtss.sparql.OpExecutorDispatcher;
import com.github.lapesd.hdtss.sparql.impl.Binder;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Predicate;

@Requires(property = "sparql.minus.strategy", value = "BIND", defaultValue = "BIND")
public class BindMinusStrategy implements MinusStrategy {
    protected final @NonNull OpExecutorDispatcher dispatcher;

    @Inject
    public BindMinusStrategy(@NonNull OpExecutorDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override public @NonNull Predicate<@Nullable Term @NonNull[]>
    createFilter(@NonNull Minus minus) {
        var binder = new Binder(minus.outer(), minus.inner());
        return r -> !dispatcher.execute(binder.bind(r)).askResult();
    }
}
