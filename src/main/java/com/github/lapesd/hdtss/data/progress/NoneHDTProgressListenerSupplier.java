package com.github.lapesd.hdtss.data.progress;

import io.micronaut.context.annotation.Requires;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.rdfhdt.hdt.listener.ProgressListener;

@Requires(property = "hdt.load.progress.impl", pattern = "(?i)none")
public class NoneHDTProgressListenerSupplier implements HDTProgressListenerSupplier {
    public static final @NonNull ProgressListener INSTANCE = (level, message) -> {
        /* do nothing */
    };

    @Override public @NonNull ProgressListener listenerFor(@NonNull String hdtLocation) {
        return INSTANCE;
    }
}
