package com.github.lapesd.hdtss.data.progress;

import io.micronaut.context.annotation.Requires;
import org.checkerframework.checker.nullness.qual.NonNull;

@Requires(property = "hdt.load.progress.impl", pattern = "(?i)none")
public class NoneHDTLoadListenerSupplier implements HDTLoadListenerSupplier {
    public static final @NonNull HDTLoadListener INSTANCE = new HDTLoadListener() {
        @Override public void onStart() {}
        @Override public void onEnd() {}
        @Override public void onError(@NonNull Throwable exception) {}
        @Override public void notifyProgress(float level, String message) {}
    };

    @Override public @NonNull HDTLoadListener listenerFor(@NonNull String hdtLocation) {
        return INSTANCE;
    }
}
