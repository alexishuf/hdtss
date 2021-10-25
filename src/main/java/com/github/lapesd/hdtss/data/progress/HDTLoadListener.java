package com.github.lapesd.hdtss.data.progress;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.rdfhdt.hdt.listener.ProgressListener;

public interface HDTLoadListener extends ProgressListener {
    /**
     * This is called right before the loading starts.
     *
     * There should be no previous calls to {@link ProgressListener#notifyProgress(float, String)}
     */
    void onStart();

    /**
     * This is called right after the loading ends.
     *
     * There should be no subsequent calls to {@link ProgressListener#notifyProgress(float, String)}
     * nor to {@link HDTLoadListener#onError(Throwable)}.
     */
    void onEnd();

    /**
     * This is called after loading is stopped due to an error.
     *
     * There should be no subsequent calls to {@link ProgressListener#notifyProgress(float, String)}
     * nor to {@link HDTLoadListener#onEnd()}.
     */
    void onError(@NonNull Throwable exception);
}
