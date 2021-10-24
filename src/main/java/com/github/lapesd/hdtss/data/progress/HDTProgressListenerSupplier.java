package com.github.lapesd.hdtss.data.progress;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.rdfhdt.hdt.listener.ProgressListener;

public interface HDTProgressListenerSupplier {
    /**
     * Create a {@link ProgressListener} that will receive progress updates when loading an
     * HDT file at given location.
     *
     * @param hdtLocation the HDT file location (can be a remote URI)
     * @return a non-null {@link ProgressListener} which should not be shared or reused on
     *         new load operations.
     */
    @NonNull ProgressListener listenerFor(@NonNull String hdtLocation);
}
