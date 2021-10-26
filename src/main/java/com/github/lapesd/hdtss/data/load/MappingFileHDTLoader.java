package com.github.lapesd.hdtss.data.load;

import com.github.lapesd.hdtss.data.progress.HDTLoadListener;
import com.github.lapesd.hdtss.data.progress.HDTLoadListenerSupplier;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;

import javax.validation.constraints.NotNull;
import java.io.IOException;

@Singleton
public class MappingFileHDTLoader implements HDTLoader {
    private final @NotNull HDTLoadListenerSupplier listenerSupplier;

    @Inject
    public MappingFileHDTLoader(@NotNull HDTLoadListenerSupplier listenerSupplier) {
        this.listenerSupplier = listenerSupplier;
    }

    @Override public boolean       isNative() {return false;}
    @Override public boolean isMemoryMapped() {return true;}
    @Override public boolean canCreateIndex() {return true;}

    @Override public boolean canLoad(@NotNull String location) {
        return HDTLoaderUtil.isFilePath(location);
    }

    @Override public @NonNull HDT load(@NotNull @NonNull String location,
                                       boolean indexIfMissing) throws IOException {
        var path = HDTLoaderUtil.toFilePath(location);
        HDTLoadListener listener = listenerSupplier.listenerFor(path);
        listener.onStart();
        try {
            HDT hdt = indexIfMissing ? HDTManager.mapIndexedHDT(path, listener)
                    : HDTManager.mapHDT(path, listener);
            listener.onEnd();
            return hdt;
        } catch (Throwable t) {
            listener.onError(t);
            throw t;
        }
    }
}
