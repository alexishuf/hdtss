package com.github.lapesd.hdtss.data.load;

import com.github.lapesd.hdtss.data.progress.HDTProgressListenerSupplier;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;

import javax.validation.constraints.NotNull;
import java.io.IOException;

@Singleton
public class BatchHDTLoader implements HDTLoader {
    private final @NonNull HDTProgressListenerSupplier listenerSupplier;

    @Inject
    public BatchHDTLoader(@NonNull HDTProgressListenerSupplier listenerSupplier) {
        this.listenerSupplier = listenerSupplier;
    }

    @Override public boolean       isNative() {return false;}
    @Override public boolean isMemoryMapped() {return false;}
    @Override public boolean canCreateIndex() {return true;}

    @Override public boolean canLoad(@NotNull String location) {
        return HDTLoaderUtil.isFilePath(location);
    }

    @Override
    public @NonNull HDT load(@NotNull @NonNull String location, boolean indexIfMissing) throws IOException {
        var path = HDTLoaderUtil.toFilePath(location);
        if (indexIfMissing)
            return HDTManager.loadIndexedHDT(path, listenerSupplier.listenerFor(path));
        else
            return HDTManager.loadHDT(path, listenerSupplier.listenerFor(path));
    }
}
