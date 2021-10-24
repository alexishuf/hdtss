package com.github.lapesd.hdtss.data.load;

import io.micronaut.validation.Validated;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.rdfhdt.hdt.hdt.HDT;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.io.IOException;

@Validated
public interface HDTLoader {
    /**
     * Indicates whether this {@link HDTLoader} implementation is able to load the location type.
     *
     * A future call to {@link HDTLoader#load(String, boolean)} with the location may still
     * fail due to a transitory or persistent {@link IOException}. Implementations are not
     * expected to perform IO in order to validate whether a specific location is valid
     * or accessible.
     *
     * @param location An HDT file location (e.g., filesystem path or URI).
     * @return true iff this implementation can load HDT files whose location is of the given type.
     */
    boolean canLoad(@NotNull @NotEmpty String location);

    /**
     * Whether this loader uses the C++ implementation of HDT via JNI
     *
     * @return true iff the HDT file will be accessed using native code.
     */
    boolean isNative();

    /**
     * Whether the HDT file will be memory mapped instead of copied to RAM.
     *
     * @return true iff the HDT is memory-mapped
     */
    boolean isMemoryMapped();

    /**
     * Whether a sidecar .index.v1-1 file will be created if missing.
     *
     * @return true if .index.v1-1 sidecard index file can be created by the loader.
     */
    boolean canCreateIndex();

    /**
     * Each HDTLoader has a name that can be referenced with {@link jakarta.inject.Named}
     * annotations or from config properties (e.g., hdt.load.impl).
     *
     * @return a non-null and non-empty name. Should be equal to the
     *         {@link jakarta.inject.Named} annotation applied to the implementing class.
     */
    default @NonNull String name() {
        return getClass().getSimpleName().replaceAll("HDTLoader$", "");
    }

    /**
     * Create a queryable {@link HDT} object given the location of an HDT file.
     *
     * @param path a location (e.g., filesystem path or URI)
     * @param indexIfMissing if true and the implementation supports this, a sidecar
     *                       {@code .index.v1-1} file will be created if it does not already
     *                       exist. If {@link HDTLoader#canCreateIndex()} is false, the file is
     *                       loaded for querying anyways, without throwing an exception.
     * @return a non-null queryable HDT file
     * @throws IOException if something goes wrong when reading/validating the HDT file.
     */
    @NonNull HDT load(@NotNull @NotEmpty @NonNull String path,
                      boolean indexIfMissing) throws IOException;
}
