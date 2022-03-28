package com.github.lapesd.hdtss;

import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static com.github.lapesd.hdtss.TestUtils.openResource;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

@Slf4j
public class TempFile extends File implements Closeable {
    public static @NonNull File create(@Nullable File dir, @Nullable String prefix,
                                        @Nullable String suffix) throws IOException {
        prefix = prefix == null ? "hdt-server-" : prefix;
        suffix = suffix == null ? "" : suffix;
        if (dir != null)
            return Files.createTempFile(dir.toPath(), prefix, suffix).toFile();
        return Files.createTempFile(prefix, suffix).toFile();
    }

    public TempFile(@NonNull String suffix) throws IOException {
        this(null, "", suffix);
    }

    public TempFile(@Nullable File parentDir, @NonNull String suffix) throws IOException {
        this(parentDir, "", suffix);
    }

    public TempFile(@Nullable File parentDir, @NonNull String prefix,
                    @NonNull String suffix) throws IOException {
        super(create(parentDir, prefix, suffix).getAbsolutePath());
        deleteOnExit();
    }

    public @NonNull TempFile initFromResource(@NonNull Class<?> cls,
                                              @NonNull String resourcePath) throws IOException {
        Files.copy(openResource(cls, resourcePath), toPath(), REPLACE_EXISTING);
        return this;
    }

    @Override public void close() throws IOException {
        if (exists() && !delete())
            throw new IOException("failed to delete temp file "+this);
    }
}
