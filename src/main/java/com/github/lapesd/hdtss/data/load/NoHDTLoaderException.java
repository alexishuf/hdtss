package com.github.lapesd.hdtss.data.load;

import org.checkerframework.checker.nullness.qual.NonNull;

public class NoHDTLoaderException extends RuntimeException {
    public NoHDTLoaderException(@NonNull String message) {
        super(message);
    }
}
