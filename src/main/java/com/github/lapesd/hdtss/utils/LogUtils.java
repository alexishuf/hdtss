package com.github.lapesd.hdtss.utils;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.event.Level;

public class LogUtils {
    public static void log(@NonNull Logger logger, @NonNull Level level, @NonNull String msg, Object... args) {
        switch (level) {
            case ERROR -> logger.error(msg, args);
            case WARN  -> logger.warn (msg, args);
            case INFO  -> logger.info (msg, args);
            case DEBUG -> logger.debug(msg, args);
            case TRACE -> logger.trace(msg, args);
        }
    }
}
