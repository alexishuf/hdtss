package com.github.lapesd.hdtss.data.progress;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.slf4j.event.Level;

import javax.validation.constraints.NotNull;
import java.time.Duration;

@Requires(property = "hdt.load.progress.type", value = "log", defaultValue = "log")
@Slf4j
public class LogHDTProgressListenerSupplier implements HDTProgressListenerSupplier {
    @ConfigurationProperties("hdt.load.progress.log")
    @Data
    public static class Config {
        private Level level = Level.INFO;
        private Duration period = Duration.ofSeconds(5);
        private boolean onStart = true, onEnd = true;
    }

    private final Config config;

    @Inject
    public LogHDTProgressListenerSupplier(@NotNull Config config) {
        this.config = config;
    }

    @SuppressWarnings("SameParameterValue")
    protected void doLog(@NonNull String msg, Object... args) {
        switch (config.getLevel()) {
            case ERROR -> log.error(msg, args);
            case WARN  -> log.warn (msg, args);
            case INFO  -> log.info (msg, args);
            case DEBUG -> log.debug(msg, args);
            case TRACE -> log.trace(msg, args);
        }
    }

    @Override public @NonNull ProgressListener listenerFor(@NonNull String hdtLocation) {
        long periodMs = config.getPeriod().toMillis();
        boolean onStart = config.isOnStart(), onEnd = config.isOnEnd();

        return new ProgressListener() {
            long lastLog = Long.MIN_VALUE;
            boolean startDone = false, endDone = false;
            @Override public void notifyProgress(float level, String message) {
                if (onStart && !startDone) {
                    startDone = true;
                    log(level, message);
                } else if (!endDone) {
                    if (onEnd && level >= 99.999999) {
                        endDone = true;
                        log(100, message);
                    } else {
                        long now = System.nanoTime();
                        if ((now - lastLog) / 1000000 > periodMs) {
                            lastLog = now;
                            log(level, message);
                        }
                    }
                }
            }

            private void log(float level, String msg) {
                doLog("Loaded {}% of HDT at {}{}",
                        hdtLocation,
                        String.format("%05.2f", level),
                        msg != null && !msg.isEmpty() ? ": "+msg : ".");
            }
        };
    }
}
