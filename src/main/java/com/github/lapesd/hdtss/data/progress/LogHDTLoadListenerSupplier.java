package com.github.lapesd.hdtss.data.progress;

import com.github.lapesd.hdtss.utils.LogUtils;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.event.Level;

import javax.validation.constraints.NotNull;
import java.time.Duration;

import static java.lang.String.format;

@Requires(property = "hdt.load.progress.type", value = "log", defaultValue = "log")
@Slf4j
public class LogHDTLoadListenerSupplier implements HDTLoadListenerSupplier {
    @ConfigurationProperties("hdt.load.progress.log")
    @Data
    public static class Config {
        private Level level = Level.INFO;
        private Duration period = Duration.ofSeconds(5);
        private boolean onStart = true, onEnd = true;
    }

    private final Config config;

    @Inject
    public LogHDTLoadListenerSupplier(@NotNull Config config) {
        this.config = config;
    }

    @Override public @NonNull HDTLoadListener listenerFor(@NonNull String hdtLocation) {
        long periodMs = config.getPeriod().toMillis();

        return new HDTLoadListener() {
            long lastLog = Long.MIN_VALUE;
            long startNs = Long.MIN_VALUE;

            private void log(@NonNull String msg, Object... args) {
                LogUtils.log(log, config.getLevel(), msg, args);
                lastLog = System.nanoTime();
            }

            @Override public void onStart() {
                if (config.isOnStart())
                    log("Starting load of HDT at {}", hdtLocation);
                startNs = System.nanoTime();
            }

            @Override public void onEnd() {
                double ms = (System.nanoTime() - startNs)/1000000.0;
                if (config.isOnEnd())
                    log("Loaded HDT at {} in {}ms", hdtLocation, format("%.3f", ms));
            }

            @Override public void onError(@NonNull Throwable exception) {
                double ms = (System.nanoTime() - startNs)/1000000.0;
                log("Load of HDT at {} failed after {}ms: {}", hdtLocation, format("%.3f", ms), exception);
            }

            @Override public void notifyProgress(float level, String message) {
                long now = System.nanoTime();
                if ((now - lastLog) / 1000000 > periodMs) {
                    log("Loaded {}% of HDT at {}{}", hdtLocation, format("%05.2f", level),
                            message != null && !message.isEmpty() ? ": "+ message : ".");
                }
            }
        };
    }
}
