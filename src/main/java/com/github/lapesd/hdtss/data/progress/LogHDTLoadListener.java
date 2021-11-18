package com.github.lapesd.hdtss.data.progress;

import com.github.lapesd.hdtss.utils.LogUtils;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.event.Level;

import java.time.Duration;

import static java.lang.String.format;

@Slf4j
public class LogHDTLoadListener implements HDTLoadListener {
    private final String hdtLocation;
    private final Config config;
    private long lastLog = Long.MIN_VALUE;
    private long startNs = Long.MIN_VALUE;
    private final long periodMs;


    @ConfigurationProperties("hdt.load.progress.log")
    @Data
    public static class Config {
        private Level level = Level.INFO;
        private Duration period = Duration.ofSeconds(5);
        private boolean onStart = true, onEnd = true;
    }

    public LogHDTLoadListener(@NonNull String hdtLocation, @NonNull Config config) {
        this.hdtLocation = hdtLocation;
        this.config = config;
        this.periodMs = config.getPeriod().toMillis();
    }

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

    @Requires(property = "hdt.load.progress.type", value = "log", defaultValue = "log")
    public static class ListenerSupplier implements HDTLoadListenerSupplier {
        private final Config config;

        @Inject public ListenerSupplier(Config config) {
            this.config = config;
        }

        @Override public @NonNull HDTLoadListener listenerFor(@NonNull String hdtLocation) {
            return new LogHDTLoadListener(hdtLocation, config);
        }
    }
}
