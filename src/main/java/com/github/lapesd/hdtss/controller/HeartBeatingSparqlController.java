package com.github.lapesd.hdtss.controller;

import com.github.lapesd.hdtss.utils.LogUtils;
import io.micronaut.context.annotation.Value;
import io.micronaut.scheduling.annotation.Scheduled;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.event.Level;

@Slf4j
public abstract class HeartBeatingSparqlController implements SparqlController {
    private long nQueries = 0;
    private double avgQueryLen = 0;

    @Value("${sparql.heartbeat.level:INFO}")
    private @Getter @Setter Level heartBeatLevel;

    protected void logQuery(@Nullable String query) {
        if (query != null && !query.isEmpty()) {
            nQueries++;
            avgQueryLen = (avgQueryLen * (nQueries - 1) + query.length()) / (double) nQueries;
        }
    }

    @Scheduled(fixedRate = "${sparql.heartbeat.period:30s}")
    public void heartBeat() {
        final String msg = "Received {} queries with an average length of {} chars";
        LogUtils.log(log, heartBeatLevel, msg, nQueries, avgQueryLen);
    }
}
