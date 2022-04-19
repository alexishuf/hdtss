package com.github.lapesd.hdtss.controller.execution;


import com.github.lapesd.hdtss.utils.LogUtils;
import io.micronaut.context.annotation.Property;
import io.micronaut.scheduling.annotation.Scheduled;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.event.Level;

import static java.lang.String.format;

@Slf4j
public class HeartbeatQueryInfoConsumer implements QueryInfoConsumer {
    private final @NonNull Level level;
    private final @NonNull String tableFormat;
    private final @NonNull String periodString;

    public HeartbeatQueryInfoConsumer(@Property(name = "sparql.heartbeat.level", defaultValue = "INFO")
                                      @NonNull Level level,
                                      @Property(name = "sparql.endpoint.flow", defaultValue = "CHUNKED")
                                      @NonNull String flowType,
                                      @Property(name = "sparql.heartbeat.period", defaultValue = "1m")
                                      @NonNull String periodString) {
        this.level = level;
        this.periodString = periodString;
        String fmt = """
                Metrics updated for %d new queries
                                               |         Avg. ms (serialization timed)
                 #Queries | #Errors | #Cancels |  #rows  |  parse  | optimize | dispatch |  total
                ----------|---------|----------|---------|---------|----------|----------|--------
                 %8d | %7d | %8d | %7.2f | %7.3f | %8.3f | %8.3f | %7.3f""";
        if ("BATCH".equals(flowType))
            fmt = fmt.replaceAll("serialization timed", "serialization not timed");
        tableFormat = fmt;
    }

    private static final double NS_IN_MS = 1000000.0;

    private long nQueries = 0, lastNQueries = -1, nErrors, nCancels;
    private double avgQueryLen = 0;
    private double avgRows = 0, avgParse = 0, avgOptimize = 0, avgDispatch = 0, avgTotal = 0;


    @Scheduled(initialDelay = "${sparql.heartbeat.period:1m}",
            fixedRate = "${sparql.heartbeat.period:1m}")
    public void heartBeat() {
        if (lastNQueries == -1) {
            lastNQueries = 0;
            if (nQueries == 0) {
                LogUtils.log(log, level, "Heartbeat active period={}, will only beat after " +
                        "receiving new queries", periodString);
            }
        }
        if (lastNQueries != nQueries) {
            long newQueries = nQueries - lastNQueries;
            lastNQueries = nQueries;
            LogUtils.log(log, level, format(tableFormat, newQueries, nQueries, nErrors, nCancels,
                    avgRows, avgParse / NS_IN_MS, avgOptimize / NS_IN_MS,
                    avgDispatch / NS_IN_MS, avgTotal / NS_IN_MS));
        }
    }

    @Override public void accept(@NonNull QueryInfo info) {
        nQueries++;
        avgRows = (avgRows * (nQueries - 1) + info.rows()) / (double) nQueries;
        avgQueryLen = (avgQueryLen * (nQueries - 1) + info.sparql().length()) / (double) nQueries;
        avgParse = (avgParse * (nQueries - 1) + info.parseNs()) / (double) nQueries;
        avgOptimize = (avgOptimize * (nQueries - 1) + info.optimizeNs()) / (double) nQueries;
        avgDispatch = (avgDispatch * (nQueries - 1) + info.dispatchNs()) / (double) nQueries;
        avgTotal = (avgTotal * (nQueries - 1) + info.totalNs()) / (double) nQueries;
    }
}
