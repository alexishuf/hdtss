package com.github.lapesd.hdtss.controller;

import com.github.lapesd.hdtss.utils.LogUtils;
import io.micronaut.context.annotation.Value;
import io.micronaut.scheduling.annotation.Scheduled;
import lombok.Getter;
import lombok.Setter;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;


public abstract class HeartBeatingSparqlController implements SparqlController {
    private static final AtomicLong nextQueryId = new AtomicLong(1);
    private static final double NS_IN_MS = 1000000.0;

    private long nQueries = 0, lastNQueries = -1, nErrors, nCancels;
    private double avgQueryLen = 0;
    private double avgRows = 0, avgParse = 0, avgOptimize = 0, avgDispatch = 0, avgTotal = 0;

    protected abstract @NonNull Logger log();

    @Value("${sparql.heartbeat.level:INFO}")
    private @Getter @Setter Level heartBeatLevel;

    protected abstract boolean isSerializationTimed();

    protected QueryInfo.QueryInfoBuilder start(String sparql) {
        long id = nextQueryId.getAndIncrement();
        Logger log = log();
        if (log.isDebugEnabled()) {
            log.debug("Starting query {}, minified sparql={}", id, sparql.replaceAll("\\s+", " "));
        }
        return QueryInfo.builder().id(id).sparql(sparql);
    }

    protected synchronized void log(@NonNull QueryInfo info) {
        nQueries++;
        Logger log = log();
        if (info.error() != null) {
            log.error("Query {}: failed with {} after {}ms and {} rows, parse={}ms, " +
                      "dispatch={}ms, optimize={}ms, sparql=\n{}", info.id(),
                      requireNonNull(info.error()).getClass().getSimpleName(), info.totalMs(),
                      info.rows(), info.parseMs(), info.dispatchMs(), info.optimizeMs(),
                      info.indentedSparql(4));
            nErrors++;
        } else if (info.cancelled()) {
            log.info("Query {}: cancelled after {}ms and {} rows, parse={}ms, dispatch={}ms, " +
                     "optimize={}ms, sparql=\n{}", info.id(), info.totalMs(), info.rows(),
                     info.parseMs(), info.dispatchMs(), info.optimizeMs(),
                     info.indentedSparql(4));
            nCancels++;
        } else if (log.isDebugEnabled()) {
            log.debug("Query {}: completed {} rows in {}ms and, parse={}ms, dispatch={}ms, " +
                      "optimize={}ms, sparql=\n{}", info.id(), info.rows(),
                      info.totalMs(), info.parseMs(), info.dispatchMs(), info.optimizeMs(),
                      info.indentedSparql(4));
        }
        avgRows     = (avgRows     * (nQueries-1) + info.rows()           ) / (double)nQueries;
        avgQueryLen = (avgQueryLen * (nQueries-1) + info.sparql().length()) / (double)nQueries;
        avgParse    = (avgParse    * (nQueries-1) + info.parseNs()        ) / (double)nQueries;
        avgOptimize = (avgOptimize * (nQueries-1) + info.optimizeNs()     ) / (double)nQueries;
        avgDispatch = (avgDispatch * (nQueries-1) + info.dispatchNs()     ) / (double)nQueries;
        avgTotal    = (avgTotal    * (nQueries-1) + info.totalNs()        ) / (double)nQueries;
    }

    @Scheduled(initialDelay = "${sparql.heartbeat.period:1m}",
               fixedRate = "${sparql.heartbeat.period:1m}")
    public void heartBeat() {
        if (lastNQueries == -1) {
            lastNQueries = 0;
            if (nQueries == 0) {
                LogUtils.log(log(), heartBeatLevel,
                            "Heartbeat active, will only show for new queries");
            }
        }
        if (lastNQueries != nQueries) {
            long newQueries = nQueries - lastNQueries;
            lastNQueries = nQueries;
            String not = isSerializationTimed() ? "" : " not";
            String msg = String.format("""
                            Metrics updated for %d new queries
                                                           |         Avg. ms (serialization%s timed)
                             #Queries | #Errors | #Cancels |  #rows  |  parse  | optimize | dispatch |  total
                            ----------|---------|----------|---------|---------|----------|----------|--------
                             %8d | %7d | %8d | %7.2f | %7.3f | %8.3f | %8.3f | %7.3f""",
                    newQueries, not, nQueries, nErrors, nCancels, avgRows, avgParse/NS_IN_MS,
                    avgOptimize/NS_IN_MS, avgDispatch/NS_IN_MS, avgTotal/NS_IN_MS);
            LogUtils.log(log(), heartBeatLevel, msg);
        }
    }
}
