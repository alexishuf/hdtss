package com.github.lapesd.hdtss.controller.execution;

import lombok.Setter;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.Accessors;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static java.lang.System.nanoTime;

@Value @Accessors(fluent = true)
public class QueryInfo {
    private static final AtomicLong nextId = new AtomicLong(1);

    long id;
    @ToString.Exclude @NonNull String sparql;
    long rows;
    @Nullable Throwable error;
    boolean cancelled;
    long parseNs;
    long optimizeNs;
    long dispatchNs;
    long totalNs;

    @Accessors(fluent = true, chain = true)
    public static class Builder {
        @Setter long start;
        @Setter @NonNull String sparql;
        @Setter long rows;
        @Setter @Nullable Throwable error;
        @Setter boolean cancelled;
        long parseNs;
        long optimizeNs;
        long dispatchNs;
        long totalNs = -1;

        public Builder(@NonNull String sparql) {
            this.start = nanoTime();
            this.sparql = sparql;
        }

        @SuppressWarnings("UnusedReturnValue")
        public @NonNull Builder addParseNs(long ns) {
            parseNs += ns;
            return this;
        }
        @SuppressWarnings("UnusedReturnValue")
        public @NonNull Builder addOptimizeNs(long ns) {
            optimizeNs += ns;
            return this;
        }
        @SuppressWarnings("UnusedReturnValue")
        public @NonNull Builder addDispatchNs(long ns) {
            dispatchNs += ns;
            return this;
        }

        public QueryInfo build() {
            if (totalNs == -1)
                totalNs = nanoTime()-start;
            return new QueryInfo(nextId.getAndIncrement(), sparql, rows, error, cancelled,
                                 parseNs, optimizeNs, dispatchNs, totalNs);
        }
    }

    public static Builder builder(@Nullable CharSequence sparql) {
        return new Builder(sparql == null ? "" : sparql.toString());
    }

    public double    parseMs() { return     parseNs/1000000.0; }
    public double optimizeMs() { return  optimizeNs/1000000.0; }
    public double dispatchMs() { return  dispatchNs/1000000.0; }
    public double    totalMs() { return     totalNs/1000000.0; }

    public @NonNull String indentedSparql(int spaces) {
        return sparql.replace("\n", "\n"+" ".repeat(Math.max(0, spaces)));
    }

    private static final Pattern SPACES = Pattern.compile("\\s+");

    @ToString.Include
    public @NonNull String shortSparql() { return SPACES.matcher(sparql).replaceAll(" "); }
}
