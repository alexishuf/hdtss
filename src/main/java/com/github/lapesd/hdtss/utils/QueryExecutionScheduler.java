package com.github.lapesd.hdtss.utils;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.scheduling.TaskExecutors;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.checkerframework.checker.nullness.qual.NonNull;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ExecutorService;

@Factory
public class QueryExecutionScheduler {
    private static final String REACTOR_QUEUE_PROPERTY = "reactor.schedulers.defaultBoundedElasticQueueSize";

    public static final @NonNull String NAME = "queryExecution";

    @Bean @Singleton @Named(NAME)
    @Requires(property = "sparql.reactive.scheduler", value = "IO", defaultValue = "IO")
    @NonNull Scheduler get(@Named(TaskExecutors.IO) @NonNull ExecutorService executorService) {
        return Schedulers.fromExecutorService(executorService, "IO-QueryExecutionScheduler");
    }

    @Bean @Singleton @Named(NAME)
    @Requires(property = "sparql.reactive.scheduler", value = "ELASTIC")
    @NonNull Scheduler get(
            @Property(name = "sparql.reactive.max-threads", defaultValue = "-1")
            int threadCap,
            @Property(name = "sparql.reactive.max-queue", defaultValue = "-1")
            int maxQueued,
            @NonNull ApplicationContext ctx
    ) {
        if (threadCap < 0)
            threadCap = 10 * Runtime.getRuntime().availableProcessors();
        if (maxQueued < 0)
            maxQueued = ctx.getProperty(REACTOR_QUEUE_PROPERTY, Integer.class).orElse(1000000);
        return Schedulers.newBoundedElastic(threadCap, maxQueued, "QueryExecutionScheduler");
    }
}
