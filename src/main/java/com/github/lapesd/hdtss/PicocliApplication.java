package com.github.lapesd.hdtss;

import com.github.lapesd.hdtss.model.FlowType;
import io.micronaut.http.server.HttpServerConfiguration;
import io.micronaut.runtime.Micronaut;
import org.checkerframework.checker.nullness.qual.NonNull;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static java.util.Arrays.asList;

@Command(name = "hdtss", mixinStandardHelpOptions = true,
         description = "Expose a SPARQL endpoint over HTTP for an HDT file")
public class PicocliApplication implements Callable<Void> {
    @Option(names = {"--port", "-p"}, description = "Override the port where the HTTP  server " +
            "will listen. If omitted and micronaut.server.port is not set elsewhere, defaults " +
            "to "+HttpServerConfiguration.DEFAULT_PORT+".")
    Integer port = null;

    @Option(names = {"-i", "--hdt-indexed"}, negatable = true, description = "Whether HDT files " +
            "should be opened in indexed mode. This will lead to faster querying, but may incur " +
            "a startup delay if the .index.v1-1 sidecar does not yet exist for the given HDT " +
            "file. If omitted and the property hdt.load.indexed is not set elsewhere, " +
            "defaults to true.")
    Boolean indexedHDT = null;

    @Option(names = {"--hdt-native"}, negatable = true, description = "Whether native " +
            "(C++) implementations of HDT will be preferred if available over the java " +
            "implementation. If omitted and the property hdt.load.native is not set elsewhere, " +
            "defaults to true.")
    Boolean nativeHDT = null;

    @Option(names = {"--hdt-mmap"}, negatable = true, description = "Prefer to memory-map " +
            "HDT files instead of copying their contents to main-memory. If omitted and the" +
            " property hdt.load.mmap is not set elsewhere, defaults to true.")
    Boolean mmapHDT = null;

    @Option(names = {"--hdt-log-load"}, negatable = true, description = "Log the progress of " +
            "loading HDT files. If omitted and the property hdt.load.progress.impl is not set" +
            " elsewhere, defaults to true.")
    Boolean logHDTLoad = null;

    @Option(names = {"--hdt-log-load-seconds"}, description = "How many seconds to wait between " +
            "log messages of HDT load progress. If omitted and the property " +
            "hdt.load.progress.log.period is not set elsewhere, defaults to 5 (seconds)")
    Boolean logHDTLoadInterval = null;

    enum SchedulerName {
        IO,
        ELASTIC
    }
    @Option(names = {"-s", "--scheduler"}, description = "When processing queries in reactive " +
            "mode, offload processing to the given scheduler. IO is the micronaut default IO " +
            "scheduler, whereas ELASTIC will create a Project reactor boundedElastic() " +
            "Scheduler dedicated to query processing. If omitted and the property " +
            "sparql.reactive.scheduler is not set elsewhere, defaults to IO")
    SchedulerName scheduler = null;

    @Option(names = "--elastic-threads", description = "If --scheduler is ELASTIC, this will " +
            "override the default maximum number of threads in the boundedElastic scheduler. " +
            "If a negative value is given or omitted and the property sparql.reactive.max-threads " +
            "is not set elsewhere, the default is determined by Project Reactor (10 times the " +
            "number of available processors (real and HT cores).")
    Integer elasticMaxThreads = null;

    @Option(names = "--chunked", negatable = true, description = "Whether the HTTP endpoint " +
            "should answer with Transfer-Encoding: chunked. This allows results to be sent " +
            "immediately, without awaiting full enumeration of query solutions. For queries with " +
            "many wolutions this saves memory on the server side and dramatically reduces " +
            "latency for clients able to incrementally parse the SPARQL result serializations. " +
            "Incremental parsing should be preferred over LIMIT/OFFSET pagination. If omitted and " +
            "sparql.endpoint.flow is not set elsewhere, defaults to true.")
    Boolean chunked = true;

    enum ReactiveOperators {
        ALL,
        MINIMAL,
        NONE;

        private static final @NonNull List<String> ALL_NAMES = asList(
                "hdt", "filter", "join", "union", "distinct", "project",
                "values", "limit", "offset", "assign", "exists", "minus");
        private static final @NonNull List<String> MINIMAL_NAMES =
                asList("hdt", "filter", "assign", "exists", "minus");

        @NonNull List<@NonNull String> operatorNames() {
            return switch (this) {
                case NONE, ALL -> ALL_NAMES;
                case MINIMAL -> MINIMAL_NAMES;
            };
        }
        @NonNull FlowType flowType() {
            return this == NONE ? FlowType.ITERATOR : FlowType.REACTIVE;
        }
    }

    @Option(names = {"-r", "--reactive-ops"}, description = "Choose one of three set of " +
            "intermediary operators to be implemented using reactive streams. With ALL, all " +
            "operators will produce and consume reactive streams. With MINIMAL, only operators " +
            "that perform heavier processing, rather than just passing solutions (e.g., UNION, " +
            "PROJECT, LIMIT and OFFSET) will be reactive (the others being plain Iterators). " +
            "With NONE, all intermediary operators will produce and consume plain Iterators. " +
            "Note that with --chunked, there will always be at least one reactive stream wrapping" +
            "everything. If omitted, sparql.*.flow properties, if set, control the flow type of " +
            "each operator, each of these properties having its own default value.")
    ReactiveOperators reactiveOperators = null;

    enum Join {
        BIND
    }

    @Option(names = "--join", description = "Implementation strategy for joins and OPTIONALs. " +
            "BIND consists in serially binding the right-hand operand with the solutions " +
            "obtained from the left operand. If omitted and sparql.join.strategy is not set " +
            "elsewhere, defaults to BIND")
    Join join = null;

    enum JoinReorder {
        NONE,
        HEURISTIC
    }
    @Option(names = "--join-reorder", description = "Which strategy to use when reordering join " +
            "operands for optimizing performance. NONE leaves operands as given in the query. " +
            "HEURISTIC uses general RDF-tailored heuristics blind to the actual query vocabulary " +
            "and HDT contents. If omitted and it sparql.join.reorder is not set elsewhere, " +
            "defaults to NONE")
    JoinReorder joinReorder = null;

    enum Distinct {
        HASH,
        WINDOW
    }
    @Option(names = "--distinct", description = "With HASH, the DISTINCT modifier is implemented " +
            "with a HashSet of projected solutions. With WINDOW, only the last W projected " +
            "solutions are kept in the HashSet. WINDOW is not conformat to the SPARQL standard " +
            "but may be a way out of untractable distinct sets. If omitted and " +
            "sparql.distinct.strategy is not set elsewhere, defaults to HASH.")
    Distinct distinct = null;

    @Option(names = "--distinct-window", description = "When --distinct=WINDOW, this sets the N " +
            "last projected solutions to be kept in memory. If omitted and sparql.distinct.window " +
            "is not set elsewhere, defaults to 8192")
    Integer distinctWindow = null;

    @Parameters(paramLabel = "HDT_FILE", arity = "1",
                description = "Path to the HDT file to be exposed in the SPARQL endpoint")
    File hdtFile;

    public static void main(String[] args) {
        System.exit(new CommandLine(new PicocliApplication()).execute(args));
    }

    @Override public Void call() throws Exception {
        List<String> args = new ArrayList<>();
        if (port != null)
            args.add("-port="+ port);
        if (indexedHDT != null)
            args.add("-hdt.load.indexed="+indexedHDT);
        if (nativeHDT != null)
            args.add("-hdt.load.native="+nativeHDT);
        if (mmapHDT != null)
            args.add("-hdt.load.mmap="+mmapHDT);
        if (logHDTLoad != null)
            args.add("-hdt.load.progress.impl="+(logHDTLoad ? "log" : "none"));
        if (logHDTLoadInterval != null)
            args.add("-hdt.load.progress.log.period="+logHDTLoadInterval+"s");
        if (scheduler != null)
            args.add("-sparql.reactive.scheduler="+scheduler.name());
        if (elasticMaxThreads != null)
            args.add("-sparql.reactive.max-threads="+elasticMaxThreads);
        if (chunked != null)
            args.add("-sparql.endpoint.flow="+(chunked ? "CHUNKED" : "BATCH"));
        if (reactiveOperators != null) {
            for (String name : reactiveOperators.operatorNames())
                args.add("-sparql."+name+".flow="+reactiveOperators.flowType());
        }
        if (join != null)
            args.add("sparql.join.strategy="+join);
        if (joinReorder != null)
            args.add("-sparql.join.reorder="+joinReorder);
        if (distinct != null)
            args.add("-sparql.distinct.strategy="+distinct);
        if (distinctWindow != null)
            args.add("-sparql.distinct.window="+distinctWindow);
        checkHdtFile();
        args.add("-hdt.location="+hdtFile.getAbsolutePath());

        String[] argsArray = args.toArray(String[]::new);
        Micronaut.run(PicocliApplication.class, argsArray);
        return null;
    }

    private void checkHdtFile() throws IOException {
        if (!hdtFile.exists())
            throw new IOException("HDT File "+hdtFile+" does not exist");
        if (!hdtFile.isFile())
            throw new IOException("HDT File "+hdtFile+" is not a file");
        if (!hdtFile.canRead())
            throw new IOException("No read permission for HDT File "+hdtFile);
        if (hdtFile.length() == 0)
            throw new IOException("HDT File "+hdtFile+" is an empty file, expected at least metadata");
    }
}
