package com.github.lapesd.hdtss;

import io.micronaut.context.ApplicationContext;
import io.micronaut.http.MediaType;
import lombok.Getter;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes.RESULTS_JSON;
import static com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes.RESULTS_TSV;
import static java.util.Arrays.asList;

public class TestConfigPermutations implements Iterable<ApplicationContext>, Closeable {
    private final @Getter Map<String, List<Object>> choices;
    private @Nullable final TempFile tempFile;
    private static final @NonNull String FMT_PROP = "sparql.test.results.format";

    public TestConfigPermutations(@NonNull Class<?> refClass,
                                  @NonNull String hdtResourcePath) throws IOException {
        tempFile = new TempFile(".hdt").initFromResource(refClass, hdtResourcePath);
        choices = createChoices(tempFile.getAbsolutePath());
    }

    private static @NonNull Map<String, List<Object>> createChoices(@NonNull String hdtLocation) {
        Map<String, List<Object>> choices = new HashMap<>();
        choices.put("hdt.location", List.of(hdtLocation));
        choices.put("sparql.flow", List.of("REACTIVE", "ITERATOR", "HEAVY_REACTIVE"));
        choices.put("sparql.join.reorder", List.of("NONE", "VARS_POS"));
        choices.put("sparql.reactive.scheduler", asList("IO", "ELASTIC"));
        choices.put("sparql.reactive.max-threads", asList("-1", "5"));
        choices.put(FMT_PROP, List.of(RESULTS_TSV, RESULTS_JSON));
        return choices;
    }

    public static @NonNull MediaType resultsMediaType(@NonNull ApplicationContext applicationContext) {
        String resultsMT = applicationContext.get(FMT_PROP, String.class).orElseThrow();
        return new MediaType(resultsMT);
    }

    @Override public Iterator<ApplicationContext> iterator() {
        return TestUtils.listApplicationContext(choices).iterator();
    }

    @Override public void close() throws IOException {
        if (tempFile != null)
            tempFile.close();
    }
}
