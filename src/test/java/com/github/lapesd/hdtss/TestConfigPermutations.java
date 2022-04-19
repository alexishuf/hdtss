package com.github.lapesd.hdtss;

import io.micronaut.context.ApplicationContext;
import io.micronaut.http.MediaType;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

import static com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes.RESULTS_JSON;
import static com.github.lapesd.hdtss.sparql.results.SparqlMediaTypes.RESULTS_TSV;
import static java.util.Arrays.asList;

public class TestConfigPermutations implements Iterable<ApplicationContext>, Closeable {
    private static final @NonNull String FMT_PROP = "sparql.test.results.format";
    private final @NonNull TempFile tempFile;
    private boolean closed = false;

    public TestConfigPermutations(@NonNull Class<?> refClass,
                                  @NonNull String hdtResourcePath) throws IOException {
        tempFile = new TempFile(".hdt").initFromResource(refClass, hdtResourcePath);

    }

    private static @NonNull Map<String, List<Object>> createChoices(@NonNull String hdtLocation) {
        Map<String, List<Object>> choices = new HashMap<>();
        choices.put("hdt.location", List.of(hdtLocation));
        choices.put("sparql.flow", List.of("REACTIVE", "ITERATOR"));
        choices.put("hdt.estimator", List.of("NONE", "PATTERN", "PEEK"));
        choices.put("sparql.reactive.scheduler", asList("IO", "ELASTIC"));
        choices.put("sparql.reactive.max-threads", asList("-1", "5"));
        choices.put(FMT_PROP, List.of(RESULTS_TSV, RESULTS_JSON));
        return choices;
    }

    public @NonNull Map<String, List<Object>> choices() {
        return Map.of(
                "sparql.flow", List.of("REACTIVE", "ITERATOR"),
                "hdt.estimator", List.of("NONE", "PATTERN", "PEEK"),
                "sparql.reactive.scheduler", asList("IO", "ELASTIC"),
                "sparql.reactive.max-threads", asList("-1", "5"),
                FMT_PROP, List.of(RESULTS_TSV, RESULTS_JSON)
        );
    }

    private @NonNull Map<String, List<Object>> choicesWithPath() {
        HashMap<String, List<Object>> map = new HashMap<>(choices());
        map.put("hdt.location", List.of(tempFile.getAbsolutePath()));
        return map;
    }

    public static @NonNull MediaType resultsMediaType(@NonNull ApplicationContext applicationContext) {
        String resultsMT = applicationContext.get(FMT_PROP, String.class).orElseThrow();
        return new MediaType(resultsMT);
    }

    @Override public Iterator<ApplicationContext> iterator() {
        var it = TestUtils.listApplicationContext(choicesWithPath()).iterator();
        return new Iterator<>() {
            @Override public boolean hasNext() {
                return it.hasNext();
            }

            @Override public ApplicationContext next() {
                if (!hasNext()) throw new NoSuchElementException();
                if (closed)
                    throw new IllegalStateException("already close()ed");
                if (!tempFile.exists())
                    throw new IllegalStateException("tempFile deleted!");
                return it.next();
            }
        };
    }

    @Override public void close() throws IOException {
        if (!closed) {
            closed = true;
            tempFile.close();
        }
    }
}
