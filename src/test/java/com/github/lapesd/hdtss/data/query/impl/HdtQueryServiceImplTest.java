package com.github.lapesd.hdtss.data.query.impl;

import com.github.lapesd.hdtss.TempFile;
import com.github.lapesd.hdtss.data.query.BaseHdtQueryServiceTest;
import com.google.common.collect.Lists;
import io.micronaut.context.ApplicationContext;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

@Tag("fast")
class HdtQueryServiceImplTest extends BaseHdtQueryServiceTest {

    private static TempFile hdtFile;
    private static final @NonNull Map<String, List<String>> propertyValues = Map.of(
            "hdt.load.indexed", asList("true", "false"),
            "hdt.load.native", asList("true", "false"),
            "hdt.load.mmap", asList("true", "false"),
            "hdt.load.can-create-index", asList("true", "false"),
            "hdt.load.progress.impl", asList("log", "none"),
            "hdt.load.progress.log.level", singletonList("DEBUG")
    );
    private static List<Map<String, Object>> configurations;

    @BeforeAll
    static void beforeAll() throws IOException {
        hdtFile = new TempFile(".hdt")
                .initFromResource(HdtQueryServiceImplTest.class, "../foaf-graph.hdt");
        record KV(String k, String v) {}
        var kvList = propertyValues.entrySet().stream()
                .flatMap(e -> e.getValue().stream().map(v -> new KV(e.getKey(), v)))
                .collect(Collectors.toList());
        configurations = Lists.cartesianProduct(kvList).stream().map(kvs -> {
            var map = kvs.stream().collect(Collectors.toMap(KV::k, kv -> (Object) kv.v()));
            map.put("hdt.location", hdtFile.getAbsolutePath());
            return map;
        }).collect(Collectors.toList());
    }

    @AfterAll
    static void afterAll() throws IOException {
        if (hdtFile != null)
            hdtFile.close();
        hdtFile = null;
    }

    @Override protected @NonNull List<Implementation> createImplementations() {
        return configurations.stream().map(props -> {
            String name = props.entrySet().stream()
                    .map(kv -> kv.getKey() + "=" + kv.getValue()).collect(Collectors.joining("\n"));
            var ctx = ApplicationContext.builder().properties(props).start();
            var svc = ctx.createBean(HdtQueryServiceImpl.class);
            return new BaseHdtQueryServiceTest.Implementation(name, svc, ctx);
        }).collect(Collectors.toList());
    }
}