package com.github.lapesd.hdtss;

import com.github.lapesd.hdtss.data.query.impl.HDTUtils;
import com.github.lapesd.hdtss.model.Row;
import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import com.google.common.collect.Lists;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.ApplicationContextBuilder;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.util.string.DelayedString;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class TestUtils {

    public static @NonNull InputStream openResource(@NonNull Class<?> cls, @NonNull String path) {
        InputStream is = cls.getResourceAsStream(path);
        assertNotNull(is, "Could not open resource "+path+" relative to "+cls);
        return is;
    }

    public static @NonNull HDT openHDTResource(@NonNull Class<?> cls,
                                               @NonNull String path) throws IOException {
        return HDTManager.loadIndexedHDT(openResource(cls, path), (l, m) -> {});
    }

    public static @Nullable Term fixEquals(@Nullable Term term) {
        if (term == null)
            return null;
        //hdt-java's DelayedString.equals is done by reference. This is a bug already fixed.
        //See https://github.com/rdfhdt/hdt-java/pull/117
        return term.sparql() instanceof DelayedString ? new Term(term.sparql().toString()) : term;
    }

    public static @Nullable Object fixEquals(@Nullable Object o) {
        if (o == null)
            return null;
        if (o instanceof List list)
            return fixEquals(list);
        if (o instanceof Set set)
            return fixEquals(set);
        if (o instanceof Row row)
            return fixEquals(row);
        if (o instanceof Term[] row)
            return fixEquals(row);
        if (o instanceof Term term)
            return fixEquals(term);
        if (o instanceof CharSequence seq)
            return seq instanceof DelayedString ? seq.toString() : seq;
        return o;
    }

    public static @NonNull Row fixEquals(@NonNull Row row) {
        return new Row(fixEquals(row.terms()));
    }

    public static @Nullable Term @NonNull[] fixEquals(@Nullable Term @NonNull[] row) {
        if (stream(row).filter(Objects::nonNull).map(Term::sparql).anyMatch(DelayedString.class::isInstance))
            return stream(row).map(TestUtils::fixEquals).toArray(Term[]::new);
        return row;
    }

    public static @NonNull Set<?> fixEquals(@NonNull Set<?> set) {
        return set.stream().map(TestUtils::fixEquals).collect(Collectors.toSet());
    }

    public static @NonNull List<?> fixEquals(@NonNull List<?> list) {
        return list.stream().map(TestUtils::fixEquals).collect(toList());
    }

    public static @NonNull TripleID parseTripleID(@NonNull String string) {
        String[] args = string.split(" ", 3);
        long s = Long.parseLong(args[0]);
        long p = Long.parseLong(args[1]);
        long o = Long.parseLong(args[2]);
        return new TripleID(s, p, o);
    }

    public static @NonNull TripleString parseTripleString(@NonNull String string) {
        TriplePattern tp = parseTriplePattern(string);
        CharSequence s = HDTUtils.toHDT(tp.subject());
        CharSequence p = HDTUtils.toHDT(tp.predicate());
        CharSequence o = HDTUtils.toHDT(tp.object());
        return new TripleString(s, p, o);
    }

    public static @NonNull TriplePattern parseTriplePattern(@NonNull String string) {
        String[] parts = string.split(" +", 3);
        assertEquals(3, parts.length, "Not enough Terms in triple!");
        Term s = new Term(parts[0]), p = new Term(parts[1]), o = new Term(parts[2]);
        assertTrue(s.isValid(), "Invalid subject: "+s);
        assertTrue(p.isValid(), "Invalid predicate: "+p);
        assertTrue(o.isValid(), "Invalid object: "+o);
        return new TriplePattern(parts[0],parts[1], parts[2]);
    }

    public static @NonNull List<Map<String, Object>>
    configPermutations(@NonNull Map<String, List<Object>> choicesMap) {
        return configPermutations(List.of(choicesMap));
    }

    public static @NonNull List<Map<String, Object>>
    configPermutations(@NonNull List<Map<String, List<Object>>> choicesMaps) {
        Map<String, List<Object>> unified = new HashMap<>();
        for (Map<String, List<Object>> map : choicesMaps) {
            for (Map.Entry<String, List<Object>> e : map.entrySet()) {
                var list = Stream.concat(
                        unified.computeIfAbsent(e.getKey(), k -> new ArrayList<>()).stream(),
                        e.getValue().stream()
                ).distinct().collect(toList());
                unified.put(e.getKey(), list);
            }
        }
        record KV(String k, Object v) {}

        List<Map<String, Object>> mapList = new ArrayList<>();
        List<List<KV>> lists = unified.entrySet().stream().map(e -> e.getValue().stream().map(v -> new KV(e.getKey(), v)).collect(toList())).collect(toList());
        for (List<KV> kvs : Lists.cartesianProduct(lists))
            mapList.add(kvs.stream().collect(toMap(KV::k, KV::v)));
        return mapList;
    }

    public static @NonNull Iterable<ApplicationContextBuilder>
    listApplicationContextBuilders(@NonNull Map<String, List<Object>> configChoicesMap) {
        return listApplicationContextBuilders(List.of(configChoicesMap));
    }

    public static @NonNull Iterable<ApplicationContextBuilder>
    listApplicationContextBuilders(@NonNull List<Map<String, List<Object>>> configChoicesMaps) {
        var it = configPermutations(configChoicesMaps).iterator();
        return () -> new Iterator<>() {
            @Override public boolean hasNext() {return it.hasNext();}
            @Override public ApplicationContextBuilder next() {
                var properties = it.next();
                log.info("micronaut properties map: {}", properties);
                return ApplicationContext.builder().environments("test").properties(properties);
            }
        };
    }

    public static @NonNull Iterable<ApplicationContext>
    listApplicationContext(@NonNull Map<String, List<Object>> configChoicesMap) {
        return listApplicationContext(List.of(configChoicesMap));

    }
    public static @NonNull Iterable<ApplicationContext>
    listApplicationContext(@NonNull List<Map<String, List<Object>>> configChoicesMaps) {
        var it = listApplicationContextBuilders(configChoicesMaps).iterator();
        return () -> new Iterator<>() {
            @Override public boolean hasNext() {return it.hasNext();}
            @Override public @NonNull ApplicationContext next() {return it.next().start();}
        };
    }
}
