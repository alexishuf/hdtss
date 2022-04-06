package com.github.lapesd.hdtss.model.solutions;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.vocab.XSD;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("fast")
class FluxQuerySolutionsTest extends BaseQuerySolutionsTest {
    @Override
    protected @NonNull QuerySolutions createFor(@NonNull List<@NonNull String> names,
                                                @NonNull List<Term[]> rows) {
        return new FluxQuerySolutions(names, Flux.fromStream(rows.stream()));
    }

    @Test
    void testIsHot() {
        assertTrue(createFor(List.of(), List.of()).isHot());
        assertTrue(createFor(List.of(), singletonList(new Term[]{XSD.xtrue})).isHot());
        assertTrue(createFor(List.of(), singletonList(new Term[]{XSD.xfalse})).isHot());
        assertTrue(createFor(List.of("x"), singletonList(new Term[]{XSD.xtrue})).isHot());
    }
}