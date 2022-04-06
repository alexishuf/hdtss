package com.github.lapesd.hdtss.model.solutions;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.vocab.XSD;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Tag("fast")
class BatchQuerySolutionsTest extends BaseQuerySolutionsTest {
    @Override
    protected @NonNull QuerySolutions createFor(@NonNull List<@NonNull String> names,
                                                @NonNull List<Term[]> rows) {
        return new BatchQuerySolutions(names, rows);
    }

    @Test
    void testIsCold() {
        assertFalse(createFor(List.of(), List.of()).isHot());
        assertFalse(createFor(List.of(), singletonList(new Term[]{XSD.xtrue})).isHot());
        assertFalse(createFor(List.of("x"), singletonList(new Term[]{XSD.xtrue})).isHot());
    }
}