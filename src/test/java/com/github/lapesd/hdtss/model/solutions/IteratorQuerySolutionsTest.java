package com.github.lapesd.hdtss.model.solutions;

import com.github.lapesd.hdtss.model.Term;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;

import java.util.List;

@Tag("fast")
class IteratorQuerySolutionsTest extends BaseQuerySolutionsTest {
    @Override
    protected @NonNull QuerySolutions createFor(@NonNull List<@NonNull String> names,
                                                @NonNull List<Term[]> rows) {
        return new IteratorQuerySolutions(names, rows.iterator());
    }
}