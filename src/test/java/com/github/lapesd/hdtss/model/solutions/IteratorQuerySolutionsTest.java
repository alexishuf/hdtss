package com.github.lapesd.hdtss.model.solutions;

import com.github.lapesd.hdtss.model.Term;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Tag;

import java.util.List;
import java.util.stream.Collectors;

@Tag("fast")
class IteratorQuerySolutionsTest extends BaseQuerySolutionsTest {
    @Override
    protected @NonNull QuerySolutions createFor(@NonNull List<@NonNull String> names,
                                                @NonNull List<Term[]> rows) {
        var wrapped = rows.stream().map(SolutionRow::new).collect(Collectors.toList());
        return new IteratorQuerySolutions(names, wrapped.iterator());
    }
}