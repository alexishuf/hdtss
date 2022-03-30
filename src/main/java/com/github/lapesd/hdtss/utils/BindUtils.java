package com.github.lapesd.hdtss.utils;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Op;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BindUtils {
    public static @NonNull Op bindWithMap(@NonNull Op op, @NonNull List<String> varNames,
                                          Term @NonNull[] row) {
        if (row.length != varNames.size())
            throw new IllegalArgumentException("varNames.size() != row.length");
        else if (row.length == 0)
            return op;
        Map<String, Term> var2term = new HashMap<>();
        for (int i = 0; i < row.length; i++) var2term.put(varNames.get(i), row[i]);
        return op.bind(var2term);
    }


}
