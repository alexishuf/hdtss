package com.github.lapesd.hdtss.utils;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.nodes.Op;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BindUtils {
    public static @NonNull Op bindWithMap(@NonNull Op op, @NonNull List<String> varNames, Term @NonNull[] row,
                            Op.@NonNull BindType bindType) {
        if (row.length != varNames.size())
            throw new IllegalArgumentException("varNames.size() != row.length");
        else if (row.length == 0)
            return op;
        Map<String, Term> v2t = new HashMap<>();
        for (int i = 0; i < row.length; i++) v2t.put(varNames.get(i), row[i]);
        return op.bind(v2t, bindType);
    }
}
