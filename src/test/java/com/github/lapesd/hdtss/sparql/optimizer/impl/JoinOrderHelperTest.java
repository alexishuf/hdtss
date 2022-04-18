package com.github.lapesd.hdtss.sparql.optimizer.impl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("fast")
class JoinOrderHelperTest {
    @ParameterizedTest @ValueSource(strings = {
            "true  :: ",
            "true  :: 0",
            "false :: 1",
            "true  :: 0, 1",
            "false :: 1, 0",
            "true  :: 0, 1, 2",
            "false :: 0, 2, 1",
    })
    void testIsNoOp(String data) {
        String[] parts = data.split(" +:: +");
        boolean expected = Boolean.parseBoolean(parts[0]);
        long[] input = Arrays.stream((parts.length > 1 ? parts[1] : "").split(" *, *"))
                .filter(s -> !s.isBlank())
                .mapToLong(Integer::parseInt).toArray();
        assertEquals(expected, JoinOrderHelper.isNoOp(input));
    }

}