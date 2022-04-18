package com.github.lapesd.hdtss.data.query.impl;

import com.github.lapesd.hdtss.TestUtils;
import com.github.lapesd.hdtss.model.nodes.TriplePattern;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("fast")
class PatternCardinalityEstimatorTest {
    @ParameterizedTest @ValueSource(strings = {
            "1      :: <a> <b> <c>",
            "10     :: <a> ?x  <c>",
            "20     :: <a> <b> ?x",
            "10     :: <a> ?x  <c>",
            "100    :: <a> ?y  ?z",
            "1000   :: ?x  <b> <c>",
            "2000   :: ?x  ?y  <c>",
            "10000  :: ?x  <b> ?z",
            "100000 :: ?x  ?y  ?z",

    })
    void testEstimate(String data) {
        String[] parts = data.split(" +:: +");
        assertEquals(2, parts.length);
        int expected = Integer.parseInt(parts[0]);
        TriplePattern tp = TestUtils.parseTriplePattern(parts[1]);
        assertEquals(expected, new PatternCardinalityEstimator().estimate(tp));
    }
}