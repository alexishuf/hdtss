package com.github.lapesd.hdtss.data.query;

import com.github.lapesd.hdtss.model.nodes.TriplePattern;

public interface CardinalityEstimator {

    /**
     * Estimate the number of solutions for the given triple.
     *
     * If the triple has no variables, the estimate will be 1 or 0, but implementations are not
     * required to return correct values (one could always return 1 even if the triple is not
     * present).
     *
     * With variables, an estimator can estimate any non-negative integer. Again, estimates are
     * not expected to be correct.
     *
     * @param triple teh triple to estimate
     * @return the estimate number of solutions for the variables in the triple.
     */
    long estimate(TriplePattern triple);
}
