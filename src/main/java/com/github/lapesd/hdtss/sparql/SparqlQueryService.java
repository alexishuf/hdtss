package com.github.lapesd.hdtss.sparql;


import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

public interface SparqlQueryService {
    /**
     * Answer an SPARQL ASK or SELECT query.
     *
     * @param sparqlQuery the SPARQL 1.1 query to execute.
     * @return A {@link QuerySolutions} instance. Subclasses should declare a specific
     *         implementation (e.g., reactive or batch) of {@link QuerySolutions} when
     *         implementing this method.
     *
     * @throws FeatureNotSupportedException if some feature used in the query is not
 *             implemented by this SparqlQueryService
     */
    @NonNull QuerySolutions query(@NotNull @NotBlank CharSequence sparqlQuery)
            throws FeatureNotSupportedException;
}
