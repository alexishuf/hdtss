package com.github.lapesd.hdtss.sparql.impl.conditional;

import io.micronaut.context.annotation.Requires;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.annotation.*;

/**
 * Makes the bean activation conditional of the "sparql."+operator()+".flow" property
 * having one of the given values(), using the value of property "sparql.flow" if the
 * operator-specific property is not set or is an empty string (after trimming).
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PACKAGE, ElementType.TYPE})
@Requires(condition = OperatorFlowCondition.class)
public @interface RequiresOperatorFlow {
    /**
     * Values of sparql.operator().flow (or sparql.flow if the former is empty) that enable
     * this bean.
     *
     * @return an array of string values
     */
    @NonNull String[] values();

    /**
     * The operator name used to build the "sparql.operator().flow" property name.
     *
     * If omitted, will try to use the name given in @Named() to this bean.
     *
     * @return The operator name for building a sparql.*.flow property name.
     */
    @NonNull String operator() default "";
}
