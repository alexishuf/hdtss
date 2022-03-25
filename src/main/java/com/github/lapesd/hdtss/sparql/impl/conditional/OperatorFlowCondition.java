package com.github.lapesd.hdtss.sparql.impl.conditional;

import io.micronaut.context.condition.Condition;
import io.micronaut.context.condition.ConditionContext;
import io.micronaut.core.annotation.AnnotationMetadata;
import io.micronaut.core.annotation.AnnotationUtil;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.value.PropertyResolver;

import java.util.Arrays;

public class OperatorFlowCondition implements Condition {
    @Override public boolean matches(ConditionContext context) {
        if (!(context.getBeanContext() instanceof PropertyResolver pr)) {
            context.fail("context.getBeanContext() is not a PropertyResolver");
            return false;
        }
        AnnotationMetadata annotationMetadata = context.getComponent().getAnnotationMetadata();
        var values = annotationMetadata.getAnnotationValuesByType(RequiresOperatorFlow.class);
        for (AnnotationValue<RequiresOperatorFlow> aValue : values) {
            String property = getOperatorPropertyName(aValue, annotationMetadata);
            String actual = readOperatorProperty(pr, property);
            String[] expectedValues = aValue.stringValues("values");
            boolean ok = false;
            for (int i = 0; !ok && i < expectedValues.length; i++)
                ok = actual.trim().equalsIgnoreCase(expectedValues[i].trim());
            if (!ok) {
                context.fail("Expected one of "+ Arrays.toString(expectedValues)+
                             " for property "+property+" (falling back to sparql.flow), got "+
                             actual);
                return false;
            }
        }
        return true;
    }

    private @NonNull String
    getOperatorPropertyName(@NonNull AnnotationValue<RequiresOperatorFlow> aValue,
                            @NonNull AnnotationMetadata annotations) {
        String op = aValue.stringValue("operator").orElse("").trim();
        if (op.isEmpty()) {
            for (var annName : annotations.getAnnotationNamesByStereotype(AnnotationUtil.NAMED)) {
                if (!(op = annotations.getValue(annName, String.class).orElse("")).isEmpty())
                    break;
            }
        }
        if (op.isEmpty()) {
            throw new IllegalArgumentException("Bean annotated with RequiresOperatorFlow has " +
                    "no operator set nor a @Named annotation.");
        }
        return "sparql." + op + ".flow";
    }

    private @NonNull String
    readOperatorProperty(@NonNull PropertyResolver resolver, @NonNull String property) {
        String value = resolver.get(property, String.class).orElse("");
        if (value.isEmpty())
            value = resolver.getProperty("sparql.flow", String.class).orElse("");
        if (value.isEmpty())
            value = "ITERATOR";
        return value;
    }
}
