package com.github.lapesd.hdtss;

import com.github.lapesd.hdtss.controller.SparqlController;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.runtime.server.event.ServerStartupEvent;
import jakarta.inject.Singleton;
import lombok.Value;
import org.checkerframework.checker.nullness.qual.NonNull;

@Singleton @Value
public class LateInitializer implements ApplicationEventListener<ServerStartupEvent> {
    @NonNull SparqlController sparqlController;

    @Override public void onApplicationEvent(ServerStartupEvent ignored) {
        sparqlController.dispatcher().init();
    }
}
