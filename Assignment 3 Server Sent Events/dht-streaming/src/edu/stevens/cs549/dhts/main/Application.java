package edu.stevens.cs549.dhts.main;

import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.media.sse.SseFeature;

public class Application extends ResourceConfig {
    public Application() {
        packages("edu.stevens.cs549.dhts.resource")
        .register(ObjectMapperProvider.class)
        .register(JacksonFeature.class)
        // TODO register SseFeature @@
        .register(SseFeature.class);
    }
}
