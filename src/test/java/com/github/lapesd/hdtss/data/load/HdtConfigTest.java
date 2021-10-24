package com.github.lapesd.hdtss.data.load;

import com.github.lapesd.hdtss.TempFile;
import io.micronaut.context.ApplicationContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HdtConfigTest {

    private static TempFile hdtFile;

    @BeforeAll
    static void beforeAll() throws IOException {
        String hdtPath = "data/query/foaf-graph.hdt";
        hdtFile = new TempFile(".hdt").initFromResource(TempFile.class, hdtPath);
    }

    @AfterAll
    static void afterAll() throws IOException {
        if (hdtFile != null)
            hdtFile.close();
    }

    @Test
    void testParseConfig() {
        String path = hdtFile.getAbsolutePath();
        Map<String, Object> properties = Map.of(
                "hdt.location", path,
                "hdt.load.native", "false",
                "hdt.load.mmap", "true",
                "hdt.load.can-create-index", "false",
                "hdt.load.require-all", "true");
        HdtConfig cfg = ApplicationContext.builder().packages("com.github.lapesd.hdtss.data.load")
                                                    .environments("test")
                                                    .properties(properties).start()
                                                    .createBean(HdtConfig.class);
        assertEquals(path, cfg.getLocation());                // no default
        assertTrue (cfg.getLoadOptions().isIndexed());        // default value
        assertTrue (cfg.getLoadOptions().isMmap());           // set to the default
        assertFalse(cfg.getLoadOptions().isNative());         // overridden
        assertTrue (cfg.getLoadOptions().isRequireAll());     // overridden
        assertFalse(cfg.getLoadOptions().isCanCreateIndex()); // overridden
    }
}