package com.github.lapesd.hdtss.sparql.results;

import com.github.lapesd.hdtss.model.Term;
import com.github.lapesd.hdtss.model.solutions.BatchQuerySolutions;
import com.github.lapesd.hdtss.model.solutions.QuerySolutions;
import com.github.lapesd.hdtss.model.solutions.SolutionRow;
import io.micronaut.buffer.netty.NettyByteBufferFactory;
import io.micronaut.context.ApplicationContext;
import io.micronaut.core.io.buffer.ByteBuffer;
import io.micronaut.http.codec.MediaTypeCodec;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.ByteArrayOutputStream;
import java.util.List;

import static com.github.lapesd.hdtss.TestVocab.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class CodecTestBase {
    public static QuerySolutions ASK_FALSE = new BatchQuerySolutions(List.of(), List.of());
    public static QuerySolutions ASK_TRUE = new BatchQuerySolutions(List.of(), List.of(SolutionRow.EMPTY));
    public static QuerySolutions ONE_ONE = new BatchQuerySolutions(List.of("x"),
            List.of(SolutionRow.of(Alice)));
    public static  QuerySolutions ONE_ROW = new BatchQuerySolutions(asList("x", "y", "z", "w"),
            List.of(SolutionRow.of(AliceEN, bob, i23, blank1)));
    public static QuerySolutions TWO_ROWS = new BatchQuerySolutions(List.of("x"),
            asList(new SolutionRow(new Term[]{null}), SolutionRow.of(Charlie)));
    ApplicationContext applicationContext;
    MediaTypeCodec codec;
    final Class<? extends MediaTypeCodec> encoderClass;

    public CodecTestBase(Class<? extends MediaTypeCodec> encoderClass) {
        this.encoderClass = encoderClass;
    }


    @BeforeEach
    void setUp() {
        codec = (applicationContext = ApplicationContext.run()).createBean(encoderClass);
    }

    @AfterEach
    void tearDown() {
        applicationContext.close();
    }

    protected void doTestEncodeToStream(@NonNull QuerySolutions solutions, @NonNull String expected) {
        ByteArrayOutputStream bOS = new ByteArrayOutputStream();
        codec.encode(solutions, bOS);
        assertEquals(expected, bOS.toString(UTF_8));
    }

    protected void doTestEncodeToArray(@NonNull QuerySolutions solutions, @NonNull String expected) {
        byte[] bytes = codec.encode(solutions);
        assertEquals(expected, new String(bytes, UTF_8));
    }

    protected void doTestEncodeToBuffer(@NonNull QuerySolutions solutions, @NonNull String expected) {
        NettyByteBufferFactory allocator = new NettyByteBufferFactory();
        ByteBuffer<?> buffer = codec.encode(solutions, allocator);
        String actual = buffer.readCharSequence(buffer.readableBytes(), UTF_8).toString();
        assertEquals(expected, actual);
    }

    protected void doTestDecode(@NonNull QuerySolutions expected, @NonNull String encoded) {
        QuerySolutions actual = codec.decode(QuerySolutions.class, encoded);
        assertEquals(expected.varNames(), actual.varNames());
        assertEquals(expected.list(), actual.list());
    }
}
