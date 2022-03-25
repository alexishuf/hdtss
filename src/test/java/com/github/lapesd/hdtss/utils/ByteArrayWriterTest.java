package com.github.lapesd.hdtss.utils;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_16;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ByteArrayWriterTest {

    private static @NonNull String filler(int len) {
        StringBuilder b = new StringBuilder(len);
        for (int i = 0; i < len; i++)
            b.append('0'+ (i%10));
        return b.toString();
    }

    private static String inputToString(Object input) {
        if (input instanceof CharSequence cs)
            return cs.toString();
        else if (input instanceof Byte b)
            return ""+Character.toString(b);
        else if (input instanceof Integer b)
            return ""+Character.toString(b);
        else if (input instanceof Character c)
            return ""+c;
        else if (input == null)
            throw new NullPointerException("input cannot be null");
        throw new IllegalArgumentException("Unexpected input type: "+input.getClass());
    }

    public static @NonNull Stream<Arguments> test() {
        String cap = filler(ByteArrayWriter.ENCODER_CAPACITY);
        String cap1 = filler(ByteArrayWriter.ENCODER_CAPACITY) + "x";
        String cap_1 = filler(ByteArrayWriter.ENCODER_CAPACITY-1);
        List<List<Object>> inputsLists = List.of(
                List.of(),
                List.of("asd"),
                List.of("ação"),
                List.of("\uD83E\uDE02"),
                List.of("asd", "qwe"),
                List.of("aç", "ão"),
                List.of("{}", "ão", "\uD83E\uDE02", "."),
                List.of('{', (byte)'"', "\uD83E\uDE02", (int)'.'),
                List.of(cap),
                List.of(cap, "asd"),
                List.of(cap, cap),
                List.of(cap1, "asd"),
                List.of(cap1, cap1),
                List.of(cap_1, "ã"),
                List.of(cap_1, "çãé"),
                List.of(cap_1, "\uD83E\uDE02"),
                List.of(cap_1+"\uD83E\uDE02", "ão")
        );
        return Stream.of(UTF_8, UTF_16).flatMap(cs -> inputsLists.stream().map(inputs -> {
            String expected = inputs.stream().map(ByteArrayWriterTest::inputToString)
                                             .reduce("", String::concat);
            return arguments(inputs, cs, expected);
        }));
    }

    @ParameterizedTest @MethodSource
    public void test(List<Object> inputs, Charset charset, String expected) {
        ByteArrayWriter writer = new ByteArrayWriter(charset);
        for (Object input : inputs) {
            if (input instanceof CharSequence cs)
                assertSame(writer, writer.append(cs));
            else if (input instanceof byte[] bytes)
                assertSame(writer, writer.append(bytes));
            else if (input instanceof Byte b)
                assertSame(writer, writer.append(b));
            else if (input instanceof Character c)
                assertSame(writer, writer.append(c));
            else if (input instanceof Integer i)
                assertSame(writer, writer.append(i));
        }
        assertEquals(expected, new String(writer.toByteArray(), charset));
    }

}