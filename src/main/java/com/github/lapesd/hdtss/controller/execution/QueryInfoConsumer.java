package com.github.lapesd.hdtss.controller.execution;

import lombok.NonNull;

import java.util.function.Consumer;

public interface QueryInfoConsumer extends Consumer<@NonNull QueryInfo> {
}
