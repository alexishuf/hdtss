package com.github.lapesd.hdtss.data.load;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.core.bind.annotation.Bindable;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;


@ConfigurationProperties("hdt")
public interface HdtConfig {
    @NotBlank @NotEmpty @NotNull String getLocation();
    @NotNull LoadOptions getLoadOptions();

    @ConfigurationProperties("load")
    interface LoadOptions {
        @Bindable(defaultValue =  "true") boolean isIndexed();
        @Bindable(defaultValue =  "true") boolean isNative();
        @Bindable(defaultValue =  "true") boolean isMmap();
        @Bindable(defaultValue =  "true") boolean isCanCreateIndex();
        @Bindable(defaultValue = "false") boolean isRequireAll();
    }
}
