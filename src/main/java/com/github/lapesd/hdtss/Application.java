package com.github.lapesd.hdtss;

import io.micronaut.runtime.Micronaut;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

@Slf4j
public class Application {
    private static final Pattern OPTION_RX = Pattern.compile("^--?");
    private static final Pattern HDT_LOCATION_RX = Pattern.compile("^--?hdt.location");

    public static void main(String[] args) {
        Micronaut.run(Application.class, promoteOrphansToHdtLocation(args));
    }

    private static @NonNull String @NonNull []
    promoteOrphansToHdtLocation(@NonNull String @NonNull [] args) {
        List<String> orphans = new ArrayList<>();
        boolean expectValue = false;
        boolean hadHdtLocation = false;
        for (String arg : args) {
            if (OPTION_RX.matcher(arg).find()) {
                expectValue = arg.indexOf('=') < 0;
                hadHdtLocation = HDT_LOCATION_RX.matcher(arg).find();
            } else if (!expectValue) {
                orphans.add(arg);
            } else {
                expectValue = false;
            }
        }
        if (orphans.size() > 1 || (orphans.size() == 1 && hadHdtLocation)) {
            System.err.println("Multiple HDT files are not supported (yet)!\n");
            System.exit(1);
        } else if (orphans.size() == 1) {
            log.debug("Taking orphan argument {} as -hdt.location", orphans.get(0));
            List<String> list = new ArrayList<>(Arrays.asList(args));
            list.add("-hdt.location="+orphans.get(0));
            args = list.toArray(String[]::new);
        }
        return args;
    }
}
