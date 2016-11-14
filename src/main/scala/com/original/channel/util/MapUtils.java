package com.original.channel.util;

import java.util.HashMap;
import java.util.Map;

public class MapUtils {

    private MapUtils() {
    }

    public static Map<Object, Object> asMap(final Object... args) {
        if (0 != (args.length % 2)) {
            throw new IllegalArgumentException("the number of arguments must be even");
        }
        final Map<Object, Object> map = new HashMap<Object, Object>();
        for (int i = 0; i < args.length; i += 2) {
            map.put(args[i], args[i + 1]);
        }
        return map;
    }

}
