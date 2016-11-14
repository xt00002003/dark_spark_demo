package com.original.channel.util;

/**
 * utils for spark RDD
 */
public class RDDUtils {
    private String delimiter;
    private int index;
    private String content;

    public RDDUtils(String delimiter, int index, String content) {
        this.delimiter = delimiter;
        this.index = index;
        this.content = content;
    }

    public boolean filterContent(String line) {
        String[] tempResult = line.split(delimiter);
        if (tempResult.length > index) {
            return tempResult[index].equalsIgnoreCase(content);
        } else {
            return false;
        }
    }
}
