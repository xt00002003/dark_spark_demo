package com.image;

/**
 * The Image demension
 * 
 * @author yliu
 * @date 30 May, 2016 3:38:04 pm
 * @version v1.0
 */
public class ImageDimension {
    private int width;
    private int height;

    public ImageDimension() {}

    public ImageDimension(int width, int height) {
        this.width = width;
        this.height = height;
    }

    public int getWidth() {
        return width;
    }

    public void setWidth(int width) {
        this.width = width;
    }

    public int getHeight() {
        return height;
    }

    public void setHeight(int height) {
        this.height = height;
    }

}
