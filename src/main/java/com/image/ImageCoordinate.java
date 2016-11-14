package com.image;

/**
 * 图像坐标对象 Created by csma on 7/15/16.
 */
public class ImageCoordinate extends ImageDimension {

    private int left = 0;
    private int top = 0;

    public ImageCoordinate(int left, int top, int width, int height) {
        super(width, height);
        this.left = left;
        this.top = top;
    }

    public int getLeft() {
        return left;
    }

    public void setLeft(int left) {
        this.left = left;
    }

    public int getTop() {
        return top;
    }

    public void setTop(int top) {
        this.top = top;
    }
}
