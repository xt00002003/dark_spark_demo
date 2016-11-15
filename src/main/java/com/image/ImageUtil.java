package com.image;

import com.gif.AnimatedGifEncoder;
import com.gif.GifDecoder;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.*;
import javax.imageio.plugins.jpeg.JPEGImageWriteParam;
import javax.imageio.stream.ImageInputStream;
import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.io.*;
import java.util.Iterator;

/**
 * 通用的图片处理工具，包括图片压缩，裁减等功能
 *
 */
public class ImageUtil {

    private static final Logger logger = LoggerFactory.getLogger(ImageUtil.class);

    /**
     * 对图片进行压缩
     * 
     * @param im 原缓冲图
     * @param dimension 目标大小
     * @return 新缓冲图
     */
    public static BufferedImage zoomCommonImage(BufferedImage im, ImageDimension dimension) {
        BufferedImage result;
        try {
            result = new BufferedImage(dimension.getWidth(), dimension.getHeight(),
                    BufferedImage.TYPE_INT_RGB);
            result.getGraphics().drawImage(im.getScaledInstance(dimension.getWidth(),
                    dimension.getHeight(), Image.SCALE_SMOOTH), 0, 0, null);
        } catch (Exception ex) {
            logger.error("zoomCommonImage error:ex={}", ex.getMessage());
            return im; // 压缩失败后，我们应该尽量返回原图
        }
        return result;

    }

    /**
     * get the local file image dimension
     *
     * @param imageData imageData
     * @param formatName the format of the image
     * @return dimension object, null if any error occurs
     */
    public static ImageDimension getImageDimension(byte[] imageData, String formatName) {

        if (StringUtils.isBlank(formatName)) {
            return null;
        }

        ImageDimension dimension = null;

        InputStream inputStream = new ByteArrayInputStream(imageData);
        try {

            if (formatName.equals(IMAGE_FORMAT.GIF.getValue())) {
                // 如果是GIF的，处理方式不一样
                GifDecoder decoder = new GifDecoder();
                int status = decoder.read(inputStream);
                if (status != GifDecoder.STATUS_OK) {
                    logger.error("read gif image error!");
                    return null;
                }
                Dimension frameDimension = decoder.getFrameSize();

                if (frameDimension == null) {
                    logger.error("cannot get the size of gif image!");
                    return null;
                }

                dimension = new ImageDimension();
                dimension.setHeight((int) frameDimension.getHeight());
                dimension.setWidth((int) frameDimension.getWidth());
            } else {

                ImageInputStream iis =
                        ImageIO.createImageInputStream(new ByteArrayInputStream(imageData));

                Iterator<ImageReader> imageReader = ImageIO.getImageReaders(iis);
                if (imageReader.hasNext()) {
                    ImageReader reader = imageReader.next();
                    formatName = reader.getFormatName().toLowerCase();

                    reader.setInput(iis, true);

                    dimension = new ImageDimension();
                    dimension.setHeight(reader.getHeight(0));
                    dimension.setWidth(reader.getWidth(0));

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("exception is {}", e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Unknown Exception　while get image size: {}, type:{}", e.getMessage(),
                    formatName);
        }
        return dimension;
    }

    /**
     * 对图片进行切割和压缩，当然也可能即不压缩，也不切割
     *
     * @param imageData 原数据
     * @param formatName 图片格式
     * @param imageCoordinate 图片切割后的大小
     * @param standardDimension 图片压缩后的大小
     * @param maxGifSize GIF类图片截图的张数, 0传0的话，则表示所有的都取
     * @return 处理后的数据流
     */
    public static ByteArrayOutputStream cutAndZoomImage(byte[] imageData, String formatName,
            ImageCoordinate imageCoordinate, ImageDimension standardDimension, int maxGifSize)
            throws IOException {

        InputStream inputStream = new ByteArrayInputStream(imageData);

        boolean needCompose = standardDimension != null;
        boolean needCut = imageCoordinate != null;

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        // GIF需要特殊处理
        if (IMAGE_FORMAT.GIF.getValue().equals(formatName)) {

            GifDecoder decoder = new GifDecoder();
            int status = decoder.read(inputStream);
            if (status != GifDecoder.STATUS_OK) {
                throw new IOException("read image error!");
            }

            AnimatedGifEncoder encoder = new AnimatedGifEncoder();

            // 把数据写到字节流里
            encoder.start(outputStream);
            encoder.setRepeat(decoder.getLoopCount());

            int countSize = decoder.getFrameCount();
            if (maxGifSize >= 1 && countSize > maxGifSize) {
                countSize = maxGifSize;
            }

            // 对于GIF来说，按约定只取前几张图片
            for (int i = 0; i < countSize; i++) {
                encoder.setDelay(decoder.getDelay(i));
                BufferedImage childImage = decoder.getFrame(i);

                if (childImage == null) {
                    continue;
                }

                // 先切图
                if (needCut) {
                    childImage = cutImage(childImage, imageCoordinate);
                }

                // 再压缩
                if (needCompose) {
                    childImage = zoomCommonImage(childImage, standardDimension);
                }

                encoder.addFrame(childImage);
            }
            encoder.finish();

        } else {

            BufferedImage image = toBufferedImage(imageData);
            if (image == null) {
                return null;
            }

            if (needCut) {
                image = cutImage(image, imageCoordinate);
            }

            if (needCompose) {
                image = zoomCommonImage(image, standardDimension);
            }

            ImageIO.write(image, formatName, outputStream);
        }

        return outputStream;
    }

    /**
     * 对图片进行裁减，其实这是个很简单的操作
     * 
     * @param image 原图片
     * @param imageCoordinate 目标区域
     * @return 裁减后的图片
     */
    private static BufferedImage cutImage(BufferedImage image, ImageCoordinate imageCoordinate) {
        return image.getSubimage(imageCoordinate.getLeft(), imageCoordinate.getTop(),
                imageCoordinate.getWidth(), imageCoordinate.getHeight());
    }

    public enum IMAGE_FORMAT {
        BMP("bmp"), JPG("jpg"), JPEG("jpeg"), PNG("png"), GIF("gif");

        private String value;

        IMAGE_FORMAT(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    /**
     * 获取图片格式
     *
     * @param imageData 图片字节
     * @return 图片格式
     */
    public static String getImageFormatName(byte[] imageData) {
        String formatName = null;

        try {
            ImageInputStream iis =
                    ImageIO.createImageInputStream(new ByteArrayInputStream(imageData));

            Iterator<ImageReader> imageReader = ImageIO.getImageReaders(iis);
            if (imageReader.hasNext()) {
                ImageReader reader = imageReader.next();
                formatName = reader.getFormatName().toLowerCase();

            }
        } catch (IOException e) {

            logger.error("cannot get the imageInputStream, exception is {}", e.getMessage());
            return null;
        }

        return formatName;
    }

    /**
     * 处理失真的图片
     * 
     * @param imageData 图片字节
     * @return 缓存图片
     */
    public static BufferedImage toBufferedImage(byte[] imageData) throws IOException {

        // This code ensures that all the pixels in the image are loaded
        ImageIcon imageIcon = new ImageIcon(imageData);
        if (imageIcon.getImageLoadStatus() != MediaTracker.COMPLETE) {
            // 如果未能正常加载，则直接返回null
            return null;
        }

        Image image = imageIcon.getImage();

        // Create a buffered image with a format that's compatible with the screen
        BufferedImage bufferedImage = null;
        GraphicsEnvironment ge = GraphicsEnvironment.getLocalGraphicsEnvironment();
        try {
            // Determine the type of transparency of the new buffered image
            int transparency = Transparency.OPAQUE;
            // Create the buffered image
            GraphicsDevice gs = ge.getDefaultScreenDevice();
            GraphicsConfiguration gc = gs.getDefaultConfiguration();
            bufferedImage = gc.createCompatibleImage(image.getWidth(null), image.getHeight(null),
                    transparency);
        } catch (HeadlessException e) {
            System.out.println("this is a bad image");
            e.printStackTrace();
        }

        if (bufferedImage == null) {
            // Create a buffered image using the default color model
            int type = BufferedImage.TYPE_INT_RGB;
            bufferedImage = new BufferedImage(image.getWidth(null), image.getHeight(null), type);
        }

        // Copy image to buffered image
        Graphics g = bufferedImage.createGraphics();
        // Paint the image onto the buffered image
        g.drawImage(image, 0, 0, null);
        g.dispose();

        return bufferedImage;
    }

    public static byte[] getBytes(String filePath) {
        byte[] buffer = null;
        try {
            File file = new File(filePath);
            FileInputStream fis = new FileInputStream(file);
            ByteArrayOutputStream bos = new ByteArrayOutputStream(1000);
            byte[] b = new byte[1000];
            int n;
            while ((n = fis.read(b)) != -1) {
                bos.write(b, 0, n);
            }
            fis.close();
            bos.close();
            buffer = bos.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buffer;
    }

    /**
     * 对图片进行压缩
     *
     * @param imageSourceData 源数据
     * @return bytes
     */
    public static byte[] compressPic(byte[] imageSourceData, String formatName) {

        if (imageSourceData.length <= MAX_JPG_SIZE
                || !formatName.equals(IMAGE_FORMAT.JPEG.getValue())
                        && !formatName.equals(IMAGE_FORMAT.JPG.getValue())) {

            // 如果图片不大，并且也不是JPG或者是JPEG，则直接不处理
            return imageSourceData;
        }

        float quality = MAX_JPG_SIZE / imageSourceData.length;

        if (quality > 0.5) {
            quality = 0.5f;
        } else if (quality < 0.2) {
            quality = 0.2f;
        }

        ImageWriter imgWriter = ImageIO.getImageWritersByFormatName(formatName).next();
        ImageWriteParam imgWriteParams = new JPEGImageWriteParam(null);

        // 设置压缩参数
        imgWriteParams.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
        // 这里指定压缩的程度，取值0~1范围内，
        imgWriteParams.setCompressionQuality(quality);
        imgWriteParams.setProgressiveMode(ImageWriteParam.MODE_DISABLED);
        ColorModel colorModel = ColorModel.getRGBdefault();
        // 指定压缩时使用的色彩模式
        imgWriteParams.setDestinationType(
                new ImageTypeSpecifier(colorModel, colorModel.createCompatibleSampleModel(16, 16)));

        try {

            BufferedImage src = toBufferedImage(imageSourceData);

            if (src == null) {
                return imageSourceData;
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream(imageSourceData.length);

            imgWriter.reset();
            // 必须先指定 out值，才能调用write方法, ImageOutputStream可以通过任何OutputStream构造
            imgWriter.setOutput(ImageIO.createImageOutputStream(out));
            // 调用write方法，就可以向输入流写图片
            imgWriter.write(null, new IIOImage(src, null, null), imgWriteParams);

            out.flush();
            out.close();
            // is.close();

            return out.toByteArray();

        } catch (Exception e) {
            e.printStackTrace();
            return imageSourceData;
        }
    }

    /**
     * 对GIF图片进行切割和压缩，当然也可能即不压缩，也不切割
     *
     * @param imageData 原数据
     * @param imageCoordinate 图片切割后的大小
     * @param standardDimension 图片压缩后的大小
     * @param maxGifSize GIF类图片截图的张数, 0传0的话，则表示所有的都取
     * @return 处理后的数据流, 这个流要包括两个，一个是GIF的第一张，另外是整个GIF，或者指定数量的GIF
     */
    public static ByteArrayOutputStream[] cutAndZoomGifImage(byte[] imageData,
            ImageCoordinate imageCoordinate, ImageDimension standardDimension, int maxGifSize)
            throws IOException {

        InputStream inputStream = new ByteArrayInputStream(imageData);

        boolean needCompose = standardDimension != null;
        boolean needCut = imageCoordinate != null;

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ByteArrayOutputStream gifCoverOs = new ByteArrayOutputStream();

        GifDecoder decoder = new GifDecoder();
        int status = decoder.read(inputStream);
        if (status != GifDecoder.STATUS_OK) {
            throw new IOException("read image error!");
        }

        AnimatedGifEncoder encoder = new AnimatedGifEncoder();

        // 把数据写到字节流里
        encoder.start(outputStream);
        encoder.setRepeat(decoder.getLoopCount());

        int countSize = decoder.getFrameCount();
        if (maxGifSize >= 1 && countSize > maxGifSize) {
            countSize = maxGifSize;
        }

        // 对于GIF来说，按约定只取前几张图片
        BufferedImage coverImage = null;
        for (int i = 0; i < countSize; i++) {
            encoder.setDelay(decoder.getDelay(i));
            BufferedImage childImage = decoder.getFrame(i);

            if (childImage == null) {
                continue;
            }

            // 先切图
            if (needCut) {
                childImage = cutImage(childImage, imageCoordinate);
            }

            // 再压缩
            if (needCompose) {
                childImage = zoomCommonImage(childImage, standardDimension);
            }


            if(coverImage == null) {
                coverImage = childImage;//封面图
            }

            encoder.addFrame(childImage);
        }
        encoder.finish();



        //接下来要处理cover
        if(coverImage != null) {
            ImageIO.write(coverImage, "gif", gifCoverOs);
        }


        return new ByteArrayOutputStream[] {gifCoverOs, outputStream};
    }

    /**
     * 构造GIF模板
     * @param gifCover gifcover
     * @param imgUrl 图片地址
     * @param width　宽度
     * @param height　高度
     * @return gif tag
     */
    public static String generateGifTag(String gifCover, String imgUrl, int width, int height) {

        return "<div class='gif-box'>" +
                "<img src='" + gifCover + "' data-src='" +
                imgUrl + "' data-width='" + width +
                "' data-height='" + height + "' alt='' />" +
                "<div class='tag-box'>"
                + "<i class='ico-tag-detail ico-tag-gif-for-detail'></i><div class='gif-loading-box'>"
                + "<i class='ico-tag-detail ico-tag-gif-bg-for-detail'></i>"
                + "<div class='loading-pic'></div></div></div></div>";
    }

    public static String generateImgTag(String imgUrl, int width, int height){


        return "<img data-width='" + width +
                "' data-height='" + height +
                "' src='" + imgUrl + "' /> ";
    }

    public static String generateNormalImg(String imgUrl){
        return "<img src='" + imgUrl + "' />";
    }

    public static final float MAX_JPG_SIZE = 100 * 1024;// JPG最大的大小，100KB，这里表示的是字节数
}
