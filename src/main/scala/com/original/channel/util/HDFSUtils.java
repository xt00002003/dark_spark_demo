package com.original.channel.util;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * HDFS utils
 */
public class HDFSUtils {

    private static Configuration conf;

    private static FileSystem fileSystem;

    private static Logger LOG = LoggerFactory.getLogger(HDFSUtils.class);

    static {
        try {
            conf = Job.getInstance().getConfiguration();
            fileSystem = FileSystem.get(conf);
        } catch (IOException e) {
            LOG.error("fail to init HDFSUtils", e);
            throw new IllegalStateException();
        }
    }

    public static FSDataOutputStream create(String filepath) throws IOException {
        return fileSystem.create(new Path(filepath), true);
    }

    public static FileStatus[] getFiles(String path) throws IOException {
        return fileSystem.listStatus(new Path(path));
    }

    public static InputStream getFile(String filename) throws IOException {
        return fileSystem.open(new Path(filename));
    }

    public static boolean delete(String filename) throws IOException {
        return fileSystem.delete(new Path(filename), true);
    }

    public static int write(String path, String destPath, Boolean overwrite) throws IOException {
        File file = new File(path);
        try (FSDataOutputStream out = fileSystem.create(new Path(destPath + file.getName()));
             BufferedInputStream in = new BufferedInputStream(new FileInputStream(file))) {
            return IOUtils.copy(in, out);
        }
    }

    public static void append(String to, String from) throws IOException {
        File file = new File(to);
        try (FSDataOutputStream out = fileSystem.append(new Path(file.getName()));
             BufferedInputStream in = new BufferedInputStream(new FileInputStream(new File(from)))) {
            byte[] b = new byte[1024];
            int numBytes = in.read(b);
            while (numBytes > 0) {
                out.write(b, 0, numBytes);
                numBytes = in.read(b);
            }
        }
    }

    public static void mkdirs(String folderPath) throws IOException {
        Path path = new Path(folderPath);
        if (!fileSystem.exists(path)) {
            fileSystem.mkdirs(path);
        }
    }

    public static void close() throws IOException {
        fileSystem.close();
    }
}
