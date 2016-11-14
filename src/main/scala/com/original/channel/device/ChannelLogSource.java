package com.original.channel.device;

import com.hadoop.mapreduce.LzoTextInputFormat;
import com.original.channel.conf.ContextFactory;
import com.original.channel.util.DateTimeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ChannelLogSource {

    private static final Logger LOG = LoggerFactory.getLogger(ChannelLogApp.class);

    /**
     * 文件转换为RDD
     *
     * @param inputFile
     * @return
     */
    public static JavaRDD<Text> lzoData(final String inputFile) {
        final Configuration conf;
        JavaSparkContext sc= ContextFactory.getSparkContext();
        try {
            conf = Job.getInstance().getConfiguration();
        } catch (final IOException e) {
            throw new RuntimeException("Fail to get the MR job configuration", e);
        }
        conf.set("io.compression.codecs", "com.hadoop.compression.lzo.LzopCodec");
        conf.set("io.compression.codec.lzo.class", "com.hadoop.compression.lzo.LzoCodec");
        final JavaPairRDD<LongWritable, Text> files =
            sc.newAPIHadoopFile(inputFile, LzoTextInputFormat.class, LongWritable.class, Text.class, conf);
        return files.values();
    }

    /**
     * 获取HDFS目录文件列表
     *
     * @param statDate
     * @param hdfsPath
     * @return
     * @throws IOException
     */
    public static Set<String> getInputFile(final DateTime statDate, final String hdfsPath) {
        LOG.info("According to statDate load inputFile data...");
        final Set<String> validFiles = new HashSet<String>();
        try {
            //本地开始使用.提交代码需要还原
//            Configuration configuration = new Configuration();
//            String localPath = "/data/workspaces/work/cta/genlog/target/classes/";
//            configuration.addResource(new Path(localPath + "core-site.xml"));
//            configuration.addResource(new Path(localPath + "hdfs-site.xml"));
            final FileSystem fileSystem = FileSystem.get(Job.getInstance().getConfiguration());
//            final FileSystem fileSystem = FileSystem.get(configuration);
            final Path dirPath = new Path(hdfsPath + DateTimeUtils.toDateString(statDate));
            final FileStatus[] dirStatus = fileSystem.globStatus(dirPath);
            if (null == dirStatus) {
                return validFiles;
            }
            for (final FileStatus df : dirStatus) {
                final FileStatus[] fileStatus = fileSystem.listStatus(df.getPath());
                for (final FileStatus fs : fileStatus) {
                    final String path = fs.getPath().toString();
                    if (fs.getLen() > 0 && !StringUtils.endsWith(path, ".tmp")) {
                        validFiles.add(path);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage(),e);
        }
        return validFiles;
    }
}
