package com.original.channel.device.analysis;

import com.google.common.base.Joiner;
import com.original.channel.conf.Configuration;
import com.original.channel.conf.ContextFactory;
import com.original.channel.device.ChannelLogParser;
import com.original.channel.device.ChannelLogSource;
import com.original.channel.model.ChannelLogInfo;
import com.original.channel.util.DataFrameConverter;
import com.original.channel.util.DateTimeUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Set;

import static org.apache.commons.lang3.StringUtils.join;

/**
 * 文件读取、解析、预处理RDD
 * @author codethink
 * @date 6/16/16 6:40 PM
 */
public class AnalysisPreRdd extends Analysis {

    private static final Logger LOG = LoggerFactory.getLogger(AnalysisNewDevice.class);

    public void execute() {
        final Configuration config = ContextFactory.getConfig();
        LOG.info("==> Loading data...");
        final String hdfsPath = config.getString(Configuration.HDFS_PATH);
        final String statDateStr = ContextFactory.getEnvMap().get("statDateStr");
        final DateTime statDate = DateTimeUtils.fromDateString(statDateStr);
        final Set<String> inputFiles = ChannelLogSource.getInputFile(statDate, hdfsPath);
        if (inputFiles.size() == 0) {
            throw new IllegalStateException("Can't find any input files.");
        }
        String inputFile = join(inputFiles, ',');
        LOG.info("==>读取解析文件,{}", inputFile);
        final JavaRDD<Text> lineRdd = ChannelLogSource.lzoData(inputFile);
        //解析
        JavaRDD<ChannelLogInfo> channelRdd = lineRdd.flatMap(new ChannelLogParser());

        LOG.info("==> Distinct device_id,projectCode,channel...");

        final JavaRDD<ChannelLogInfo> processedChannelRdd = processDeviceRdd(channelRdd);

        ContextFactory.getRddMap().put("channelRdd", channelRdd);

        ContextFactory.getRddMap().put("processedChannelRdd",processedChannelRdd);

        final HiveContext sqlContext = ContextFactory.getHiveContext();

        LOG.info("==> Preparing SQL Context...");
//        final DataFrame deviceDF = DataFrameConverter.toDataFrame(sqlContext, processedChannelRdd);

//        deviceDF.registerTempTable("sdk_log");
//        LOG.info("==> Preparing Not Distinct SQL Context...");
//        final DataFrame deviceDFNotDistinct =
//            DataFrameConverter.toDataFrame(sqlContext, channelRdd);
//        deviceDFNotDistinct.registerTempTable("sdk_log_not_distinct");
    }


    /**
     * 按应用设备数去重
     *
     * @param channelRdd
     * @return
     */
    private JavaRDD<ChannelLogInfo> processDeviceRdd(final JavaRDD<ChannelLogInfo> channelRdd) {
        return channelRdd.mapToPair(new PairFunction<ChannelLogInfo, String, ChannelLogInfo>() {
            public Tuple2<String, ChannelLogInfo> call(final ChannelLogInfo log) throws Exception {
                //原来逻辑,使用project_id#device_id#channel_id
//                String logKey = Joiner.on("#").join(log.project_id(),log.device_id(),log.channel_id());
                //现在修改逻辑,使用packet_name来做唯一标识.pkg_name#device_id#channel_id
                String logKey = Joiner.on("#").join(log.pkg_name(),log.device_id(),log.channel_id());
                return new Tuple2<String, ChannelLogInfo>(logKey, log);
            }
        }).reduceByKey(new Function2<ChannelLogInfo, ChannelLogInfo, ChannelLogInfo>() {
            public ChannelLogInfo call(final ChannelLogInfo info1, final ChannelLogInfo info2)
                throws Exception {
                return info1.timestamp() > info2.timestamp() ? info1 : info2;
            }
        }).values().groupBy(new Function<ChannelLogInfo, String>() {
            public String call(final ChannelLogInfo info) throws Exception {
                //原来的逻辑
//                String logKey = Joiner.on("#").join(info.project_id(),info.device_id(),info.channel_id());
                //现在修改逻辑,使用packet_name来做唯一标识.pkg_name#device_id#channel_id
                String logKey = Joiner.on("#").join(info.pkg_name(),info.device_id(),info.channel_id());
                return logKey;
            }
        }).flatMap(new FlatMapFunction<Tuple2<String, Iterable<ChannelLogInfo>>, ChannelLogInfo>() {
            public Iterable<ChannelLogInfo> call(final Tuple2<String, Iterable<ChannelLogInfo>> entry)
                throws Exception {
                return entry._2();
            }
        });
    }
}
