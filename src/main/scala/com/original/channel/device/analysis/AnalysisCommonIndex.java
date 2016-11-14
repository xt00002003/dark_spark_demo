package com.original.channel.device.analysis;

import com.google.common.base.Joiner;
import com.original.channel.conf.Configuration;
import com.original.channel.conf.ContextFactory;
import com.original.channel.util.Constants;
import com.original.channel.util.MapUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.text.NumberFormat;
import java.util.Map;

/**
 * 渠道应用活跃数，启动次数分析
 * @author codethink
 * @date 6/16/16 6:04 PM
 */
public class AnalysisCommonIndex extends Analysis {

    private static final Logger LOG = LoggerFactory.getLogger(AnalysisNewDevice.class);

    /**
     * 统计应用渠道的活跃数
     */
    @Override
    public void execute() {

        // 渠道应用活跃用户数
        final JavaPairRDD<String, Map<Object, Object>> actives = actives();
        // 渠道应用启动次数
        final JavaPairRDD<String, Map<Object, Object>> startups = startups();

        final Configuration config = ContextFactory.getConfig();
        final JavaPairRDD<String, Map<Object, Object>> esRdd = actives.union(startups)
            .reduceByKey(new Function2<Map<Object, Object>, Map<Object, Object>, Map<Object, Object>>() {
                public Map<Object, Object> call(final Map<Object, Object> v1, final Map<Object, Object> v2) throws Exception {
                    //这里需要获取出starts和actives值.然后计算出startAvg的值.
                    Object  starts=null==v1.get("starts")?v2.get("starts"):v1.get("starts");
                    Object  actives=null==v1.get("actives")?v2.get("actives"):v1.get("actives");
                    LOG.info("==> 获取的starts和actives的值是"+starts+";"+actives);
                    float denominator=Float.valueOf(actives.toString());
                    float startsF=Float.valueOf(starts.toString());
                    if (0==denominator){
                        denominator=1;
                    }
                    NumberFormat numberFormat = NumberFormat.getInstance();
                    numberFormat.setGroupingUsed(false);
                    // 设置精确到小数点后2位
                    numberFormat.setMaximumFractionDigits(2);
                    String startAvg=numberFormat.format(startsF / denominator);
                    v1.put("startAvg",startAvg);
                    v1.putAll(v2);
                    return v1;
                }
            });
//        JavaEsSpark.saveToEsWithMeta(esRdd, Constants.ES.CHANNEL_INDEX + "/"
//            + Constants.ES.CHANNEL_TYPE_STATISTICS, config.getEsConfigMap());
    }


    /**
     * 分析应用渠道的活跃用户数
     *
     * @return
     */
    private JavaPairRDD<String, Map<Object, Object>> actives() {
        final Map<String, String> envMap = ContextFactory.getEnvMap();
        LOG.info("==> 分析应用渠道活跃用户数:");
        final SQLContext sqlContext = ContextFactory.getHiveContext();
//        final DataFrame df = sqlContext.sql("SELECT project_id ,channel_id,country,count(1) as actives " +
//            "FROM sdk_log WHERE channel_id is not null and country is not null and startup_list is not null  GROUP BY project_id,channel_id,country");

        final DataFrame df = sqlContext.sql("SELECT pkg_name ,channel_id,country,count(1) as actives " +
                "FROM sdk_log WHERE channel_id is not null and country is not null and startup_list is not null  GROUP BY pkg_name,channel_id,country");

        return df.toJavaRDD().mapToPair(new PairFunction<Row, String, Map<Object, Object>>() {
            public Tuple2<String, Map<Object, Object>> call(final Row row)
                throws Exception {
//                String projectId = row.getAs("project_id").toString();
                String packetName = row.getAs("pkg_name").toString();
                String channel = row.getAs("channel_id").toString();
                String country=row.getAs("country").toString();
                String id= Joiner.on("#").join(envMap.get("statDateString"),packetName,channel,country);
                return new Tuple2<String, Map<Object, Object>>(id,
                    MapUtils.asMap(
                        "actives", row.getAs("actives"),"pkg_name",packetName,"channel",channel,"country",country,"day",envMap.get("statDateString")
                    )
                );
            }
        }).coalesce(Constants.SDK.NODE_NUM);
    }


    /**
     * 分析应用渠道的用户启动次数
     *
     * @return
     */
    private static JavaPairRDD<String, Map<Object, Object>> startups() {
        final Map<String, String> envMap = ContextFactory.getEnvMap();
        LOG.info("==> 分析应用渠道活启动次数:");
        final SQLContext sqlContext = ContextFactory.getHiveContext();
//        final DataFrame df = sqlContext.sql("SELECT project_id ,channel_id,country,count(1) as starts " +
//            "FROM sdk_log_not_distinct a LATERAL VIEW explode(startup_list) b as v_startUp " +
//            "WHERE startup_list is not null and channel_id is not null and country is not null " +
//            "GROUP BY project_id,channel_id,country");

        final DataFrame df = sqlContext.sql("SELECT pkg_name ,channel_id,country,count(1) as starts " +
                "FROM sdk_log_not_distinct a LATERAL VIEW explode(startup_list) b as v_startUp " +
                "WHERE startup_list is not null and channel_id is not null and country is not null " +
                "GROUP BY pkg_name,channel_id,country");
        return df.toJavaRDD().mapToPair(new PairFunction<Row, String, Map<Object, Object>>() {
            public Tuple2<String, Map<Object, Object>> call(final Row row)
                throws Exception {
//                String projectId = row.getAs("project_id").toString();
                String packetName = row.getAs("pkg_name").toString();
                String channel = row.getAs("channel_id").toString();
                String country=row.getAs("country").toString();
                String id= Joiner.on("#").join(envMap.get("statDateString"),packetName,channel,country);
                return new Tuple2<String, Map<Object, Object>>(id,
                    MapUtils.asMap(
                        "starts", row.getAs("starts"),"pkg_name",packetName,"channel",channel,"country",country,"day",envMap.get("statDateString")
                    )
                );
            }
        }).coalesce(Constants.SDK.NODE_NUM);
    }
}
