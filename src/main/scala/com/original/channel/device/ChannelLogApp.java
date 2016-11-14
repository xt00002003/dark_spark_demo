package com.original.channel.device;

import com.original.channel.conf.ContextFactory;
import com.original.channel.device.analysis.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * @author codethink
// * @date 6/14/16 3:29 PM
 */
public class ChannelLogApp {

    private static final Logger LOG = LoggerFactory.getLogger(ChannelLogApp.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            LOG.info("Usage: com.tcl.mig.datacenter.sdk.device.SdkLogApp <input>, "
                + "e.g.input");
            System.exit(1);
        }
        String statDateStr = args[0];
//        String statDateStr = "2016-09-05";


        LOG.info("==> Prepare ENV & register UDFs....{}",statDateStr);
        ContextFactory.createContext(statDateStr);

        LOG.info("==>分析开始");
        List<Analysis> analysisSteps = new LinkedList<Analysis>();
        analysisSteps.add(new AnalysisPreRdd());   //  RDD预处理
        analysisSteps.add(new AnalysisNewDevice());//  分析应用渠道新增用户数,总用户数
        analysisSteps.add(new AnalysisCommonIndex());//分析应用渠道活跃用户数，启动次数
        analysisSteps.add(new AnalysisKeepDevice()); //1/3/7/30日后留存率分析
        for (Analysis analysis : analysisSteps) {
            analysis.execute();
        }
        LOG.info("==>分析结束.");
    }



}



