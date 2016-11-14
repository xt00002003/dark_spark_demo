package com.original.channel.util;

/**
 * Created with IntelliJ IDEA.
 *
 * @author xiongzhao
 * @date 5/31/16
 * @Time 2:39 PM
 */
public abstract class Constants {

    public abstract class SDK {
        //用户事件 0..n
        public static final String EV = "ev";
        //异常信息 0...n
        public static final String EX = "ex";
        //用户启动次数 /Session 0 .. n
        public static final String SE = "se";
        //用户设备 1..1
        public static final String US = "us";

        public static final int NODE_NUM = 7;
    }


    public abstract class US {
        //IMEI 号
        public static final String IMEI = "im";

        //mac 地址
        public static final String MAC = "mac";

        //(androidId)
        public static final String ANDROID_ID = "ai";

        //业务编码
        public static final String APP_KEY = "appkey";

        //机型
        public static final String MODEL = "t";

        //国家
        public static final String COUNTRY = "c";

        //语言
        public static final String LANGUAGE = "la";

        //渠道
        public static final String CHANNEL = "ch";

        //包名

        public static final String PACKAGE="pn";

    }


    public abstract class ES {
        public static final String CHANNEL_INDEX = "p_channel";
        public static final String CHANNEL_TYPE_APP_DEVICE = "app_devices";
        public static final String CHANNEL_TYPE_STATISTICS = "channel_statistics";
        public static final String CHANNEL_TYPE_DEVICES = "devices";


        public static final String ES_SOURCE = "_source";
        public static final String TS = "@timestamp";
    }


    //字段属性
    public abstract class FIELD {
        public static final String KEEP_1 = "keep1";
        public static final String KEEP_3 = "keep3";
        public static final String KEEP_7 = "keep7";
        public static final String KEEP_30 = "keep30";
    }

}
