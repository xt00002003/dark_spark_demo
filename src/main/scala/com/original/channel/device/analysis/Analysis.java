package com.original.channel.device.analysis;

import java.io.Serializable;

/**
 * @author codethink
 * @date 6/16/16 6:00 PM
 */
public abstract class Analysis implements Serializable {
    /**
     * 分析任务
     */
    public abstract void execute();
}
