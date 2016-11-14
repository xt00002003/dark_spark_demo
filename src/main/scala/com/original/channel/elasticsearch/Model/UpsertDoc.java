package com.original.channel.elasticsearch.Model;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author xiongzhao
 * @date 6/3/16
 * @Time 3:08 PM
 */
public class UpsertDoc extends UpdateDoc {

    private Map<Object, Object> upsert;

    public Map<Object, Object> getUpsert() {
        return upsert;
    }

    public void setUpsert(final Map<Object, Object> upsert) {
        this.upsert = upsert;
    }

    /*@Override
    public String toString() {
        return JSON.toJSONString(this);
    }*/
}
