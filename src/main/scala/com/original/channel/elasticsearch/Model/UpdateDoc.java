package com.original.channel.elasticsearch.Model;

import java.io.Serializable;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author xiongzhao
 * @date 6/3/16
 * @Time 3:59 PM
 */
public class UpdateDoc implements Serializable {

    private Map<Object, Object> doc;

    public Map<Object, Object> getDoc() {
        return doc;
    }

    public void setDoc(final Map<Object, Object> doc) {
        this.doc = doc;
    }
}
