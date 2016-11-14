package com.original.channel.elasticsearch;

import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.http.JestHttpClient;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author xiongzhao
 * @date 6/2/16
 * @Time 1:13 PM
 */
public interface EsOperation extends Serializable {

    /**
     * Get es document by id
     *
     * @param jestClient jest client
     * @param index      index
     * @param type       type
     * @param esId       id
     * @return source document JsonObject
     */
    JsonObject getDocumentById(JestClient jestClient, String index, String type, String esId);

    /**
     * insert or update es document
     *
     * @param jestClient jest client
     * @param index      index
     * @param type       type
     * @param esId       id
     * @param source     need to save document
     * @return index success or not
     */
    Boolean indexDocument(JestClient jestClient, String index, String type, String esId, Map<Object, Object> source);

    /**
     * upsert es document by id
     *
     * @param jestClient jest client
     * @param index      index
     * @param type       type
     * @param esId       id
     * @param source     upsert document source
     * @return upsert success or not
     */
    Boolean upsertDocument(JestClient jestClient, String index, String type, String esId, Map<Object, Object> source);

    /**
     * update es documet by id
     *
     * @param jestClient jest client
     * @param index      index
     * @param type       type
     * @param esId       id
     * @param source     update document source
     * @return update success or not
     */
    Boolean updateDocument(JestClient jestClient, String index, String type, String esId, Map<Object, Object> source);


    /**
     * 查询某天的新设备列表
     *
     * @param jestClient
     * @param day
     * @return
     */
    public List<HashMap> search(JestClient jestClient, DateTime start);

    /**
     * 按字段聚合
     *
     * @param jestClient
     * @param field
     * @return
     */
    public Map<String, Long> aggregation(JestClient jestClient, String field);

    /**
     * @return jest client
     */
    JestHttpClient getJestClient();
}
