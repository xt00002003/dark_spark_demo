package com.original.channel.elasticsearch;

import com.google.common.base.Preconditions;
import com.google.gson.JsonObject;
import com.original.channel.elasticsearch.Model.UpdateDoc;
import com.original.channel.elasticsearch.Model.UpsertDoc;
import com.original.channel.util.Constants;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.client.http.JestHttpClient;
import io.searchbox.core.*;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author xiongzhao
 * @date 6/2/16
 * @Time 1:16 PM
 */
public class EsJestSupport implements EsOperation, Serializable {

    private final Logger LOG = LoggerFactory.getLogger(EsJestSupport.class);

    private final Map<String, String> esConfig;

    public EsJestSupport(final Map<String, String> esMap) {
        this.esConfig = esMap;
    }

    public JsonObject getDocumentById(final JestClient jestClient, final String index, final String type, final String esId) {
        Preconditions.checkNotNull(jestClient);

        final DocumentResult result;
        final Get get = new Get.
            Builder(index, esId)
            .type(type)
            .build();
        try {
            result = jestClient.execute(get);
        } catch (final IOException e) {
            LOG.error("Get Es Document failure", e);
            return null;
        }
        LOG.info(result.getJsonString());
        if (!result.isSucceeded()) {
            return null;
        }
        return result.getJsonObject().getAsJsonObject(Constants.ES.ES_SOURCE);
    }

    public Boolean indexDocument(
        final JestClient jestClient,
        final String index,
        final String type,
        final String esId,
        final Map<Object, Object> source) {
        Preconditions.checkNotNull(jestClient);

        final DocumentResult result;
        try {
            result = jestClient.execute(
                new Index.Builder(source)
                    .index(index)
                    .type(type)
                    .id(esId)
                    .refresh(true)
                    .build()
            );
        } catch (final IOException e) {
            LOG.error("Save Es Document failure", e);
            return false;
        }
        LOG.info(result.getJsonString());
        if (!result.isSucceeded()) {
            LOG.error(result.getErrorMessage());
        }
        return result.isSucceeded();
    }

    public Boolean upsertDocument(
        final JestClient jestClient,
        final String index,
        final String type,
        final String esId,
        final Map<Object, Object> source) {
        final UpsertDoc doc = new UpsertDoc();
        doc.setUpsert(source);
        doc.setDoc(source);
        return this.updateDocument0(jestClient, index, type, esId, doc);

    }

    public Boolean updateDocument(
        final JestClient jestClient,
        final String index,
        final String type,
        final String esId,
        final Map<Object, Object> source) {
        final UpdateDoc doc = new UpdateDoc();
        doc.setDoc(source);
        return this.updateDocument0(jestClient, index, type, esId, doc);
    }

    private boolean updateDocument0(
        final JestClient jestClient,
        final String index,
        final String type,
        final String esId,
        final UpdateDoc doc) {
        final DocumentResult result;
        try {
            result = jestClient.execute(
                new Update.Builder(doc)
                    .index(index)
                    .type(type)
                    .id(esId)
                    .build()
            );
        } catch (final IOException e) {
            LOG.error("Update Es Document failure", e);
            return false;
        }
        LOG.info(result.getJsonString());
        if (!result.isSucceeded()) {
            LOG.error(result.getErrorMessage());
        }
        return result.isSucceeded();
    }


    public List<HashMap> search(JestClient jestClient,DateTime start) {
        List<HashMap> list = null;
        try {
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//            DateTime start = DateTimeUtils.fromUtcDateString(day);
            DateTime end = start.plusDays(1);
            sourceBuilder.postFilter(QueryBuilders.rangeQuery("first_time").from(start).to(end));
            Search search = new Search.Builder(sourceBuilder.toString()).addIndex(Constants.ES.CHANNEL_INDEX).addType(Constants.ES.CHANNEL_TYPE_APP_DEVICE)
                .build();
            JestResult result = jestClient.execute(search);
            list = result.getSourceAsObjectList(HashMap.class);
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        return list;
    }

    public Map<String,Long> aggregation(JestClient jestClient,String field) {
        //查询索引
        SearchSourceBuilder sourceBuilder=new SearchSourceBuilder();

        Map<String,Long> aggregationMap=new HashMap();

        try {
            sourceBuilder.aggregation(AggregationBuilders.terms("rarities").field(field)
                .size(0));
            Search search = new Search.Builder(sourceBuilder.toString()).addIndex(Constants.ES.CHANNEL_INDEX).addType(Constants.ES.CHANNEL_TYPE_APP_DEVICE)
                .build();
            SearchResult result1 = jestClient.execute(search);


            List<TermsAggregation.Entry> entries=result1.getAggregations().getTermsAggregation("rarities").getBuckets();
            for (TermsAggregation.Entry entry:entries){
                aggregationMap.put(entry.getKey(),entry.getCount());
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        return aggregationMap;
    }

    public JestHttpClient getJestClient() {
        return EsJestClient.buildClient(esConfig);
    }
}
