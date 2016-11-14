package com.original.channel.elasticsearch;

import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.client.http.JestHttpClient;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @author xiongzhao
 * @date 6/2/16
 * @Time 11:11 AM
 */
public class EsJestClient {

    private static final String URI_PREFIX = "http://";

    private static final char SEPERATOR_CH = ':';

    private static JestHttpClient client;

    public static JestHttpClient buildClient(Map<String, String> esMap) {
        if (client != null) {
            return client;
        }
        String nodes = esMap.get("es.nodes");
        int port = Integer.valueOf(esMap.get("es.port"));
        List<String> serverUris = new ArrayList<String>();
        for (final String node : StringUtils.split(nodes, ',')) {
            String serverUri;
            if (node.indexOf(SEPERATOR_CH) > 0) {
                serverUri = URI_PREFIX + node;
            } else {
                serverUri = URI_PREFIX + node + ':' + port;
            }
            serverUris.add(serverUri);
        }
        return buildClient0(serverUris, esMap);
    }

    private static JestHttpClient buildClient0(Collection<String> serverUris, Map<String, String> esMap) {
        if (CollectionUtils.isEmpty(serverUris))
            throw new IllegalArgumentException("Server Uri is empty!");

        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(serverUris)
                .multiThreaded(true)
                .connTimeout(esMap.get("es.timeout") != null ? Integer.valueOf(esMap.get("es.timeout")) : 20000).readTimeout(20000)
                .build());

        client = (JestHttpClient) factory.getObject();
        return client;
    }

    public static void tearDown() throws Exception {
        client.shutdownClient();
        client = null;
    }

}
