package com.dark.channel.util;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;

/**
 * Created by dark on 1/09/16.
 */
public class EsClientFactory {

    private static TransportClient client;

    private EsClientFactory() {
        try {
            init();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static Client getEsTransportClient(){
        if(client == null){
            synchronized (EsClientFactory.class) {
                if(client == null) {
                    new EsClientFactory();
                }
            }
        }
        return client;
    }

    public void init() throws Exception {

        final Settings settings = Settings.settingsBuilder()
//                .put("cluster.name", "dev-es-cluster")
                .put("client.transport.ping_timeout", "2m")
                .build();

        client = TransportClient.builder().settings(settings).build();
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));

    }
    public static void main(String[] args){
        new EsClientFactory();
        if(client != null){
            System.out.println("Ok");
        }
    }
}
