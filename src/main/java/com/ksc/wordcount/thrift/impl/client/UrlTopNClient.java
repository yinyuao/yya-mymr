package com.ksc.wordcount.thrift.impl.client;

import com.ksc.wordcount.driver.DriverEnv;
import com.ksc.wordcount.thrift.UrlTopNService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class UrlTopNClient {

    static UrlTopNService.Client client;

    public static UrlTopNService.Client getUrlTopNClient() {
        if (client != null) {
            return client;
        }
        try {
            TTransport transport = new TSocket("127.0.0.1", DriverEnv.thriftPort);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            client = new UrlTopNService.Client(protocol);
        } catch (TTransportException e) {
            throw new RuntimeException(e);
        }
        return client;
    }
}
