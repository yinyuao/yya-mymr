package com.ksc.wordcount.thrift.impl.server;

import com.ksc.wordcount.driver.DriverEnv;
import com.ksc.wordcount.thrift.UrlTopNService;
import com.ksc.wordcount.thrift.impl.UrlTopNServiceImpl;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;

public class UrlTopNServer {
    public void start() {
        try {
            UrlTopNServiceImpl handler = new UrlTopNServiceImpl();
            UrlTopNService.Processor<UrlTopNServiceImpl> processor = new UrlTopNService.Processor<>(handler);

            TServerSocket serverTransport = new TServerSocket(DriverEnv.thriftPort);
            TServer server = new TSimpleServer(new TServer.Args(serverTransport).processor(processor));

            System.out.println("Starting the server...");
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
