package com.ksc.wordcount.thrift;

import com.ksc.wordcount.driver.DriverEnv;
import com.ksc.wordcount.thrift.UrlTopNService;
import com.ksc.wordcount.thrift.impl.UrlTopNServiceImpl;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

public class UrlTopNServer {
    public static void start() {

        try {
            UrlTopNServiceImpl handler = new UrlTopNServiceImpl();
            UrlTopNService.Processor<UrlTopNService.Iface> processor = new UrlTopNService.Processor<>(handler);

            TServerSocket serverTransport = new TServerSocket(5151);
            TServer server = new TSimpleServer(new TServer.Args(serverTransport).processor(processor));

            server.serve();
        } catch (Exception e) {
//            System.out.println(e);
        }

    }
}
