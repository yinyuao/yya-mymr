package com.ksc.wordcount.shuffle;

import java.io.Serializable;

public class ShuffleBlockId implements Serializable {

    String host;
    int port;
    String shuffleBaseDir;
    String shuffleId;
    String stageId;
    String applicationId;
    int mapId;
    int reduceId;


    public ShuffleBlockId(String shuffleBaseDir, String applicationId, String shuffleId, String stageId, int mapId, int reduceId) {
        this.shuffleBaseDir = shuffleBaseDir;
        this.applicationId = applicationId;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
        this.reduceId = reduceId;
        this.stageId = stageId;
    }

    public void setHostAndPort(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getReduceId() {
        return reduceId;
    }


    public String name() {
        return "shuffle_" + shuffleId + "_" + stageId + "_" + mapId + "_" + reduceId;
    }

    public String getShufflePath() {
        return getShuffleParentPath() + "/" + name() + ".data";
    }

    public String getShuffleParentPath() {
        return shuffleBaseDir + "/" + applicationId;
    }
}
