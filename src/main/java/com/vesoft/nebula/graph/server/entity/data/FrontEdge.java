package com.vesoft.nebula.graph.server.entity.data;

import java.io.Serializable;
import java.util.Map;

public class FrontEdge implements Serializable {

    private static final long serialVersionUID = 8028510134121921500L;

    private Map<String, Object> properties;
    private String type = "edge";
    private String edgeName;
    private Object srcID;
    private Object dstID;
    private Long rank;

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getEdgeName() {
        return edgeName;
    }

    public void setEdgeName(String edgeName) {
        this.edgeName = edgeName;
    }

    public Object getSrcID() {
        return srcID;
    }

    public void setSrcID(Object srcID) {
        this.srcID = srcID;
    }

    public Object getDstID() {
        return dstID;
    }

    public void setDstID(Object dstID) {
        this.dstID = dstID;
    }

    public Long getRank() {
        return rank;
    }

    public void setRank(Long rank) {
        this.rank = rank;
    }

    @Override
    public String toString() {
        return "FrontEdge{" +
                "properties=" + properties +
                ", type='" + type + '\'' +
                ", edgeName='" + edgeName + '\'' +
                ", srcID='" + srcID + '\'' +
                ", dstID='" + dstID + '\'' +
                ", rank=" + rank +
                '}';
    }
}
