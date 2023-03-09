package com.vesoft.nebula.graph.server.entity.data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class FrontVertex implements Serializable {

    private static final long serialVersionUID = -798696590672307580L;

    private Object vid;
    private String type = "vertex";
    private List<String> tags;
    private Map<String, Map<String, Object>> properties;

    public Object getVid() {
        return vid;
    }

    public void setVid(Object vid) {
        this.vid = vid;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public Map<String, Map<String, Object>> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Map<String, Object>> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "FrontVertex{" +
                "vid='" + vid + '\'' +
                ", type='" + type + '\'' +
                ", tags=" + tags +
                ", properties=" + properties +
                '}';
    }
}
