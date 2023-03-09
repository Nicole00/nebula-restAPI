package com.vesoft.nebula.graph.server.entity.data;

import java.io.Serializable;
import java.util.List;

public class FrontPath implements Serializable {

    private static final long serialVersionUID = -8764244947740237118L;


    private List<FrontEdge> relationships;
    private String type = "path";

    public List<FrontEdge> getRelationships() {
        return relationships;
    }

    public void setRelationships(List<FrontEdge> relationships) {
        this.relationships = relationships;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "FrontPath{" +
                "relationships=" + relationships +
                ", type='" + type + '\'' +
                '}';
    }
}
