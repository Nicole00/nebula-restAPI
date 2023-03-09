package com.vesoft.nebula.graph.server.entity;

import java.io.Serializable;

public class NebulaQueryRequest implements Serializable {
    private String gql;

    public String getGql() {
        return gql;
    }

    public void setGql(String gql) {
        this.gql = gql;
    }

    @Override
    public String toString() {
        return "NgqlRequest{" +
                "ngql='" + gql + '\'' +
                '}';
    }
}
