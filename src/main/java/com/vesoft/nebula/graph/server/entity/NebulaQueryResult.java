package com.vesoft.nebula.graph.server.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NebulaQueryResult implements Serializable {

    private List<String> headers = new ArrayList<>();

    private List<Map<String, Object>> tables = new ArrayList<>();

    /**
     * latency of the query execute time, unit: us
     */
    private long timeCost;

    private int errorCode;

    private String errorMessage;


    public List<String> getHeaders() {
        return headers;
    }

    public void setHeaders(List<String> headers) {
        this.headers = headers;
    }

    public List<Map<String, Object>> getTables() {
        return tables;
    }

    public void setTables(List<Map<String, Object>> tables) {
        this.tables = tables;
    }

    public long getTimeCost() {
        return timeCost;
    }

    public void setTimeCost(long timeCost) {
        this.timeCost = timeCost;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Override
    public String toString() {
        return "NebulaQueryResult{" +
                "header=" + headers +
                ", tables=" + tables +
                ", timeCost=" + timeCost +
                ", errorCode=" + errorCode +
                ", errorMessage='" + errorMessage + '\'' +
                '}';
    }
}
