package com.vesoft.nebula.graph.server.entity;

import java.io.Serializable;

public class NebulaQueryResponse implements Serializable {

    /** error code */
    private int code;

    /** error message */
    private String message;

    private NebulaQueryResult data;

    public NebulaQueryResponse(){
        code = ErrorCode.SUCCESS.getErrorCode();
        message = ErrorCode.SUCCESS.getErrorMsg();
    }

    public void setResp(ErrorCode errorCode){
        code = errorCode.getErrorCode();
        message = errorCode.getErrorMsg();
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public NebulaQueryResult getData() {
        return data;
    }

    public void setData(NebulaQueryResult data) {
        this.data = data;
    }


}
