package com.vesoft.nebula.graph.server.service;

import com.vesoft.nebula.graph.server.entity.NebulaQueryResult;

public interface NebulaGraphService {
    /**
     * connect the NebulaGraph
     *
     * @param hosts  NebulaGraph 连接地址
     * @param user   用户名
     * @param passwd 密码
     * @throws Exception
     */
    String connect(String hosts, String user, String passwd) throws Exception;

    /**
     * disconnect the session with NebulaGraph
     *
     * @param sessionId session id
     */
    void disconnect(String sessionId);


    /**
     * execute ngql
     */
    NebulaQueryResult executeNgql(String sessionId, String ngql) throws Exception;
}
