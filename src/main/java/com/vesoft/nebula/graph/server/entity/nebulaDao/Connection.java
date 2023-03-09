package com.vesoft.nebula.graph.server.entity.nebulaDao;

import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import java.io.Serializable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Connection implements Serializable {

    private String sessionId;

    private NebulaPool pool;

    private Session session;

    private BlockingQueue<String> ngqlQueue = new ArrayBlockingQueue<>(1);

    private String user;

    private String passwd;

    public Connection(String sessionId, NebulaPool pool, Session session, String user, String passwd){
        this.sessionId = sessionId;
        this.pool = pool;
        this.session = session;
        this.user = user;
        this.passwd = passwd;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public NebulaPool getPool() {
        return pool;
    }

    public void setPool(NebulaPool pool) {
        this.pool = pool;
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public BlockingQueue<String> getNgqlQueue() {
        return ngqlQueue;
    }

    public void setNgqlQueue(BlockingQueue<String> ngqlQueue) {
        this.ngqlQueue = ngqlQueue;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPasswd() {
        return passwd;
    }

    public void setPasswd(String passwd) {
        this.passwd = passwd;
    }

    public void release(){
        ngqlQueue.clear();
        session.release();
        pool.close();
    }

    @Override
    public String toString() {
        return "Connection{" +
                "sessionId='" + sessionId + '\'' +
                ", pool=" + pool +
                ", session=" + session +
                ", ngqlQueue=" + ngqlQueue +
                ", user='" + user + '\'' +
                ", passwd='" + passwd + '\'' +
                '}';
    }
}
