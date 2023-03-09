package com.vesoft.nebula.graph.server.service.impl;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.Node;
import com.vesoft.nebula.client.graph.data.PathWrapper;
import com.vesoft.nebula.client.graph.data.Relationship;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.data.ValueWrapper;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import com.vesoft.nebula.graph.server.entity.NebulaQueryResult;
import com.vesoft.nebula.graph.server.entity.data.FrontEdge;
import com.vesoft.nebula.graph.server.entity.data.FrontPath;
import com.vesoft.nebula.graph.server.entity.data.FrontVertex;
import com.vesoft.nebula.graph.server.entity.nebulaDao.Connection;
import com.vesoft.nebula.graph.server.exceptions.ConnectionException;
import com.vesoft.nebula.graph.server.exceptions.NullObjectException;
import com.vesoft.nebula.graph.server.service.NebulaGraphService;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

public class NebulaGraphServiceImpl implements NebulaGraphService {
    private static Logger LOG = LoggerFactory.getLogger(NebulaGraphServiceImpl.class);

    private static final ConcurrentHashMap<String, Connection> connections =
            new ConcurrentHashMap();

    @Value("${nebula.graph.maxConnSize}")
    private int maxConnSize = 10;

    @Value("${nebula.graph.minConnSize}")
    private int minConnSize = 1;

    @Value("${nebula.graph.timeout}")
    private int timeout = 0;

    @Value("${nebula.graph.idleTime}")
    private int idleTime = 60 * 60 * 24 * 1000;

    private String timeZoneOffset;

    @Override
    public String connect(String hosts, String user, String passwd) throws Exception {
        LOG.info("NebulaGraphService.connect, parameters:hosts={},user={},passwd={}", hosts, user
                , passwd);
        List<HostAddress> addresses = new ArrayList<>();
        for (String host : hosts.split(",")) {
            String ip = host.split(":")[0];
            int port = Integer.parseInt(host.split(":")[1]);
            addresses.add(new HostAddress(ip, port));
        }
        NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
        nebulaPoolConfig.setMaxConnSize(maxConnSize);
        nebulaPoolConfig.setMinConnSize(minConnSize);
        nebulaPoolConfig.setTimeout(timeout);
        nebulaPoolConfig.setIdleTime(idleTime);
        NebulaPool pool = new NebulaPool();
        pool.init(addresses, nebulaPoolConfig);

        Session session = getSession(pool, user, passwd);
        String sessionId = UUID.randomUUID().toString();
        Connection connection = new Connection(sessionId, pool, session, user, passwd);
        connections.put(sessionId, connection);

        return sessionId;
    }

    @Override
    public void disconnect(String sessionId) {
        if (connections.containsKey(sessionId)) {
            Connection connection = connections.get(sessionId);
            connection.release();
            connections.remove(sessionId);
        }
    }


    @Override
    public NebulaQueryResult executeNgql(String sessionId, String ngql) throws Exception {
        Connection connection = getConnection(sessionId);
        Boolean offerNgql = connection.getNgqlQueue().offer(ngql);
        if (!offerNgql) {
            LOG.warn("ngql {} request lost, ", ngql);
        }
        ResultSet resultSet = execute(connection);

        if (resultSet == null) {
            LOG.error("execute ngql failed for null response");
            throw new Exception("null response");
        }
        NebulaQueryResult results = new NebulaQueryResult();
        if (!resultSet.isSucceeded()) {
            LOG.error("execute ngql failed for " + resultSet.getErrorMessage());
            results.setHeaders(null);
            results.setTables(null);
        } else {
            List<Map<String, Object>> queryResult = resolveDataSet2Object(resultSet);
            if (!resultSet.isEmpty()) {
                results.setHeaders(resultSet.getColumnNames());
            }
            results.setTables(queryResult);
        }
        results.setErrorCode(resultSet.getErrorCode());
        results.setErrorMessage(resultSet.getErrorMessage());
        results.setTimeCost(resultSet.getLatency());
        return results;
    }


    /**
     * get session from pool to execute NebulaGraph
     *
     * @return Session
     */
    private Session getSession(NebulaPool pool, String user, String passwd) throws NullObjectException,
            ConnectionException {
        Session session;
        try {
            session = pool.getSession(user, passwd, true);
        } catch (Exception e) {
            LOG.error("failed to get nebula graph connection.", e);
            throw new ConnectionException("failed to get nebula graph connection.");
        }
        return session;
    }


    /**
     * get connection
     */
    private Connection getConnection(String sessionId) throws NullObjectException {
        if (!connections.containsKey(sessionId)) {
            LOG.error("sessionId has no connection, reconnect Nebula Graph");
            throw new NullObjectException("connection refused, please reconnect Nebula Graph");
        }
        Connection connection = connections.get(sessionId);
        if (connection == null) {
            LOG.error("connection is null, reconnect Nebula Graph");
            throw new NullObjectException("connection refused, please reconnect Nebula Graph");
        }
        return connection;
    }


    /**
     * execute graph query
     *
     * @param connection connection info
     * @return ResultSet
     */
    private ResultSet execute(Connection connection) throws Exception {

        Session session = connection.getSession();
        if (!session.ping()) {
            LOG.error("an existing connection was forcibly closed ");
            throw new ConnectionException("an existing connection was forcibly closed");
        }
        String ngql = connection.getNgqlQueue().poll();
        LOG.info("exeucte ngql: {}", ngql);
        ResultSet resultSet;
        try {
            resultSet = session.execute(ngql);
        } catch (IOErrorException e) {
            LOG.error(String.format("execute ngql %s happens IOError Exception, ", ngql), e);
            throw e;
        } catch (Exception e) {
            LOG.error(String.format("execute ngql %s happens Unsupported encoding Exception, ",
                    ngql), e);
            throw e;
        }
        return resultSet;
    }

    /**
     * resolve java client dataset to table
     *
     * @param resultSet NGQL execute result, see{@link ResultSet}
     * @return List
     */
    private List<List<String>> resolveDataSet2Table(ResultSet resultSet) {
        List<List<String>> results = new ArrayList<>();
        if (resultSet == null || resultSet.isEmpty() || !resultSet.isSucceeded()) {
            return results;
        }
        int size = resultSet.rowsSize();
        int i = 0;
        while (i < size) {
            List<String> rowValues = new ArrayList<>();
            for (ValueWrapper value : resultSet.rowValues(i)) {
                if (value.isString()) {
                    String valueStr = value.toString();
                    rowValues.add(valueStr.substring(1, valueStr.length() - 1));
                } else {
                    rowValues.add(value.toString());
                }
            }
            results.add(rowValues);
            i++;
        }
        return results;
    }

    /**
     * resolve java client dataset to Map: {col_name: col_value}
     *
     * @param resultSet NGQL execute result, see{@link ResultSet}
     * @return List
     */
    private List<Map<String, Object>> resolveDataSet2Object(ResultSet resultSet) throws UnsupportedEncodingException {
        List<Map<String, Object>> results = new ArrayList<>();
        if (resultSet == null || resultSet.isEmpty() || !resultSet.isSucceeded()) {
            return results;
        }

        List<String> columns = resultSet.getColumnNames();
        int rowSize = resultSet.rowsSize();
        int colSize = columns.size();

        for (int i = 0; i < rowSize; i++) {
            Map<String, Object> rowValue = new HashMap<>();
            List<FrontVertex> verticesParsedList = null;
            List<FrontEdge> edgesParsedList = null;
            List<FrontPath> pathsParsedList = null;

            ResultSet.Record row =
                    resultSet.rowValues(i);
            Iterator<ValueWrapper> valueIter = row.iterator();
            for (int index = 0; index < colSize; index++) {
                ValueWrapper valueWrapper = valueIter.next();
                // 封装console显示结果
                String key = columns.get(index);
                String value;
                if (valueWrapper.isString()) {
                    value = valueWrapper.asString();
                } else {
                    value = valueWrapper.toString();
                }
                rowValue.put(key, value);
                // 封装可视化显示结果
                if (valueWrapper.isVertex()) {
                    Node node = valueWrapper.asNode();
                    FrontVertex frontVertex = new FrontVertex();
                    frontVertex.setTags(node.tagNames());
                    frontVertex.setVid(getNebulaId(node.getId()));
                    Map<String, Map<String, Object>> vertexProperties = new HashMap<>();
                    for (String tag : node.tagNames()) {
                        Map<String, Object> properties = getProperties(node.properties(tag));
                        vertexProperties.put(tag, properties);
                    }
                    frontVertex.setProperties(vertexProperties);
                    if (verticesParsedList == null) {
                        verticesParsedList = new ArrayList<>();
                    }
                    verticesParsedList.add(frontVertex);
                }
                if (valueWrapper.isEdge()) {
                    Relationship relationship = valueWrapper.asRelationship();
                    FrontEdge frontEdge = new FrontEdge();
                    frontEdge.setEdgeName(relationship.edgeName());
                    frontEdge.setSrcID(getNebulaId(relationship.srcId()));
                    frontEdge.setDstID(getNebulaId(relationship.dstId()));
                    frontEdge.setRank(relationship.ranking());
                    Map<String, Object> properties = getProperties(relationship.properties());
                    frontEdge.setProperties(properties);
                    if (edgesParsedList == null) {
                        edgesParsedList = new ArrayList<>();
                    }
                    edgesParsedList.add(frontEdge);
                }
                if (valueWrapper.isPath()) {
                    PathWrapper path = valueWrapper.asPath();
                    FrontPath frontPath = new FrontPath();
                    List<Relationship> relationShipList = path.getRelationships();
                    List<FrontEdge> edges = new ArrayList<>();
                    for (Relationship relationship : relationShipList) {
                        FrontEdge frontEdge = new FrontEdge();
                        frontEdge.setEdgeName(relationship.edgeName());
                        frontEdge.setSrcID(getNebulaId(relationship.srcId()));
                        frontEdge.setDstID(getNebulaId(relationship.dstId()));
                        frontEdge.setRank(relationship.ranking());
                        edges.add(frontEdge);
                    }
                    frontPath.setRelationships(edges);
                    if (pathsParsedList == null) {
                        pathsParsedList = new ArrayList<>();
                    }
                    pathsParsedList.add(frontPath);
                }
            }
            if (verticesParsedList != null) {
                rowValue.put("_verticesParsedList", verticesParsedList);
            }
            if (edgesParsedList != null) {
                rowValue.put("_edgesParsedList", edgesParsedList);
            }
            if (pathsParsedList != null) {
                rowValue.put("_pathsParsedList", pathsParsedList);
            }
            results.add(rowValue);
        }
        return results;
    }

    /**
     * resolve nebula vid, srcID, dstID
     *
     * @param valueWrapper see{@link ValueWrapper}
     * @return Object  id: String or Long
     */
    private Object getNebulaId(ValueWrapper valueWrapper) throws UnsupportedEncodingException {
        if (valueWrapper.isLong()) {
            return valueWrapper.asLong();
        } else {
            return valueWrapper.asString();
        }
    }

    /**
     * resolve nebula properties
     *
     * @param properties Nebula vertex or edge properties
     * @return Map {prop_name, prop_value}
     */
    private Map<String, Object> getProperties(Map<String, ValueWrapper> properties) throws UnsupportedEncodingException {
        Map<String, Object> nebulaProperties = new HashMap<>();
        for (Map.Entry<String, ValueWrapper> kv : properties.entrySet()) {
            Object value = null;
            ValueWrapper valueWrapper = kv.getValue();
            if (valueWrapper.isLong()) {
                value = valueWrapper.asLong();
            }
            if (valueWrapper.isDouble()) {
                value = valueWrapper.asDouble();
            }
            if (valueWrapper.isBoolean()) {
                value = valueWrapper.asBoolean();
            }
            if (valueWrapper.isString()) {
                value = valueWrapper.asString();
            }
            if (valueWrapper.isDate()) {
                value = valueWrapper.asDate().toString();
            }
            if (valueWrapper.isTime()) {
                value = valueWrapper.asTime().toString();
            }
            if (valueWrapper.isDateTime()) {
                value = valueWrapper.asDateTime().toString();
            }
            if (valueWrapper.isEmpty()) {
                value = "__EMPTY__";
            }
            if (valueWrapper.isList()) {
                value = valueWrapper.asList();
            }
            if (valueWrapper.isSet()) {
                value = valueWrapper.asSet();
            }
            if (valueWrapper.isMap()) {
                value = valueWrapper.asMap();
            }
            if (valueWrapper.isNull()) {
                value = valueWrapper.asNull().toString();
            }
            nebulaProperties.put(kv.getKey(), value);
        }
        return nebulaProperties;
    }

}
