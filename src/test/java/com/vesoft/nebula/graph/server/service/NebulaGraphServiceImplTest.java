package com.vesoft.nebula.graph.server.service;

import com.vesoft.nebula.ErrorCode;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.graph.server.data.MockNebulaData;
import com.vesoft.nebula.graph.server.entity.nebulaDao.Connection;
import com.vesoft.nebula.graph.server.entity.NebulaQueryResult;
import com.vesoft.nebula.graph.server.entity.data.FrontEdge;
import com.vesoft.nebula.graph.server.entity.data.FrontPath;
import com.vesoft.nebula.graph.server.entity.data.FrontVertex;
import com.vesoft.nebula.DataSet;
import com.vesoft.nebula.Date;
import com.vesoft.nebula.DateTime;
import com.vesoft.nebula.Edge;
import com.vesoft.nebula.NList;
import com.vesoft.nebula.NMap;
import com.vesoft.nebula.NSet;
import com.vesoft.nebula.NullType;
import com.vesoft.nebula.Path;
import com.vesoft.nebula.Row;
import com.vesoft.nebula.Step;
import com.vesoft.nebula.Tag;
import com.vesoft.nebula.Time;
import com.vesoft.nebula.Value;
import com.vesoft.nebula.Vertex;
import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.Node;
import com.vesoft.nebula.client.graph.data.Relationship;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.data.ValueWrapper;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import com.vesoft.nebula.graph.ExecutionResponse;
import com.vesoft.nebula.graph.PlanDescription;
import com.vesoft.nebula.graph.server.service.impl.NebulaGraphServiceImpl;
import java.lang.reflect.Method;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class NebulaGraphServiceImplTest {
    private static Logger LOG = LoggerFactory.getLogger(NebulaGraphServiceImplTest.class);

    @Autowired
    NebulaGraphService service;

    boolean connectFlag = false;
    String sessionId = null;

    @Before
    public void setUp() throws Exception {
        MockNebulaData.createSchema();
    }

    @After
    public void tearDown() throws Exception {
    }


    @Test
    public void testConnect() {
        List<HostAddress> addresse = new ArrayList<HostAddress>();
        addresse.add(new HostAddress("192.168.8.138", 9669));
        String hosts = "192.168.8.138:9669";
        try {
            sessionId = service.connect(hosts, "root", "nebula");
            service.executeNgql(sessionId, "USE test");
        } catch (Exception e) {
            e.printStackTrace();
            assert (false);
        }
        connectFlag = true;
        assert (true);
    }


    @Test
    public void testExecuteNgql() {
        if (!connectFlag) {
            testConnect();
        }
        // test succeed execute result
        NebulaQueryResult resultSet = null;
        try {
            resultSet = service.executeNgql(sessionId, "show spaces");
        } catch (Exception e) {
            LOG.error("execute ngql error, ", e);
            assert (false);
        }
        assert (resultSet != null);
        if (resultSet.getErrorCode() != 0) {
            LOG.info(resultSet.getErrorMessage());
            assert (false);
        } else {
            LOG.info("headers: {}", resultSet.getHeaders());
            LOG.info("tables: {}", resultSet.getTables());
            assert (resultSet.getHeaders().size() == 1);
            assert (resultSet.getTables().size() == 2);
            assert (String.valueOf(resultSet.getTables().get(0).get("Name")).equals("test")
                    || String.valueOf(resultSet.getTables().get(1).get("Name")).equals("test"));
        }

        // test failed execute result
        NebulaQueryResult failedResultSet = null;
        try {
            failedResultSet = service.executeNgql(sessionId, "test ngql");
        } catch (Exception e) {
            LOG.error("execute ngql error, ", e);
            assert (false);
        }
        assert (failedResultSet.getErrorCode() != ErrorCode.SUCCEEDED.getValue());
        assert (failedResultSet.getHeaders() == null);
        assert (failedResultSet.getTables() == null);
        LOG.info(resultSet.getErrorMessage());

        // sessionId not exist
        try {
            resultSet = service.executeNgql(sessionId, "show spaces");
        } catch (IOErrorException e) {
            LOG.error("expect NullObjectException");
            assert (true);
        } catch (Exception e) {
            LOG.error("execute ngql error,", e);
            assert (false);
        }
    }

    @Test
    public void testNgqlForNode() {
        if (!connectFlag) {
            testConnect();
        }
        NebulaQueryResult resultSet = null;
        try {
            resultSet = service.executeNgql(sessionId, "FETCH PROP ON person \"1\"");
        } catch (Exception e) {
            LOG.error("execute ngql error, ", e);
            assert (false);
        }
        assert (resultSet != null);
        if (resultSet.getErrorCode() != 0) {
            LOG.error("ngql query failed, errorCode: {}, errorMsg:{}", resultSet.getErrorCode(),
                    resultSet.getErrorMessage());
            assert (false);
        } else {
            LOG.info("headers: {}", resultSet.getHeaders());
            LOG.info("tables: {}", resultSet.getTables());
            assert (resultSet.getHeaders().size() == 1);
            assert (resultSet.getTables().get(0).containsKey("vertices_"));
            assert (resultSet.getTables().get(0).containsKey("_verticesParsedList"));
        }
    }

    @Test
    public void testNgqlForEdge() {
        if (!connectFlag) {
            testConnect();
        }
        NebulaQueryResult resultSet = null;
        try {
            resultSet = service.executeNgql(sessionId, "FETCH PROP ON friend \"1\"->\"2\"");
        } catch (Exception e) {
            LOG.error("execute ngql error, ", e);
            assert (false);
        }
        assert (resultSet != null);
        if (resultSet.getErrorCode() != 0) {
            LOG.error("ngql query failed, errorCode: {}, errorMsg:{}", resultSet.getErrorCode(),
                    resultSet.getErrorMessage());
            assert (false);
        } else {
            LOG.info("headers: {}", resultSet.getHeaders());
            LOG.info("tables: {}", resultSet.getTables());
            assert (resultSet.getHeaders().size() == 1);
            assert (resultSet.getTables().get(0).containsKey("edges_"));
            assert (resultSet.getTables().get(0).containsKey("_edgesParsedList"));
        }
    }

    @Test
    public void testNgqlForPath() {
        if (!connectFlag) {
            testConnect();
        }
        NebulaQueryResult resultSet = null;
        try {
            resultSet = service.executeNgql(sessionId,
                    "MATCH p=(v)-[e:friend]->(v2) WHERE id(v) IN [\"1\"] RETURN p, v, e");
        } catch (Exception e) {
            LOG.error("execute ngql error, ", e);
            assert (false);
        }
        assert (resultSet != null);
        if (resultSet.getErrorCode() != 0) {
            LOG.error("ngql query failed, errorCode: {}, errorMsg:{}", resultSet.getErrorCode(),
                    resultSet.getErrorMessage());
            assert (false);
        } else {
            LOG.info("headers: {}", resultSet.getHeaders());
            LOG.info("tables: {}", resultSet.getTables());
            assert (resultSet.getHeaders().size() == 3);
            assert (resultSet.getTables().size() == 1);
            assert (resultSet.getTables().get(0).containsKey("p"));
            assert (resultSet.getTables().get(0).containsKey("v"));
            assert (resultSet.getTables().get(0).containsKey("e"));
        }
    }


    @Test
    public void resolveDataSet2Object() {
        NebulaGraphServiceImpl nebulaGraphService = new NebulaGraphServiceImpl();

        Class clazz = nebulaGraphService.getClass();
        try {
            Method resolveDataSet2Object = clazz.getDeclaredMethod("resolveDataSet2Object",
                    ResultSet.class);
            resolveDataSet2Object.setAccessible(true);

            // test not succeed resultSet
            ResultSet failedResultSet = getResultSet(ErrorCode.E_BAD_PERMISSION.getValue());
            List<Map<String, Object>> failedResult =
                    (List<Map<String, Object>>) resolveDataSet2Object.invoke(nebulaGraphService,
                            failedResultSet);
            assert (failedResult.size() == 0);

            // test null resultSet
            List<Map<String, Object>> nullResult =
                    (List<Map<String, Object>>) resolveDataSet2Object.invoke(nebulaGraphService,
                            null);
            assert (nullResult.size() == 0);

            // test empty resultSet
            ExecutionResponse response = new ExecutionResponse();
            response.setData(null);
            ResultSet emptyResultSet = new ResultSet(response, 0);
            List<Map<String, Object>> emptyResult =
                    (List<Map<String, Object>>) resolveDataSet2Object.invoke(nebulaGraphService,
                            emptyResultSet);
            assert (emptyResult.size() == 0);

            // test succeed resultSet
            ResultSet resultSet = getResultSet(ErrorCode.SUCCEEDED.getValue());
            List<Map<String, Object>> result =
                    (List<Map<String, Object>>) resolveDataSet2Object.invoke(nebulaGraphService,
                            resultSet);
            assert (result.size() == 1);
            Map<String, Object> frontData = result.get(0);
            assert (frontData.size() == 18);
            assert (frontData.get("col1_null").equals("__NULL__"));
            assert (((ArrayList<FrontVertex>) frontData.get("_verticesParsedList")).size() == 1);
            assert (((ArrayList<FrontEdge>) frontData.get("_edgesParsedList")).size() == 1);
            assert (((ArrayList<FrontPath>) frontData.get("_pathssParsedList")).size() == 1);
        } catch (Exception e) {
            LOG.error("reflect error,", e);
        }
    }


    @Test
    public void resolveDataSet2Table() {
        NebulaGraphServiceImpl nebulaGraphService = new NebulaGraphServiceImpl();
        // test mock data
        ResultSet resultSet = getResultSet(ErrorCode.SUCCEEDED.getValue());

        Class clazz = nebulaGraphService.getClass();
        List<List<String>> results = null;
        Method resolveDataSet2Table = null;
        try {
            resolveDataSet2Table = clazz.getDeclaredMethod("resolveDataSet2Table",
                    ResultSet.class);
            resolveDataSet2Table.setAccessible(true);
            results = (List<List<String>>) resolveDataSet2Table.invoke(nebulaGraphService,
                    resultSet);
        } catch (Exception e) {
            e.printStackTrace();
            assert (false);
        }

        for (int i = 0; i < results.size(); i++) {
            System.out.println(results.get(i));
        }
        assert (results.size() == 1);

        List<String> result = results.get(0);
        assert (result.get(0).equals("__EMPTY__"));
        assert (result.get(1).equals("__NULL__"));
        assert (result.get(2).equals("false"));
        assert (result.get(3).equals("1"));
        assert (result.get(4).equals("10.01"));
        assert (result.get(5).equals("value1"));
        assert (result.get(6).equals("[1, 2]"));
        assert (result.get(7).equals("[1, 2]"));
        assert (result.get(8).equals("{key1=1, key2=2}"));
        assert (result.get(9).equals("10:30:00.000100"));
        assert (result.get(10).equals("2020-10-10"));
        assert (result.get(11).equals("2020-10-10T10:30:00.000100"));
        assert (result.get(12).startsWith("(\"Tom\""));
        assert (result.get(13).startsWith("(\"Tom\""));
        assert (result.get(14).startsWith("(\"Tom\""));

        // test server data
        if (!connectFlag) {
            testConnect();
        }

        try {
            Connection connection = getConnection();
            connection.getNgqlQueue().offer("SHOW SPACES");
            Method exeucte = clazz.getDeclaredMethod("execute", Connection.class);
            exeucte.setAccessible(true);
            ResultSet serverResultSet = (ResultSet) exeucte.invoke(nebulaGraphService, connection);
            List<List<String>> serverResults =
                    (List<List<String>>) resolveDataSet2Table.invoke(nebulaGraphService,
                            serverResultSet);
            for (int i = 0; i < serverResults.size(); i++) {
                System.out.println(serverResults.get(i));
            }
            assert (serverResults.size() == 2);
            assert (serverResults.get(0).get(0).equals("test") || serverResults.get(1).get(0).equals("test"));
        } catch (Exception e) {
            e.printStackTrace();
            assert (false);
        }
    }


    @Test
    public void getPropertiesForMockData() {
        NebulaGraphServiceImpl nebulaGraphService = new NebulaGraphServiceImpl();

        Class clazz = nebulaGraphService.getClass();
        try {
            Method getProperties = clazz.getDeclaredMethod("getProperties", Map.class);
            getProperties.setAccessible(true);

            // mock data
            Edge edge = getEdge("src", "dst");
            Relationship relationship = new Relationship(edge);
            Map<String, Object> edgeProperties =
                    (Map<String, Object>) getProperties.invoke(nebulaGraphService,
                            relationship.properties());
            assertProperties(edgeProperties);

            Vertex vertex = getVertex("vid");
            Node node = new Node(vertex);
            Map<String, Object> vertexProperties =
                    (Map<String, Object>) getProperties.invoke(nebulaGraphService,
                            node.properties("tag1"));
            assertProperties(vertexProperties);
        } catch (Exception e) {
            e.printStackTrace();
            assert (false);
        }
    }

    @Test
    public void getPropertiesFromServer() {
        NebulaGraphServiceImpl nebulaGraphService = new NebulaGraphServiceImpl();

        Class clazz = nebulaGraphService.getClass();
        try {
            Method getProperties = clazz.getDeclaredMethod("getProperties", Map.class);
            getProperties.setAccessible(true);

            NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
            NebulaPool pool = new NebulaPool();
            List<HostAddress> addresses = new ArrayList<>();
            addresses.add(new HostAddress("192.168.8.138", 9669));
            pool.init(addresses, nebulaPoolConfig);
            Session session = pool.getSession("user", "nebula", true);
            ResultSet resultSet = session.execute("USE test; FETCH PROP ON person \"1\"");
            Node node = resultSet.rowValues(0).get("vertices_").asNode();
            Map<String, Object> vertexProperties =
                    (Map<String, Object>) getProperties.invoke(nebulaGraphService,
                            node.properties("person"));
            assert (vertexProperties.size() == 13);
            for (Map.Entry<String, Object> prop : vertexProperties.entrySet()) {
                assert !prop.getKey().equals("col1") || (String.valueOf(prop.getValue()).equals(
                        "person1"));
                assert !prop.getKey().equals("col2") || ((String.valueOf(prop.getValue()).startsWith("person1") && String.valueOf(prop.getValue()).length() == 8));
                assert !prop.getKey().equals("col3") || (((Long) prop.getValue()) == 11L);
                assert !prop.getKey().equals("col4") || (((Long) prop.getValue()) == 200L);
                assert !prop.getKey().equals("col5") || (((Long) prop.getValue()) == 1000L);
                assert !prop.getKey().equals("col6") || (((Long) prop.getValue()) == 188888L);
                Value dValue = new Value();
                dValue.setDVal(new Date((short) 2021, (byte) 1, (byte) 1));
                assert !prop.getKey().equals("col7") || ((prop.getValue()).equals((new ValueWrapper(dValue, "utf-8")).toString()));
                Value dtValue = new Value();
                dtValue.setDtVal(new DateTime((short) 2021, (byte) 1, (byte) 1, (byte) 12,
                        (byte) 0, (byte) 0, 0));
                assert !prop.getKey().equals("col8") || ((prop.getValue()).equals((new ValueWrapper(dtValue, "utf-8")).toString()));
                if (prop.getKey().equals("col9")) {
                    String objectType = prop.getValue().getClass().toString();
                    assert (objectType.substring(objectType.lastIndexOf(".") + 1).equals("Long"));
                }
                assert !prop.getKey().equals("col10") || ((Boolean) prop.getValue());
                assert !prop.getKey().equals("col11") || ((Double) prop.getValue() < 1.00001);
                assert !prop.getKey().equals("col12") || ((Double) prop.getValue() < 2.00001);
                Value timeValue = new Value();
                timeValue.setTVal(new Time((byte) 12, (byte) 1, (byte) 1, 0));
                assert !prop.getKey().equals("col13") || ((prop.getValue()).equals((new ValueWrapper(timeValue, "utf-8")).toString()));
            }
        } catch (Exception e) {
            e.printStackTrace();
            assert (false);
        }
    }

    @Test
    public void testDisconnect() {
        try {
            testConnect();
            service.disconnect(sessionId);
        } catch (Exception e) {
            LOG.error("failed to disconnect, ", e);
            assert (false);
        }
    }

    /**
     * mock {@link ResultSet}
     */
    private ResultSet getResultSet(int code) {
        ExecutionResponse resp = new ExecutionResponse();
        resp.error_code = ErrorCode.findByValue(code);
        resp.error_msg = "test".getBytes();
        resp.comment = "test_comment".getBytes();
        resp.latency_in_us = 1000;
        resp.plan_desc = new PlanDescription();
        resp.space_name = "test_space".getBytes();
        resp.data = getDateset();
        ResultSet resultSet = new ResultSet(resp, 0);
        return resultSet;
    }


    /**
     * mock {@link DataSet}
     */
    private DataSet getDateset() {
        final ArrayList<Value> list = new ArrayList<>();
        list.add(new Value(Value.IVAL, 1L));
        list.add(new Value(Value.IVAL, 2L));
        final HashSet<Value> set = new HashSet<>();
        set.add(new Value(Value.IVAL, 1L));
        set.add(new Value(Value.IVAL, 2L));
        final HashMap<byte[], Value> map = new HashMap<>();
        map.put("key1".getBytes(), new Value(Value.IVAL, 1L));
        map.put("key2".getBytes(), new Value(Value.IVAL, 2L));
        final Row row = new Row(Arrays.asList(
                new Value(),
                new Value(Value.NVAL, NullType.__NULL__),
                new Value(Value.BVAL, false),
                new Value(Value.IVAL, 1L),
                new Value(Value.FVAL, 10.01),
                new Value(Value.SVAL, "value1".getBytes()),
                new Value(Value.LVAL, new NList(list)),
                new Value(Value.UVAL, new NSet(set)),
                new Value(Value.MVAL, new NMap(map)),
                new Value(Value.TVAL, new Time((byte) 10, (byte) 30, (byte) 0, 100)),
                new Value(Value.DVAL, new Date((short) 2020, (byte) 10, (byte) 10)),
                new Value(Value.DTVAL,
                        new DateTime((short) 2020, (byte) 10,
                                (byte) 10, (byte) 10, (byte) 30, (byte) 0, 100)),
                new Value(Value.VVAL, getVertex("Tom")),
                new Value(Value.EVAL, getEdge("Tom", "Lily")),
                new Value(Value.PVAL, getPath("Tom", 3))));
        final List<byte[]> columnNames = Arrays.asList(
                "col0_empty".getBytes(),
                "col1_null".getBytes(),
                "col2_bool".getBytes(),
                "col3_int".getBytes(),
                "col4_double".getBytes(),
                "col5_string".getBytes(),
                "col6_list".getBytes(),
                "col7_set".getBytes(),
                "col8_map".getBytes(),
                "col9_time".getBytes(),
                "col10_date".getBytes(),
                "col11_datetime".getBytes(),
                "col12_vertex".getBytes(),
                "col13_edge".getBytes(),
                "col14_path".getBytes());
        return new DataSet(columnNames, Collections.singletonList(row));
    }

    /**
     * mock {@link Vertex}
     */
    private Vertex getVertex(String vid) {
        List<Tag> tags = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            Map<byte[], Value> props = mockProperties();
            Tag tag = new Tag(String.format("tag%d", i).getBytes(), props);
            tags.add(tag);
        }
        return new Vertex(new Value(Value.SVAL, vid.getBytes()), tags);
    }

    /**
     * mock {@link Edge}
     */
    private Edge getEdge(String srcId, String dstId) {
        Map<byte[], Value> props = mockProperties();
        return new Edge(new Value(Value.SVAL, srcId.getBytes()),
                new Value(Value.SVAL, dstId.getBytes()),
                1,
                "classmate".getBytes(),
                100,
                props);
    }

    /**
     * mock {@link Path}
     */
    private Path getPath(String startId, int stepsNum) {
        List<Step> steps = new ArrayList<>();
        for (int i = 0; i < stepsNum; i++) {
            Map<byte[], Value> props = new HashMap<>();
            for (int j = 0; j < 5; j++) {
                Value value = new Value();
                value.setIVal(j);
                props.put(String.format("prop%d", j).getBytes(), value);
            }
            int type = 1;
            if (i % 2 != 0) {
                type = -1;
            }
            Vertex dstId = getVertex(String.format("vertex%d", i));
            steps.add(new Step(getVertex(String.format("vertex%d", i)),
                    type, "classmate".getBytes(), 100, props));
        }
        return new Path(getVertex(startId), steps);
    }


    private Map<byte[], Value> mockProperties() {
        Map<byte[], Value> props = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            Value value = new Value();
            if (i == 0) {
                value.setIVal(i);
            }
            if (i == 1) {
                value.setSVal((String.valueOf(i)).getBytes());
            }
            if (i == 2) {
                value.setFVal(Double.parseDouble(String.valueOf(i)));
            }
            if (i == 3) {
                value.setBVal(true);
            }
            if (i == 4) {
                value.setDVal(new Date((short) 2021, (byte) 5, (byte) 5));
            }
            if (i == 5) {
                value.setTVal(new Time((byte) 12, (byte) 12, (byte) 12, 1000));
            }
            if (i == 6) {
                value.setDtVal(new DateTime((short) 2021, (byte) 5, (byte) 6, (byte) 1, (byte) 30
                        , (byte) 20, 1));
            }
            if (i == 7) {
                List<Value> list = new ArrayList<>();
                list.add(new Value(Value.IVAL, 1L));
                list.add(new Value(Value.IVAL, 2L));
                value.setLVal(new NList(list));
            }
            if (i == 8) {
                Set<Value> set = new HashSet<>();
                set.add(new Value(Value.IVAL, 1L));
                set.add(new Value(Value.IVAL, 2L));
                value.setUVal(new NSet(set));
            }
            if (i == 9) {
                Map<byte[], Value> map = new HashMap<>();
                map.put("key1".getBytes(), new Value(Value.IVAL, 1L));
                map.put("key2".getBytes(), new Value(Value.IVAL, 2L));
                value.setMVal(new NMap(map));
            }
            props.put(String.format("prop%d", i).getBytes(), value);
        }
        return props;
    }


    /**
     * assert properties
     */
    private void assertProperties(Map<String, Object> properties) {
        for (Map.Entry<String, Object> prop : properties.entrySet()) {
            String propType = prop.getValue().getClass().toString();
            if (prop.getKey().equals(String.format("prop%d", 0))) {
                assert (propType.substring(propType.lastIndexOf(".") + 1).equals("Long"));
                assert (prop.getValue().toString().equals("0"));
            }
            if (prop.getKey().equals(String.format("prop%d", 1))) {
                assert (propType.substring(propType.lastIndexOf(".") + 1).equals("String"));
                assert (prop.getValue().toString().equals("1"));
            }
            if (prop.getKey().equals(String.format("prop%d", 2))) {
                assert (propType.substring(propType.lastIndexOf(".") + 1).equals("Double"));
                assert (Double.parseDouble(String.valueOf(prop.getValue())) < 2.0001);
            }
            if (prop.getKey().equals(String.format("prop%d", 3))) {
                assert (propType.substring(propType.lastIndexOf(".") + 1).equals("Boolean"));
                assert (Boolean.parseBoolean(String.valueOf(prop.getValue())));
            }
            if (prop.getKey().equals(String.format("prop%d", 4))) {
                assert (propType.substring(propType.lastIndexOf(".") + 1).equals("String"));
                Value value = new Value();
                value.setDVal(new Date((short) 2021, (byte) 5, (byte) 5));
                assert ((prop.getValue()).equals((new ValueWrapper(value, "utf-8")).toString()));
            }
            if (prop.getKey().equals(String.format("prop%d", 5))) {
                assert (propType.substring(propType.lastIndexOf(".") + 1).equals("String"));
                Value value = new Value();
                value.setTVal(new Time((byte) 12, (byte) 12, (byte) 12, 1000));
                assert ((prop.getValue()).equals((new ValueWrapper(value, "utc-8")).toString()));
            }
            if (prop.getKey().equals(String.format("prop%d", 6))) {
                assert (propType.substring(propType.lastIndexOf(".") + 1).equals("String"));
                Value value = new Value();
                value.setDtVal(new DateTime((short) 2021, (byte) 5, (byte) 6, (byte) 1, (byte) 30
                        , (byte) 20, 1));
                assert ((prop.getValue()).equals(new ValueWrapper(value, "utc-8").toString()));
            }
            if (prop.getKey().equals(String.format("prop%d", 7))) {
                assert (propType.substring(propType.lastIndexOf(".") + 1).equals("ArrayList"));
                List<ValueWrapper> values = (List<ValueWrapper>) prop.getValue();
                assert (values.get(0).asLong() == 1L || values.get(0).asLong() == 2L);
                assert (values.get(1).asLong() == 1L || values.get(1).asLong() == 2L);
            }
            if (prop.getKey().equals(String.format("prop%d", 8))) {
                assert (propType.substring(propType.lastIndexOf(".") + 1).equals("HashSet"));
                Set<ValueWrapper> values = (Set<ValueWrapper>) prop.getValue();
                Set<Long> longValues = new HashSet<>();
                for (ValueWrapper v : values) {
                    longValues.add(v.asLong());
                }
                assert (longValues.contains(1L));
                assert (longValues.contains(2L));
            }
            if (prop.getKey().equals(String.format("prop%d", 9))) {
                assert (propType.substring(propType.lastIndexOf(".") + 1).equals("HashMap"));
                assert (((Map<String, ValueWrapper>) prop.getValue()).keySet().contains("key1"));
                assert (((Map<String, ValueWrapper>) prop.getValue()).keySet().contains("key2"));
                assert (((Map<String, ValueWrapper>) prop.getValue()).get("key1").asLong() == 1L);
                assert (((Map<String, ValueWrapper>) prop.getValue()).get("key2").asLong() == 2L);
            }
        }
    }

    private Connection getConnection() {
        NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
        NebulaPool pool = new NebulaPool();
        List<HostAddress> addresses = new ArrayList<>();
        addresses.add(new HostAddress("192.168.8.138", 9669));
        Session session = null;
        try {
            pool.init(addresses, nebulaPoolConfig);
            session = pool.getSession("user", "nebula", true);
        } catch (UnknownHostException | NotValidConnectionException | IOErrorException | AuthFailedException | ClientServerIncompatibleException e) {
            LOG.error("failed to get session", e);
            assert (false);
        }

        Connection connection = new Connection(sessionId, pool, session, "user", "nebula");
        return connection;
    }


}
