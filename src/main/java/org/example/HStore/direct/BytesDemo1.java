package org.example.HStore.direct;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.driver.GraphManager;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.driver.SchemaManager;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.serializer.direct.HBaseSerializer;
import org.apache.hugegraph.serializer.direct.HStoreSerializer;
import org.apache.hugegraph.serializer.direct.RocksDBSerializer;
import org.apache.hugegraph.serializer.direct.util.IdGenerator;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;
import org.example.HStore.HStoreSessionImpl;

/**
 * This class is a demo for Rocksdb/HBase/HStore put(rowkey, values) which use Client-Side's graph struct
 * And we don't need to construct the graph element, just use it and transfer them to bytes array
 * instead of json format
 */
public class BytesDemo1 {

    static HugeClient client;
    boolean bypassServer = false;
    static SchemaManager schema;
    HStoreSerializer hStoreSerializer;
    static HStoreSessionImpl HStore;
//    RocksDBSerializer ser;
//    HBaseSerializer HBaseSer;



    public BytesDemo1(){
        HStore = new HStoreSessionImpl();
        client = HugeClient.builder("http://localhost:8080", "hugegraph").build();
        schema = client.schema();
        hStoreSerializer = new HStoreSerializer(client); // KV 点边序列化逻辑

    }


    public static void main(String[] args) throws PDException {
        String deleteActionType = "delete";
        String putActionType = "put";

        BytesDemo1 ins = new BytesDemo1();


        //ins.createSchema(schema);//创建Schema

        ins.CRUDGraphElements(putActionType);;//写入点边数据

        System.out.println("====scan vertices====");
        HStore.scan("vertices");//查询点边数据

        System.out.println("====scan out edges====");
        HStore.scan("out_edge");//查询点边数据

        System.out.println("====get store partitions====");
        HStore.getPartitions();
//
//
//        System.out.println("====scan in edges====");
//        HStore.scan("in_edge");//查询点边数据




        client.close();//关闭客户端链接
    }



    private void createSchema(SchemaManager schema){

        schema.propertyKey("name").asText().ifNotExist().create();
        schema.propertyKey("age").asInt().ifNotExist().create();
        schema.propertyKey("lang").asText().ifNotExist().create();
        schema.propertyKey("date").asText().ifNotExist().create();
        schema.propertyKey("price").asText().ifNotExist().create();

        schema.vertexLabel("person")
                .properties("name", "age")
                .useCustomizeStringId()
                .enableLabelIndex(false)
                .ifNotExist()
                .create();

        schema.vertexLabel("personB")
                .properties("price")
                .nullableKeys("price")
                .useCustomizeNumberId()
                .enableLabelIndex(false)
                .ifNotExist()
                .create();

        schema.vertexLabel("software")
                .properties("name", "lang", "price")
                .useCustomizeStringId()
                .enableLabelIndex(false)
                .ifNotExist()
                .create();

        schema.edgeLabel("knows")
                .link("person", "person")
                .properties("date")
                .enableLabelIndex(false)
                .ifNotExist()
                .create();

        schema.edgeLabel("created")
                .link("person", "software")
                .properties("date")
                .enableLabelIndex(false)
                .ifNotExist()
                .create();


    }

    private void CRUDGraphElements(String actionType) {
        GraphManager graph = client.graph();
        // construct some vertexes & edges
        Vertex peter = new Vertex("person");
        peter.property("name", "peter");
        peter.property("age", 35);
        peter.id("peter");

        Vertex lop = new Vertex("software");
        lop.property("name", "lop");
        lop.property("lang", "java");
        lop.property("price", "328");
        lop.id("lop");

        Vertex vadasB = new Vertex("personB");
        vadasB.property("price", "120.2");
        vadasB.id(12345);

        Edge peterCreateLop = new Edge("created").source(peter).target(lop)
                .property("date", "2017-03-24");

        List<Vertex> vertices = new ArrayList<Vertex>() {{
            add(peter);
            add(lop);
            add(vadasB);
        }};

        List<Edge> edges = new ArrayList<Edge>() {{
            add(peterCreateLop);
        }};

        // Old way: encode to json then send to server
        if (bypassServer) {
            if(actionType.equals("put")){
                writeDirectly(vertices, edges);
            }else if(actionType.equals("delete")){
                deleteDirectly(vertices, edges);
            }
        } else {
            if(actionType.equals("put")){
                writeByServer(graph, vertices, edges);
            }else if(actionType.equals("delete")){
                deleteByServer(graph, vertices, edges);
            }
        }
    }

    /* we transfer the vertex & edge into bytes array
     * TODO: use a batch and send them together
     * */
    void writeDirectly(List<Vertex> vertices, List<Edge> edges) {
        for (Vertex vertex : vertices) {
            byte[] ownerRowkey = hStoreSerializer.getOwnerKeyBytes(vertex);
            byte[] rowkey = hStoreSerializer.getKeyBytes(vertex);
            byte[] values = hStoreSerializer.getValueBytes(vertex);
            sendRpcToHStorePut("vertex",ownerRowkey, rowkey, values);
        }

        for (Edge edge : edges) {
            byte[] ownerRowkey = hStoreSerializer.getOwnerKeyBytes(edge);
            byte[] rowkey = hStoreSerializer.getKeyBytes(edge);
            byte[] values = hStoreSerializer.getValueBytes(edge);
            sendRpcToHStorePut("edge", ownerRowkey, rowkey, values);

            //TODO: switch 需要使用 target_vertiecs 当作ownerKey
            byte[] ownerRowkeySwitch = hStoreSerializer.getOwnerKeyBytes(edge);
            byte[] rowkeySwitch = hStoreSerializer.getKeyBytesSwitchDirection(edge);
            byte[] valuesSwitch = hStoreSerializer.getValueBytes(edge);
            sendRpcToHStorePut("edge", ownerRowkeySwitch, rowkeySwitch, valuesSwitch);
        }
    }


    void deleteDirectly(List<Vertex> vertices, List<Edge> edges) {
        for (Vertex vertex : vertices) {
            byte[] rowkey = hStoreSerializer.getKeyBytes(vertex);
            byte[] values = hStoreSerializer.getValueBytes(vertex);
            sendRpcToHStoreDelete("vertex",null, rowkey, values);
        }

        for (Edge edge : edges) {
            byte[] rowkey = hStoreSerializer.getKeyBytes(edge);
            byte[] values = hStoreSerializer.getValueBytes(edge);
            sendRpcToHStoreDelete("edge", IdGenerator.of(edge.sourceId()).asBytes(), rowkey, values);

            byte[] rowkeySwitch = hStoreSerializer.getKeyBytesSwitchDirection(edge);
            byte[] valuesSwitch = hStoreSerializer.getValueBytes(edge);
            sendRpcToHStoreDelete("edge", IdGenerator.of(edge.targetId()).asBytes(), rowkeySwitch, valuesSwitch);
        }
    }

    boolean sendRpcToRocksDB(byte[] rowkey, byte[] values) {
        // here we call the rpc
        boolean flag = false;
        //flag = put(rowkey, values);
        return flag;
    }

    void writeByServer(GraphManager graph, List<Vertex> vertices, List<Edge> edges) {
        vertices = graph.addVertices(vertices);
        vertices.forEach(System.out::println);

        edges = graph.addEdges(edges, false);
        edges.forEach(System.out::println);
    }

    void deleteByServer(GraphManager graph, List<Vertex> vertices, List<Edge> edges) {

        for(Vertex vertex : vertices){
            graph.removeVertex(vertex.id());
        }

    }



    boolean sendRpcToHBase(String type, byte[] ownerKey,byte[] rowkey, byte[] values) {
        boolean flag = false;
        try {
            flag = put(type, ownerKey,rowkey, values);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return flag;
    }

    boolean sendRpcToHStorePut(String type, byte[] ownerKey ,byte[] rowkey, byte[] values) {
        boolean flag = false;
        try {
            flag = put(type,ownerKey, rowkey, values);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return flag;
    }

    boolean sendRpcToHStoreDelete(String type, byte[] ownerKey ,byte[] rowkey, byte[] values) {
        boolean flag = false;
        try {
            flag = delete(type,ownerKey, rowkey, values);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return flag;
    }

    boolean put(String type, byte[] ownerKey ,byte[] rowkey, byte[] values) throws IOException {
        // TODO: put to HBase/HStore
        if(type.equals("vertex")){
            HStore.addVetices(ownerKey,rowkey,values);
        }else if(type.equals("edge")){
            HStore.addEdges(ownerKey,rowkey,values);
        }
        return true;
    }

    boolean delete(String type, byte[] ownerKey ,byte[] rowkey, byte[] values) throws IOException {
        // TODO: put to HBase/HStore
        if(type.equals("vertex")){
            HStore.deleteVertices(rowkey,rowkey);
        }else if(type.equals("edge")){
            HStore.deleteEdges(ownerKey,rowkey);
        }
        return true;
    }
}
