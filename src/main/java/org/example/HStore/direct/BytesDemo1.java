package org.example.HStore.direct;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.driver.GraphManager;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.driver.SchemaManager;
import org.apache.hugegraph.serializer.direct.HBaseSerializer;
import org.apache.hugegraph.serializer.direct.HStoreSerializer;
import org.apache.hugegraph.serializer.direct.RocksDBSerializer;
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
    boolean bypassServer = true;
    RocksDBSerializer ser;
    HStoreSerializer hStoreSerializer;
    HStoreSessionImpl HStore;
//    HBaseSerializer HBaseSer;

    public static void main(String[] args) {
        BytesDemo1 ins = new BytesDemo1();

        ins.initGraph();
    }

    void initGraph() {
        // If connect failed will throw an exception.
        client = HugeClient.builder("http://localhost:8080", "hugegraph").build();


        SchemaManager schema = client.schema();

//
//        schema.propertyKey("name").asText().ifNotExist().create();
//        schema.propertyKey("age").asInt().ifNotExist().create();
//        schema.propertyKey("lang").asText().ifNotExist().create();
//        schema.propertyKey("date").asText().ifNotExist().create();
        schema.propertyKey("price").asText().ifNotExist().create();
//
//        schema.vertexLabel("person")
//                .properties("name", "age")
//                .useCustomizeStringId()
//                .enableLabelIndex(false)
//                .ifNotExist()
//                .create();
//
        schema.vertexLabel("personB")
                .properties("price")
                .nullableKeys("price")
                .useCustomizeNumberId()
                .enableLabelIndex(false)
                .ifNotExist()
                .create();
//
//        schema.vertexLabel("software")
//                .properties("name", "lang", "price")
//                .useCustomizeStringId()
//                .enableLabelIndex(false)
//                .ifNotExist()
//                .create();
//
//        schema.edgeLabel("knows")
//                .link("person", "person")
//                .properties("date")
//                .enableLabelIndex(false)
//                .ifNotExist()
//                .create();
//
//        schema.edgeLabel("created")
//                .link("person", "software")
//                .properties("date")
//                .enableLabelIndex(false)
//                .ifNotExist()
//                .create();

        hStoreSerializer = new HStoreSerializer(client);
        HStore = new HStoreSessionImpl();
        writeGraphElements();

        client.close();
    }

    private void writeGraphElements() {
        GraphManager graph = client.graph();
        // construct some vertexes & edges
//        Vertex peter = new Vertex("person");
//        peter.property("name", "peter");
//        peter.property("age", 35);
//        peter.id("peter");
//
//        Vertex lop = new Vertex("software");
//        lop.property("name", "lop");
//        lop.property("lang", "java");
//        lop.property("price", "328");
//        lop.id("lop");

        Vertex vadasB = new Vertex("personB");
        vadasB.property("price", "120");
        vadasB.id(12345);

//        Edge peterCreateLop = new Edge("created").source(peter).target(lop)
//                .property("date", "2017-03-24");

        List<Vertex> vertices = new ArrayList<Vertex>() {{
//            add(peter);
//            add(lop);
            add(vadasB);
        }};

        List<Edge> edges = new ArrayList<Edge>() {{
//            add(peterCreateLop);
        }};

        // Old way: encode to json then send to server
        if (bypassServer) {
            writeDirectly(vertices, edges);
        } else {
            writeByServer(graph, vertices, edges);
        }
    }

    /* we transfer the vertex & edge into bytes array
     * TODO: use a batch and send them together
     * */
    void writeDirectly(List<Vertex> vertices, List<Edge> edges) {
        for (Vertex vertex : vertices) {
            byte[] rowkey = hStoreSerializer.getKeyBytes(vertex);
            byte[] values = hStoreSerializer.getValueBytes(vertex);
            sendRpcToHStore("vertex", rowkey, values);
        }

        for (Edge edge : edges) {
            byte[] rowkey = hStoreSerializer.getKeyBytes(edge);
            byte[] values = hStoreSerializer.getValueBytes(edge);
            sendRpcToHStore("edge", rowkey, values);
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

    boolean sendRpcToHBase(String type, byte[] rowkey, byte[] values) {
        boolean flag = false;
        try {
            flag = put(type, rowkey, values);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return flag;
    }

    boolean sendRpcToHStore(String type, byte[] rowkey, byte[] values) {
        boolean flag = false;
        try {
            flag = put(type, rowkey, values);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return flag;
    }

    boolean put(String type, byte[] rowkey, byte[] values) throws IOException {
        // TODO: put to HBase/HStore
        if(type.equals("vertex")){
            HStore.addVetices(rowkey,values);
        }else if(type.equals("edge")){
            HStore.addEdges(rowkey,values);
        }
        return true;
    }
}
