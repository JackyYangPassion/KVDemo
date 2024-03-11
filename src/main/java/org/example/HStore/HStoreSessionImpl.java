package org.example.HStore;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgStoreClient;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;


import java.nio.charset.StandardCharsets;

import static org.apache.hugegraph.store.client.util.HgStoreClientUtil.toStr;

/**
 * 模仿 HgKvStoreTest.java 测试用例开发使用
 * 1. 实现 HStore-Client 读写
 *
 * 2. 实现基本逻辑
 *   a. createSession
 *   b. CRUD
 *
 * 底层点边表
 * put("unknown", 0);
 * put("g+v", 1);
 * put("g+oe", 2);
 * put("g+ie", 3);
 * put("g+index", 4);
 * put("g+task", 5);
 * put("g+olap", 6);
 * put("g+server", 7);
 *
 */
public class HStoreSessionImpl {
    public static HgStoreClient storeClient;
    public static PDClient pdClient;
    public static HgStoreSession graph;
    public static final String VETEX_TABLE_NAME = "g+v";
    public static final String OUT_EDGE_TABLE_NAME = "g+oe";
    public static final String IN_EDGE_TABLE_NAME = "g+ie";



    public HStoreSessionImpl(){
        storeClient = HgStoreClient.create(PDConfig.of("127.0.0.1:8686")
                .setEnableCache(true));
        pdClient = storeClient.getPdClient();
        graph = storeClient.openSession("hugegraph/g");
    }

    /**
     * 写入点表
     * @param rowkey
     * @param values
     */
    public void addVetices(byte[] rowkey, byte[] values){

        //TODO: 当前缺少OwnerKey bytes[] 生成逻辑
        //this.graph.put(table, HgOwnerKey.of(ownerKey, key), value);
        HgOwnerKey key = HgOwnerKey.of(rowkey, rowkey);

        graph.put(VETEX_TABLE_NAME, key, values);
    }

    public void deleteVertices(byte[] ownerkey,byte[] rowkey){
        HgOwnerKey key = HgOwnerKey.of(ownerkey, rowkey);
        graph.delete(VETEX_TABLE_NAME, key);
    }


    /**
     * 写入边表
     * @param rowkey
     * @param values
     */
    public void addEdges(byte[] ownerkey, byte[] rowkey, byte[] values){
        HgOwnerKey key = HgOwnerKey.of(ownerkey, rowkey);
        graph.put(OUT_EDGE_TABLE_NAME, key, values);
    }



    public void deleteEdges(byte[] ownerkey,byte[] rowkey){
        HgOwnerKey key = HgOwnerKey.of(ownerkey, rowkey);
        graph.delete(OUT_EDGE_TABLE_NAME, key);
        graph.delete(IN_EDGE_TABLE_NAME, key);
    }


    public void truncateTest() {
        System.out.println("--- test put/scan/truncate  ---");
        graph.truncate();

        for (int i = 0; i < 3; i++) {
            HgOwnerKey key = toOwnerKey("owner-" + i, "ownerKey-" + i);
            byte[] value0 = toBytes("g0 owner-" + i + ";ownerKey-" + i);
            graph.put(OUT_EDGE_TABLE_NAME, key, value0);
        }

        HgKvIterator<HgKvEntry> iterator = graph.scanIterator(OUT_EDGE_TABLE_NAME);

        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
            System.out.println("key: "+ toStr(entry.key())+"  value: "+toStr(entry.value()));

        }

        graph.truncate();
        iterator = graph.scanIterator(OUT_EDGE_TABLE_NAME);
    }



    public void scan(String type){
        HgKvIterator<HgKvEntry> iterator = null;
        if(type.equals("in_edge")){
            iterator = graph.scanIterator(IN_EDGE_TABLE_NAME);
        } else if(type.equals("out_edge")) {
            iterator = graph.scanIterator(OUT_EDGE_TABLE_NAME);
        } else if(type.equals("vertices")){
            iterator = graph.scanIterator(VETEX_TABLE_NAME);
        }


        System.out.println("Scan type: "+type);
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
            byte[] keyFromHStore = entry.key();
            byte[] valueFromHStore = entry.value();

            System.out.println("key: "+ toStr(keyFromHStore)+"  value: "+toStr(valueFromHStore));
        }
    }

    public static HgOwnerKey toOwnerKey(String owner, String key) {
        return new HgOwnerKey(toBytes(owner), toBytes(key));
    }

    public static byte[] toBytes(String str) {
        if (str == null) {
            return null;
        }
        return str.getBytes(StandardCharsets.UTF_8);
    }

    public static void main(String args[]){
        new HStoreSessionImpl().scan("vertices");
    }
}
