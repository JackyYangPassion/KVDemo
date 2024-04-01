package org.example.HStore;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.*;


import java.nio.charset.StandardCharsets;
import java.util.List;

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
    public static final byte[] EMPTY_BYTES = new byte[0];



    public HStoreSessionImpl(){

//        PDConfig pdConfig =
//                PDConfig.of(config.get(CoreOptions.PD_PEERS))
//                        .setEnableCache(true);
//
//        hgStoreClient =
//                HgStoreClient.create(defaultPdClient);

        PDConfig pdConfig =
                PDConfig.of("127.0.0.1:8686")
                        .setEnableCache(true);
        pdClient = PDClient.create(pdConfig);



        storeClient = HgStoreClient.create(pdClient);// 创建HStoreClient

        graph = storeClient.openSession("hugegraph/g");
    }

    /**
     * 查询指定图分区数 并扫描指定Partition 下的数据
     * @throws PDException
     */
    public void getPartitions() throws PDException {
        List<Metapb.Partition> partitions = pdClient.getPartitions(0, "hugegraph/g");

        HgStoreSession session = storeClient.openSession("hugegraph/g");
        for (Metapb.Partition partition : partitions) {
            try (HgKvIterator<HgKvEntry> iterators = session.scanIterator("g+v",
                                                                            (int) (partition.getStartKey()),
                                                                            (int) (partition.getEndKey()),
                                                                            HgKvStore.SCAN_HASHCODE,
                                                                            EMPTY_BYTES)) {

                System.out.println(
                        " " + partition.getId() + " " + HgStoreTestUtil.amountOf(iterators));
            }
        }


    }




    /**
     * 写入点表
     * @param rowkey
     * @param values
     */
    public void addVetices(byte[] ownerKey, byte[] rowkey, byte[] values){

        //TODO:如何计算Partition_ID
        //int partitionID = ;
        //TODO: 当前缺少OwnerKey bytes[] 生成逻辑
        //this.graph.put(table, HgOwnerKey.of(ownerKey, key), value);
        HgOwnerKey key = HgOwnerKey.of(ownerKey, rowkey);

        graph.put(VETEX_TABLE_NAME, key, values);
        //graph.directPut(VETEX_TABLE_NAME,1, key, values);
    }

    public void deleteVertices(byte[] ownerkey,byte[] rowkey){
        HgOwnerKey key = HgOwnerKey.of(ownerkey, rowkey);
        graph.delete(VETEX_TABLE_NAME, key);
        //graph.deletePrefix(VETEX_TABLE_NAME,key);
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
