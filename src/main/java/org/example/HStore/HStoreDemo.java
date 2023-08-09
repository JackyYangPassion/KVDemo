package org.example.HStore;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgStoreClient;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgKvStore;


import java.util.List;

import static org.apache.hugegraph.store.client.util.HgStoreClientConst.ALL_PARTITION_OWNER;
import static org.apache.hugegraph.store.client.util.HgStoreClientUtil.*;

/**
 * 主要是访问HStore
 * 验证 KV 存储接口能力
 * 1. 客户端引用 便于集成开发测试
 * 2. 快速学习资料：源码 UnitTests
 */
public class HStoreDemo {
    //1. 初始化HStore
    //2. 通过HStore的Put接口写入数据
    //3. 通过HStore的Get接口读取数据
    //4. 通过HStore的Delete接口删除数据
    //5. 通过HStore的Scan接口扫描数据

    //请给出上述5个代码的实现

    public static HgStoreClient storeClient;

    public static PDClient pdClient;

    public static final byte[] EMPTY_BYTES = new byte[0];

    private static final String graphName = "testGraphName";

    private static String tableName = "testTableName";

    public static final String TABLE_NAME = "unit-table";


    public static void beforeClass() {
        storeClient = HgStoreClient.create(PDConfig.of("127.0.0.1:8686")
                .setEnableCache(true));
        pdClient = storeClient.getPdClient();
    }

    public void truncateTest() {
        System.out.println("--- test truncateTest ---");
        HgStoreSession graph0 = storeClient.openSession("hugegraph0");
        HgStoreSession graph1 = storeClient.openSession("hugegraph1");

        graph0.truncate();
        graph1.truncate();

        for (int i = 0; i < 3; i++) {
            HgOwnerKey key = toOwnerKey("owner-" + i, "ownerKey-" + i);
            byte[] value0 = toBytes("g0 owner-" + i + ";ownerKey-" + i);
            graph0.put(TABLE_NAME, key, value0);

            byte[] value1 = toBytes("g1 owner-" + i + ";ownerKey-" + i);
            graph1.put(TABLE_NAME, key, value1);
        }

        HgKvIterator<HgKvEntry> iterator = graph0.scanIterator(TABLE_NAME);
        //Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
//            log.info("key:{} value:{}", toStr(entry.key()), toStr(entry.value()));
            //Assert.assertEquals(0, toStr(entry.value()).indexOf("g0"));
        }
        iterator = graph1.scanIterator(TABLE_NAME);
        //Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
//            log.info("key:{} value:{}", toStr(entry.key()), toStr(entry.value()));
            //Assert.assertEquals(0, toStr(entry.value()).indexOf("g1"));
        }

        graph0.truncate();
        iterator = graph0.scanIterator(TABLE_NAME);
        //Assert.assertFalse(iterator.hasNext());

        iterator = graph1.scanIterator(TABLE_NAME);
        //Assert.assertTrue(iterator.hasNext());
        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
            // System.out.println("key:" + toStr(entry.key()) + " value:" + toStr(entry.value()));
            //Assert.assertEquals(0, toStr(entry.value()).indexOf("g1"));
        }
    }






    public void testPutData(HgStoreClient storeClient) {
        HgStoreSession session = storeClient.openSession(graphName);
        session.createTable(tableName);
        long start = System.currentTimeMillis();
        int loop = 100000;
        //session.truncate();
        HgStoreTestUtil.batchPut(session, tableName, "testKey", loop);

        System.out.println("Time is " + (System.currentTimeMillis() - start));
        try (HgKvIterator<HgKvEntry> iterator = session.scanIterator(tableName)) {
            //Assert.assertEquals(loop, HgStoreTestUtil.amountOf(iterator));
            //遍历访问返回的数据
            while (iterator.hasNext()) {
                HgKvEntry entry = iterator.next();
                System.out.println("key:" + toStr(entry.key()) + " value:" + toStr(entry.value()));
            }
        }
    }


    public void testPutData2() {
        String graphName = "testGraphName2";
        HgStoreSession session = storeClient.openSession(graphName);
        long start = System.currentTimeMillis();
        int loop = 100000;
        session.truncate();
        HgStoreTestUtil.batchPut(session, tableName, "testKey", loop);

        System.out.println("Time is " + (System.currentTimeMillis() - start));
        try (HgKvIterator<HgKvEntry> iterator = session.scanIterator(tableName)) {
            //Assert.assertEquals(loop, HgStoreTestUtil.amountOf(iterator));
        }
    }


    public void testScan() throws PDException {

        HgStoreSession session = storeClient.openSession(graphName);
        HgStoreTestUtil.batchPut(session, tableName, "testKey", 12);

        int count = 0;
        byte[] position = null;
        try (HgKvIterator<HgKvEntry> iterator = session.scanIterator(tableName)) {
            while (iterator.hasNext()) {
                iterator.next();
                position = iterator.position();
                dumpPosition(position);
                if (++count > 5) {
                    break;
                }
            }
        }


        System.out.println("--------------------------------");
        try (HgKvIterator<HgKvEntry> iterator = session.scanIterator(tableName)) {
            iterator.seek(position);
            while (iterator.hasNext()) {
                iterator.next();
                dumpPosition(iterator.position());
            }
        }

        System.out.println("--------------------------------");


        byte[] start = new byte[]{0x0};
        byte[] end = new byte[]{-1};
        try (HgKvIterator<HgKvEntry> iterator = session.scanIterator(tableName,
                HgOwnerKey.of(
                        ALL_PARTITION_OWNER,
                        start),
                HgOwnerKey.of(
                        ALL_PARTITION_OWNER,
                        end))) {
            iterator.seek(position);
            while (iterator.hasNext()) {
                iterator.next();
                dumpPosition(iterator.position());
            }
        }
    }

    public void dumpPosition(byte[] b) {
        byte[] buf = new byte[Long.BYTES];
        System.arraycopy(b, 0, buf, 0, Long.BYTES);
        // long storeId = HgStoreTestUtil.toLong(buf);
        buf = new byte[Integer.BYTES];
        System.arraycopy(b, Long.BYTES, buf, 0, Integer.BYTES);
        // int partId = HgStoreTestUtil.toInt(buf);
        // String key = new String(b);

        // System.out.println(" " + storeId + ", " + partId + ", " + key);
    }


    public void testDeleteData() {
        tableName = "deleteData5";
        HgStoreSession session = storeClient.openSession(graphName);
        int ownerCode = 1;
        HgStoreTestUtil.batchPut(session, tableName, "T", 10, (key) -> {
                    return HgStoreTestUtil.toOwnerKey(ownerCode, key);
                }
        );
        try (HgKvIterator<HgKvEntry> iterators = session.scanIterator(tableName)) {
//            while (iterators.hasNext()){
//                System.out.println(new String(iterators.next().key()));
//            }
           // Assert.assertEquals(10, HgStoreTestUtil.amountOf(iterators));
        }
        session.beginTx();
        session.deletePrefix(tableName, HgStoreTestUtil.toOwnerKey(ownerCode, "T"));
        session.commit();

        System.out.println("=================================");
        try (HgKvIterator<HgKvEntry> iterators = session.scanIterator(tableName)) {
            //Assert.assertEquals(0, HgStoreTestUtil.amountOf(iterators));
//            while (iterators.hasNext()){
//                System.out.println(new String(iterators.next().key()));
//            }
        }
    }


    public void testDropTable() throws PDException {
        HgStoreSession session = storeClient.openSession(graphName);

        String table1 = "Table1";
        session.createTable(table1);
        HgStoreTestUtil.batchPut(session, table1, "testKey", 1000);

        try (HgKvIterator<HgKvEntry> iterators = session.scanIterator(table1)) {
            //Assert.assertEquals(1000, HgStoreTestUtil.amountOf(iterators));
        }

        session.dropTable(table1);
        try (HgKvIterator<HgKvEntry> iterators = session.scanIterator(table1)) {
            //Assert.assertEquals(0, HgStoreTestUtil.amountOf(iterators));
        }

        deleteGraph(graphName);
    }


    public void deleteGraph(String graphName) throws PDException {
        HgStoreSession session = storeClient.openSession(graphName);
        session.deleteGraph(graphName);
        pdClient.delGraph(graphName);

        Metapb.Graph graph = null;
        try {
            graph = pdClient.getGraph(graphName);
        } catch (PDException e) {
            //Assert.assertEquals(103, e.getErrorCode());
        }
        //Assert.assertNull(graph);
    }


    public void testScanPartition() throws PDException {
        // testPutData();
        List<Metapb.Partition> partitions = pdClient.getPartitions(0, "DEFAULT/hugegraph/g");
        HgStoreSession session = storeClient.openSession("DEFAULT/hugegraph/g");
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


    public static void main(String[] args) {
        beforeClass();
        new HStoreDemo().testPutData(storeClient);
    }
}
