package org.example.HStore;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.store.*;

import java.nio.charset.StandardCharsets;

import static org.apache.hugegraph.store.client.util.HgStoreClientUtil.toStr;

/**
 * 模仿 HgKvStoreTest.java 测试用例开发使用
 * 1. 实现 HStore-Client 读写
 */
public class HStoreDemo1 {
    public static HgStoreClient storeClient;
    public static PDClient pdClient;
    protected static String tableName = "testTableName";
    public static final String TABLE_NAME = "g+v";


    public static void beforeClass() {
        storeClient = HgStoreClient.create(PDConfig.of("127.0.0.1:8686")
                .setEnableCache(true));
        pdClient = storeClient.getPdClient();
    }

    public void truncateTest() {
        System.out.println("--- test put/scan/truncate  ---");
        HgStoreSession graph0 = storeClient.openSession("hugegraph0");

        graph0.truncate();

        for (int i = 0; i < 3; i++) {
            HgOwnerKey key = toOwnerKey("owner-" + i, "ownerKey-" + i);
            byte[] value0 = toBytes("g0 owner-" + i + ";ownerKey-" + i);
            graph0.put(TABLE_NAME, key, value0);
        }

        HgKvIterator<HgKvEntry> iterator = graph0.scanIterator(TABLE_NAME);

        while (iterator.hasNext()) {
            HgKvEntry entry = iterator.next();
            System.out.println("key: "+ toStr(entry.key())+"  value: "+toStr(entry.value()));

        }

        graph0.truncate();
        iterator = graph0.scanIterator(TABLE_NAME);
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
        beforeClass();
        new HStoreDemo1().truncateTest();

    }
}
