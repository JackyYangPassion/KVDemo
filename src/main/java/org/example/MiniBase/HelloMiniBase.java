package org.example.MiniBase;

import org.apache.minibase.*;

import java.io.File;
import java.io.IOException;

public class HelloMiniBase {


    public static void main(String args[]) throws IOException {
        System.out.println("Hello World!");

        /**
         * 1. 创建目录
         */
        String dataDir = "target/minihbase-" + System.currentTimeMillis();
        File f = new File(dataDir);
        f.mkdirs();

        /**
         * 2. 链接KV存储
         */
        Config conf = new Config().setDataDir(dataDir).setMaxMemstoreSize(1).setFlushMaxRetries(1).setMaxDiskFiles(10);
        final MiniBase db = MStore.create(conf).open();

        /**
         * 3. 写入数据
         */
        // Put
        db.put(Bytes.toBytes(1), Bytes.toBytes(1));

        /**
         * 4. 查询数据
         */
        // Scan
        MiniBase.Iter<KeyValue> kv = db.scan();
        while (kv.hasNext()) {
            KeyValue kv1 = kv.next();
            System.out.println("Key: "+ Bytes.toInt(kv1.getKey()));
            System.out.println("Value: "+ Bytes.toInt(kv1.getValue()));
            //...
        }


        db.close();
        System.exit(0);

    }

}
