package org.example.RocksDB;


import org.rocksdb.*;
import org.rocksdb.util.BytewiseComparator;

import java.io.File;

import java.util.*;
import java.util.stream.Collectors;


/**
 * 功能中关键技术
 * 1. Java 相关实现 https://stackoverflow.com/questions/58784381/creating-rocksdb-sst-file-in-java-for-bulk-loading
 * 2. C++ 相关实现  https://github.com/facebook/rocksdb/wiki/Creating-and-Ingesting-SST-files
 *
 * 实现的功能
 * 1. generate SST
 * 2. bulkload into rocksDB
 * 3. 读取数据
 * 4. 验证 Copilot 协助编程工作流 沉淀下来 提效
 *
 *
 */

public class GenerateSST {
    private static final String cfdbPath = "./rocksdb-data-bulkload/";


    public static void main(String[] args) throws RocksDBException {
        final EnvOptions envOptions = new EnvOptions();
        final StringAppendOperator stringAppendOperator = new StringAppendOperator();
        Options options1 = new Options();
        RocksDB.loadLibrary();

        /****************************Gen SST**************************************/

        File sstFile = newSstFile(envOptions,options1,stringAppendOperator);

        /****************************Ingesting SST files**************************************/
        final StringAppendOperator stringAppendOperator1 = new StringAppendOperator();
        IngestingSSTfiles(envOptions,options1,stringAppendOperator1);

        /****************************Read Data From the RocksDB********************************/
        scanRockDBTable("test","new-cf");



    }

    /**
     * 此方法生成SST
     *
     * @param envOptions
     * @param options
     * @param stringAppendOperator
     * @return
     */
    private static File newSstFile(EnvOptions envOptions,Options options,StringAppendOperator stringAppendOperator){
        //TODO: 重点是 K-V 有序
        final Random random = new Random();

        SstFileWriter fw = null;
        try {
            ComparatorOptions comparatorOptions = null;
            BytewiseComparator comparator = null;
            comparatorOptions = new ComparatorOptions().setUseDirectBuffer(false);
            comparator = new BytewiseComparator(comparatorOptions);

            options = options
                    .setCreateIfMissing(true)
                    .setEnv(Env.getDefault())
                    .setComparator(comparator);

            fw = new SstFileWriter(envOptions, options);
            fw.open("./tmp/db/sst_upload_01");

            Map<String, String> data = new HashMap<>();
            for (int index = 0; index < 1000; index++) {
                data.put("Key-" + random.nextLong(), "Value-" + random.nextDouble());
            }
            List<String> keys = new ArrayList<String>(data.keySet());
            Collections.sort(keys);
            for (String key : keys) {
                Slice keySlice = new Slice(key);
                Slice valueSlice = new Slice(data.get(key));
                fw.put(keySlice, valueSlice);
            }
            fw.finish();
        } catch (RocksDBException ex) {
            ex.printStackTrace();
        } finally {
            stringAppendOperator.close();
            envOptions.close();
            options.close();
            if (fw != null) {
                fw.close();
            }
        }

        File sstFile = new File("./tmp/db/sst_upload_01");
        return sstFile;
    }

    /**
     * 此方法主要是将SST 文件装载到RocksDB
     *
     */
    private  static void IngestingSSTfiles(EnvOptions envOptions,Options options,StringAppendOperator stringAppendOperator) throws RocksDBException {
        Options options2 = new Options()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMergeOperator(stringAppendOperator);


        final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();

        IngestExternalFileOptions ingestExternalFileOptions = new IngestExternalFileOptions();
        ColumnFamilyOptions cf_opts = new ColumnFamilyOptions().setMergeOperator(stringAppendOperator);


        String columnFamilyName = "new_cf";
        List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
                new ColumnFamilyDescriptor(columnFamilyName.getBytes(), cf_opts)
        );

        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
        try (final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)) {
            try (final RocksDB db = RocksDB.open(dbOptions, cfdbPath, cfDescriptors, cfHandles)) {
                ColumnFamilyHandle cf_handle = null;
                for (ColumnFamilyHandle handle : cfHandles) {
                    if (new String(handle.getName()).equals(columnFamilyName)) {
                        cf_handle = handle;
                        break;
                    }
                }
                if (cf_handle != null) {
                    db.ingestExternalFile(cf_handle, Collections.singletonList("./tmp/db/sst_upload_01"), ingestExternalFileOptions);
                }
            }
        }

        System.out.println("Generate SST File");

    }


    /**
     * 指定表名+CFName
     * 遍历此表
     * @param tableName
     * @param CfName
     */
    private static void scanRockDBTable(String tableName,String CfName){
        final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();
        RocksDB db = null;
        String cfName = "new_cf";
        // list of column family descriptors, first entry must always be default column family
        final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                new ColumnFamilyDescriptor(cfName.getBytes(), cfOpts)
        );



        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();


        final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        try {
            db = RocksDB.open(dbOptions, cfdbPath, cfDescriptors, cfHandles);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }


        ColumnFamilyHandle cfHandle = cfHandles.stream().filter(x -> {
            try {
                return (new String(x.getName())).equals(cfName);
            } catch (RocksDBException e) {
                return false;
            }
        }).collect(Collectors.toList()).get(0);

        //Scan 动作
        RocksIterator iter = db.newIterator(cfHandle);
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println("iterator key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
        }

    }

}
