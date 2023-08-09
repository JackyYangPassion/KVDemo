package org.example.HBase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * 1. 创建表
 * 2. 数据进行CRUD 了解使用特性
 */
public class Example {
    public static final int DEFAULT_SCAN_CACHING = 1000;//此处保证了 一次RPC请求 结果够用，next 不会再发起请求
    public static final String EMPTY_COLUMN_NAME = "_0";
    public static final byte[] EMPTY_COLUMN_BYTES = Bytes.toBytes(EMPTY_COLUMN_NAME);
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private final Configuration hbaseConf;
    private final TableName tableName;
    private final Connection conn;
    public Example(Configuration hbaseConf, final String tableName) {
        try {
            this.hbaseConf = hbaseConf;
            hbaseConf.set("hbase.zookeeper.quorum", "127.0.0.1:2181");
            hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure");
            this.conn = ConnectionFactory.createConnection(hbaseConf);
            this.tableName = TableName.valueOf(tableName);
        } catch (Exception ex) {
            throw new RuntimeException("HBase client preparation failed.", ex);
        }
    }


    /**
     * 此方法主要是对HBase Scan 封装，对上层屏蔽掉优化细节
     * TODO： 这种方式是不是比迭代器方式浪费内存？但是性能好？
     * @param startRow
     * @param endRow
     * @param startTime
     * @param endTime
     * @param caching
     * @param versions
     * @param filterList
     * @param limit
     * @return
     * @throws IOException
     */
    public List<Result> scan(byte[] startRow, byte[] endRow, long startTime, long endTime,
                             int caching, int versions, FilterList filterList, int limit)
            throws IOException {
        Scan scan = new Scan();

        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(true));//开启scan metrics
        if (startRow != null) {
            scan.withStartRow(startRow);
        }

        if(endRow != null){
            scan.withStopRow(endRow);
        }

        if (startTime > 0 && endTime > 0) {
            scan.setTimeRange(startTime, endTime);
        }
        if (caching >= 0) {
            scan.setCaching(caching);
        }
        if (versions > 0) {


        }
        if (filterList != null) {
            scan.setFilter(filterList);
        }

        Table table = getTable();
        ResultScanner scanner = null;

        List<Result> results = new ArrayList<>();
        try {
            scanner = table.getScanner(scan);
            Result result;
            int i = 0;
            while ((result = scanner.next()) != null && i < limit) {
                if (!result.isEmpty()) {
                    results.add(result);
                    i++;
                }
            }
        } finally {
            if (scanner != null) scanner.close();
            table.close();
        }
        return results;
    }

    public List<Result> scan(byte[] startRow, byte[] endRow, long startTime, long endTime,
                             int caching, int versions, FilterList filterList)
            throws IOException {
        return scan(startRow, endRow, startTime, endTime, caching, versions, filterList,
                Integer.MAX_VALUE);
    }

    public List<Result> scan(byte[] startRow, byte[] endRow)
            throws IOException {
        return scan(startRow, endRow, -1, -1, DEFAULT_SCAN_CACHING, 1, null);
    }

    public List<Result> scan(byte[] startRow, byte[] endRow, int limit)
            throws IOException {
        return scan(startRow, endRow, -1, -1, DEFAULT_SCAN_CACHING, 1, null, limit);
    }

    public List<Result> scan(long startTime, long endTime, FilterList filterList)
            throws IOException {
        return scan(null, null, startTime, endTime, DEFAULT_SCAN_CACHING, 1, filterList);
    }

    public List<Result> scan(long startTime, long endTime)
            throws IOException {
        return scan(null, null, startTime, endTime, DEFAULT_SCAN_CACHING, 1, null);
    }

    public List<Result> scan(byte[] startRow, byte[] endRow, long startTime, long endTime,
                             FilterList filterList) throws IOException {
        return scan(startRow, endRow, startTime, endTime, DEFAULT_SCAN_CACHING, 1, filterList);
    }

    public List<Result> scan(byte[] startRow, byte[] endRow, int caching, FilterList filterList)
            throws IOException {
        return scan(startRow, endRow, -1, -1, caching, 1, filterList);
    }

    public List<Result> scan(byte[] startRow, byte[] endRow, int caching, FilterList filterList,
                             int limit) throws IOException {
        return scan(startRow, endRow, -1, -1, caching, 1, filterList, limit);
    }

    public Result get(Get get) throws IOException {
        Table table = getTable();
        try {
            return table.get(get);
        } finally {
            table.close();
        }
    }

    public boolean exists(Get get) throws IOException {
        Table table = getTable();
        try {
            return table.exists(get);
        } finally {
            table.close();
        }
    }

    public Result[] batchGet(List<Get> gets) throws IOException {
        Table table = getTable();
        try {
            return table.get(gets);
        } finally {
            table.close();
        }
    }

    public void put(Put put) throws IOException {
        Table table = getTable();
        try {
            table.put(put);
        } finally {
            table.close();
        }
    }

    /**
     * This is used to make a put compatible to phoenix table.
     * An empty column will be added silently so that queries by phoenix
     * work as expected.
     *
     * @param put    the original put
     * @param family the first column family in phoenix where the empty column will be added
     */
    public void putWithEmptyColumn(Put put, byte[] family) throws IOException {
        Table table = getTable();
        try {
            table.put(addEmptyColumn(put, family));
        } finally {
            table.close();
        }
    }

    public Put addEmptyColumn(Put put, byte[] family) {
        put.addColumn(family, EMPTY_COLUMN_BYTES, EMPTY_BYTE_ARRAY);
        return put;
    }

    public void delete(Delete delete) throws IOException {
        Table table = getTable();
        try {
            table.delete(delete);
        } finally {
            table.close();
        }
    }

    public void batchMutate(List<Mutation> mutations) throws IOException, InterruptedException {
        Table table = getTable();
        try {
            Object[] result = new Object[mutations.size()];
            table.batch(mutations, result);
        } finally {
            table.close();
        }
    }

    public void close() throws IOException {
        conn.close();
    }

    public Table getTable() throws IOException {
        Table table = conn.getTable(tableName);
        return table;
    }


    public static void main(String[] args) throws IOException {
        Configuration hbaseConf = new Configuration();
        Example example = new Example(hbaseConf,"mygraph:edges");

        /**
         * ADD KeyValue To HBase
         */
        Put put = new Put(Bytes.toBytes("1"));
        put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("1"),Bytes.toBytes("1"));
        example.put(put);


        Put put2 = new Put(Bytes.toBytes("2"));
        put2.addColumn(Bytes.toBytes("f"),Bytes.toBytes("1"),Bytes.toBytes("2"));
        example.put(put2);



        Put put3 = new Put(Bytes.toBytes("13"));
        put3.addColumn(Bytes.toBytes("f"),Bytes.toBytes("1"),Bytes.toBytes("3"));
        example.put(put3);


        Put put4 = new Put(Bytes.toBytes("24"));
        put4.addColumn(Bytes.toBytes("f"),Bytes.toBytes("1"),Bytes.toBytes("4"));
        example.put(put4);


        /**
         * Get KeyValue From HBase
         */
        Get get = new Get(Bytes.toBytes("1"));
        Result rsGet = example.get(get);
        System.out.println("rsGet: " + rsGet);



        /**
         * Scan KeyValue From HBase with Start Row and End Row
         */
        String startRow = "1";
        String endRow = "2";

        //此处返回的是一个List 不是一个迭代器
        //迭代器Iterator 模式有什么好处以及区别：
        //内存节省角度： scan cache + list<Result> 比迭代器双倍内存？ 但是速度快？
        List<Result> result = example.scan(Bytes.toBytes(startRow),Bytes.toBytes(endRow));
        System.out.println("===========print result for scan==========");
        for(Result rs: result){
            System.out.println(rs);
        }


        //迭代器Iterator 访问 Scan 结果
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(endRow));
        Table table = example.getTable();
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        System.out.println("============Get Result From Iterator============");
        while(iterator.hasNext()){
            System.out.println(iterator.next());
        }


        /**
         * Scan KeyValue From HBase with PrefixFilter
         * 1. 使用双层迭代器
         * 2. 使用单层迭代器
         */
        Set<byte[]> prefixes = new HashSet<>();
        prefixes.add(Bytes.toBytes("1"));
        prefixes.add(Bytes.toBytes("2"));

        //传递多个 Filter 到底层
        FilterList orFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (byte[] prefix : prefixes) {
            FilterList andFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            List<MultiRowRangeFilter.RowRange> ranges = new ArrayList<>();
            ranges.add(new MultiRowRangeFilter.RowRange(prefix, true, null, true));
            andFilters.addFilter(new MultiRowRangeFilter(ranges));
            andFilters.addFilter(new PrefixFilter(prefix));

            orFilters.addFilter(andFilters);
        }

        Scan scanFilterList = new Scan().setFilter(orFilters);


        Table tableFilter = example.getTable();
        ResultScanner scannerFilter = tableFilter.getScanner(scanFilterList);
        Iterator<Result> iteratorFilter = scannerFilter.iterator();
        System.out.println("============Get Result From Iterator FilterList============");
        while(iteratorFilter.hasNext()){
            System.out.println(iteratorFilter.next());
        }

        //HBase 多个prefixKey 一次scan 查询返回结果


    }

}
