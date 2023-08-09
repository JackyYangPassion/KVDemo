package org.example.HBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;
import java.util.concurrent.*;

public class HBaseMultiplePrefixScan {

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("mygraph:edges");
        Table table = connection.getTable(tableName);

        Set<byte[]> prefixes = new HashSet<>();
        prefixes.add(Bytes.toBytes("1"));
        prefixes.add(Bytes.toBytes("2"));

        // Create a thread pool with a fixed number of threads
        int numThreads = Math.min(prefixes.size(), Runtime.getRuntime().availableProcessors());
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

        // List to hold the Future objects representing each scan operation
        List<Future<Iterator<Result>>> futures = new ArrayList<>();

        // Submit scan tasks for each prefix key
        for (byte[] prefixKey : prefixes) {
            Future<Iterator<Result>> future = executorService.submit(() -> {
                return performScan(table, prefixKey);
            });
            futures.add(future);
        }

        // Wait for all scan tasks to complete and combine the iterators
        List<Iterator<Result>> resultList = new ArrayList<>();
        for (Future<Iterator<Result>> future : futures) {
            Iterator<Result> partialResults = future.get();
            resultList.add(partialResults);
        }

        // Process the combined results from all the scans
        for (Iterator<Result> it : resultList) {
            // Process the result data as needed
            // e.g., result.getValue(family, qualifier);
            while( it.hasNext() ) {
                System.out.println(it.next());
            }

        }

        // Close resources
        table.close();
        connection.close();
        executorService.shutdown();
    }

    // Perform the scan operation for a specific prefix key
    private static Iterator<Result> performScan(Table table, byte[] prefixKey) throws Exception {
        Scan scan = new Scan();
        scan.setStartStopRowForPrefixScan(prefixKey);
        ResultScanner scanner = table.getScanner(scan);

        // Return the iterator directly
        return scanner.iterator();
    }
}


