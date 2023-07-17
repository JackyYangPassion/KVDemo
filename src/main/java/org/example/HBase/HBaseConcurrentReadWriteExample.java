package org.example.HBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

//请编写一段Java 代码，进行多线程并发读写HBase mygraph:edges表 1000万条数据 code by compilot
//目标：复现CPU load 高的问题
public class HBaseConcurrentReadWriteExample {

    private static final String TABLE_NAME = "mygraph:edges";
    private static final int TOTAL_RECORDS = 10000000;
    private static final int THREAD_POOL_SIZE = 10;

    public static void main(String[] args) {
        // 创建HBase配置对象
        Configuration configuration = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            // 获取表对象
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

            // 创建线程池
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

            // 创建任务列表
            List<Callable<Void>> tasks = new ArrayList<>();

            // 并发写入数据
            for (int i = 0; i < TOTAL_RECORDS; i++) {
                final int recordNumber = i;
                Callable<Void> task = () -> {
                    try {
                        //每一组进行10次操作
                        for(int j = 0; j < 10; j++){
                            //先进行Get 操作


                            //构造行键和数据
                            String rowKey = "row" + recordNumber;
                            String columnFamily = "f";
                            String qualifier = "1";
                            String value = "value" + recordNumber;

                            Get get = new Get(rowKey.getBytes());
                            // 插入数据
                            table.get(get);


                            // 创建Put对象并设置行键和数据
                            Put put = new Put(rowKey.getBytes());
                            put.addColumn(columnFamily.getBytes(), qualifier.getBytes(), value.getBytes());

                            // 插入数据
                            table.put(put);

                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                };
                tasks.add(task);
            }

            // 提交任务到线程池执行
            List<Future<Void>> futures = executorService.invokeAll(tasks);

            // 等待所有任务完成
            for (Future<Void> future : futures) {
                future.get(); // 等待任务完成并获取结果
            }

            // 关闭线程池
            executorService.shutdown();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
