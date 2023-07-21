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

/**
 * 复现步骤：
 * 1. 前置条件：HBase(分布式HMaster/RS 配置中Mem 不刷新)/ZK/Grafana/arthas 已在本机安装
 * 2. 运行该程序，观察CPU load
 *    a. 先写入 100 条, 每条 10,000 条重复数据(同步观察Mem，不刷新)
 *    b. 停止写入，修改逻辑，100条数据并发读取 100,000 次(同步观察Mem，不刷新)
 *       1. 不指定列名 new Get(rowKey.getBytes())
 *          火焰图 20230721-134019.html，20230721-134044.html
 *       2. 指定列名   new Get(rowKey.getBytes()).addColumn(columnFamily.getBytes(), qualifier.getBytes());
 *          火焰图 20230721-134334.html，20230721-134420.html
 *       以上两个步骤分别抓取火焰图两次
 */

public class HBaseConcurrentReadWriteExample {

    private static final String TABLE_NAME = "mygraph:edges";
    private static final int TOTAL_RECORDS = 100;
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
                        for(int j = 0; j < 100000; j++){
                            //先进行Get 操作

                            //构造行键和数据
                            String rowKey = "row" + recordNumber;
                            String columnFamily = "f";
                            String qualifier = "";
                            String value = "value" + recordNumber;

                            // 创建Put对象并设置行键和数据
//                            Put put = new Put(rowKey.getBytes());
//                            put.addColumn(columnFamily.getBytes(), qualifier.getBytes(), value.getBytes());
//
//                            // 插入数据
//                            table.put(put);



                            Get get = new Get(rowKey.getBytes()).addColumn(columnFamily.getBytes(), qualifier.getBytes());
                            // 插入数据
                            table.get(get);




//                            // 创建Put对象并设置行键和数据
//                            Put put1 = new Put(rowKey.getBytes());
//                            put1.addColumn(columnFamily.getBytes(), qualifier.getBytes(), value.getBytes());
//
//                            // 插入数据
//                            table.put(put1);

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
