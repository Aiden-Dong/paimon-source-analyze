package org.apache.paimon.mytest;

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;

import java.util.List;
import java.util.Random;
import java.util.UUID;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : lan                                                                                    *
 * @date : 2024/11/13                                                                             *
 * ============================================================================================== */
public class BatchWrite {
  public static void main(String[] args) throws Exception {
    // 1. 创建一个WriteBuilder（可序列化）
    Table table = TableUtil.getTable();   // PrimaryKeyFileStoreTable
    BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
    // BatchWriteBuilderImpl

    String[] items = new String[]{"h1", "h2", "h3"};

    // 2. 在分布式任务中写入记录
    BatchTableWrite write = writeBuilder.newWrite();  // TableWriteImpl

    long startTime = System.currentTimeMillis();

    for(int i = 0; i < 400000; i++){

      GenericRow genericRow = GenericRow.of(
              (long)i,
              BinaryString.fromString(items[i%3]),
              BinaryString.fromString(UUID.randomUUID().toString()),
              (float)i,
              (double)i,
              (i % 2) == 0,
              BinaryArray.fromLongArray(new Long[]{(long)i, (long)i+1,(long) i+2})
      );

      write.write(genericRow);

      if ((i % 10000) == 0) {
        System.out.println("write rows : " + i);
      }
    }

    System.out.println("precommit start");
    List<CommitMessage> messages = write.prepareCommit();
    System.out.println("precommit success");

    // 3. 将所有 CommitMessages 收集到一个全局节点并提交
    BatchTableCommit commit = writeBuilder.newCommit();

    System.out.println("commit start");
    commit.commit(messages);
    System.out.println("commit success");

    long stopTime = System.currentTimeMillis();

    System.out.println("耗时 : " + (stopTime - startTime));

    // 中止不成功的提交以删除数据文件
    // commit.abort(messages);
  }
}

