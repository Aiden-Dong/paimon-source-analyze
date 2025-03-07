package org.apache.paimon.mytest;

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.RowKind;

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
    Table table = TableUtil.getTable();                                // PrimaryKeyFileStoreTable
    BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();     // BatchWriteBuilderImpl

    IOManager ioManager  = new IOManagerImpl("/Users/lan/tmp/paimon-catalog/my_db.db/tmp");

    // 2. 在分布式任务中写入记录
    BatchTableWrite write = (BatchTableWrite)writeBuilder.newWrite()
            .withIOManager(ioManager);  // TableWriteImpl

    long startTime = System.currentTimeMillis();

    for(int i = 0; i < 10; i++){

      GenericRow genericRow = GenericRow.of (
              (long)i,
              Timestamp.fromEpochMillis(i)
      );
      write.write(genericRow);
    }

    List<CommitMessage> messages = write.prepareCommit();

    // 3. 将所有 CommitMessages 收集到一个全局节点并提交
    BatchTableCommit commit = writeBuilder.newCommit();

    System.out.println("commit start");
    commit.commit(messages);
    System.out.println("commit success");

    long stopTime = System.currentTimeMillis();
    System.out.println("耗时 : " + (stopTime - startTime));

  }
}

