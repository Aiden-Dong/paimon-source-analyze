package org.apache.paimon.mytest;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
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

import static org.apache.paimon.CoreOptions.WRITE_ONLY;

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
    BatchTableWrite write = (BatchTableWrite)writeBuilder
            .newWrite()
            .withIOManager(ioManager);  // TableWriteImpl

    for(int i = 10; i < 20; i++){
      GenericRow genericRow = GenericRow.ofKind(RowKind.DELETE,
              BinaryString.fromString(String.valueOf(i)),
              BinaryString.fromString(String.format("hello_%d", i)),
              BinaryString.fromString("pt_1"));

//      GenericRow genericRow = GenericRow.of (
//              BinaryString.fromString(String.valueOf(i)),
//              BinaryString.fromString(String.format("hello_%d", i)),
//              BinaryString.fromString("pt_1")
//      );
      write.write(genericRow);
    }

    // 记录每个 write 的文件变更信息
    List<CommitMessage> messages = write.prepareCommit();

    // 3. 将所有 CommitMessages 收集到一个全局节点并提交
    BatchTableCommit commit = writeBuilder.newCommit();
    commit.commit(messages);
  }
}

