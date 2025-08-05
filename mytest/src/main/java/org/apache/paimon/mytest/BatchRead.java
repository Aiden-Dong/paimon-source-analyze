package org.apache.paimon.mytest;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : lan                                                                                *
 * @date : 2024/11/13                                                                                *
 * ============================================================================================== */
public class BatchRead {

 public static void main(String[] args) throws IOException {

  Table table = TableUtil.getTable();   // PrimaryKeyFileStoreTable

  int[] projection = new int[] {0, 1, 2};


  ReadBuilder readBuilder = table.newReadBuilder()
          .withProjection(projection);

  // 3. Distribute these splits to different tasks

  List<Split> splits = readBuilder.newScan()
          .plan()
          .splits();

   InnerTableRead read = (InnerTableRead)readBuilder.newRead();

   RecordReader<InternalRow> reader = read.createReader(splits);

   reader.forEachRemaining(internalRow -> {
    String s = internalRow.getString(0).toString();
    String s1 = internalRow.getString(1).toString();
    String s2 = internalRow.getString(2).toString();

    System.out.println(String.format("%s,%s,%s", s, s1, s2));
   });
  }


}

