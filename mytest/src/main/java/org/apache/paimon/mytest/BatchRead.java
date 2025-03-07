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

  // 1. Create a ReadBuilder and push filter (`withFilter`)
  // and projection (`withProjection`) if necessary
//  TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  Table table = TableUtil.getTable();   // PrimaryKeyFileStoreTable

  PredicateBuilder builder = new PredicateBuilder(
          RowType.of(DataTypes.BIGINT(),
                  DataTypes.TIMESTAMP()
          ));

  int[] projection = new int[] {0, 1};

  ReadBuilder readBuilder = table.newReadBuilder()
          .withProjection(projection);

  // 3. Distribute these splits to different tasks

  List<Split> splits = readBuilder
          .newScan()
          .plan()
          .splits();

   InnerTableRead read = (InnerTableRead)readBuilder.newRead();
   RecordReader<InternalRow> reader = read.createReader(splits);

   reader.forEachRemaining(internalRow -> {

    long f0 = internalRow.getLong(0);
    Timestamp f1 = internalRow.getTimestamp(1, 6);
    System.out.println(String.format("(%d, %d)",f0, f1.getMillisecond()));
   });
  }


}

