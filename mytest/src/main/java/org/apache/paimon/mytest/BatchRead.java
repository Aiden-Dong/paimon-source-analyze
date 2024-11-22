package org.apache.paimon.mytest;

import org.apache.paimon.data.InternalRow;
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
  Table table = TableUtil.getTable();   // PrimaryKeyFileStoreTable

  PredicateBuilder builder = new PredicateBuilder(RowType.of(DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()));

  int[] projection = new int[] {0, 1, 2};

  ReadBuilder readBuilder = table.newReadBuilder()
          .withProjection(projection);

  // 2. Plan splits in 'Coordinator' (or named 'Driver')
  List<Split> splits = readBuilder.newScan().plan().splits();

  // 3. Distribute these splits to different tasks

  // 4. Read a split in task
  long startTime = System.currentTimeMillis();



  Random random = new Random();

  for(int i = 0 ; i < 20 ; i ++){
   InnerTableRead read = (InnerTableRead)readBuilder.newRead();
   int value = random.nextInt(5000) * 3;
   int key = (value * value) % 4000000;
   Predicate keyFilter = builder.equal(0, key);
   read.withFilter(keyFilter).executeFilter();
   RecordReader<InternalRow> reader = read.createReader(splits);
   reader.forEachRemaining(internalRow -> {

    int f0 = internalRow.getInt(0);
    String f1 = internalRow.getString(1).toString();
    String f2 = internalRow.getString(2).toString();
    System.out.println(String.format("%d-%d, %s, %s",key, f0, f1, f2));
   });


  }
  long stopTime = System.currentTimeMillis();
  System.out.println("耗时 : " + (stopTime - startTime));

 }

}

