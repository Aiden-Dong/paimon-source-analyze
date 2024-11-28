package org.apache.paimon.mytest;

import org.apache.paimon.data.InternalArray;
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

  PredicateBuilder builder = new PredicateBuilder(
          RowType.of(DataTypes.BIGINT(),
                  DataTypes.STRING(),
                  DataTypes.STRING(),
                  DataTypes.FLOAT(),
                  DataTypes.DOUBLE(),
                  DataTypes.BOOLEAN(),
                  DataTypes.ARRAY(DataTypes.BIGINT())));

  int[] projection = new int[] {0, 1, 2, 3, 4, 5, 6};

  ReadBuilder readBuilder = table.newReadBuilder()
          .withProjection(projection);


  // 3. Distribute these splits to different tasks

  List<Split> splits = readBuilder
          .newScan()
          .plan()
          .splits();

  // 4. Read a split in task
  long startTime = System.currentTimeMillis();

  Random random = new Random();

  for(int i = 0 ; i < 30 ; i ++){

   int value = random.nextInt(4000000) ;
   Predicate keyFilter = builder.equal(0, (long)value);

   InnerTableRead read = (InnerTableRead)readBuilder.newRead();

   read.withFilter(keyFilter);//.executeFilter();

//   List<Split> splits1 = table.newReadBuilder()
//           .withFilter(keyFilter)
//           .withProjection(projection)
//           .newScan()
//           .plan()
//           .splits();
   
   RecordReader<InternalRow> reader = read.createReader(splits);

   reader.forEachRemaining(internalRow -> {

    long f0 = internalRow.getLong(0);
    String f1 = internalRow.getString(1).toString();
    String f2 = internalRow.getString(2).toString();
    float f3 = internalRow.getFloat(3);
    double f4 = internalRow.getDouble(4);
    boolean f5 = internalRow.getBoolean(5);

    long[] f6 = internalRow.getArray(6).toLongArray();

    System.out.println(String.format("%d : [%d, %s, %s, %f, %f, %b, (%s))",value, f0, f1, f2, f3, f4, f5, toString(f6)));//;);
   });


  }
  long stopTime = System.currentTimeMillis();
  System.out.println("耗时 : " + (stopTime - startTime));

 }

 private static String toString(long[] value){
  StringBuilder builder = new StringBuilder();
  builder.append("[");
  for (int i = 0; i<value.length; i++){
   builder.append(value[i]);
   builder.append(",");
  }
  builder.append("]");
  return builder.toString();
 }

}

