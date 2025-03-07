package org.apache.paimon.mytest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.*;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : lan                                                                                    *
 * @date : 2024/11/20                                                                             *
 * ============================================================================================== */
public class ORCReader {
  private static void getMetaData(Reader reader) throws IOException {
    TypeDescription schema = reader.getSchema(); // schema 信息

    System.out.println("col-size : " + schema.getFieldNames().size());
    System.out.println(schema.toJson());
    System.out.println("rows : " + reader.getNumberOfRows());

//    for (StripeStatistics stripeStatistic : reader.getStripeStatistics()) {
//      // strip
//      System.out.println("==============================================");
//      for (ColumnStatistics columnStatistic : stripeStatistic.getColumnStatistics()) {
//        if (columnStatistic instanceof IntegerColumnStatistics){
//          long minValue = ((IntegerColumnStatistics) columnStatistic).getMinimum();
//          long maxValue = ((IntegerColumnStatistics) columnStatistic).getMaximum();
//          System.out.println(minValue + "--" + maxValue);
//        } else if (columnStatistic instanceof BinaryColumnStatistics) {
//
//        }else if (columnStatistic instanceof StringColumnStatistics){
//          String minValue = ((StringColumnStatistics) columnStatistic).getMinimum();
//          String maxValue = ((StringColumnStatistics) columnStatistic).getMaximum();
//          System.out.println(minValue + "--" + maxValue);
//        }
//      }
//    }
  }

  // 读取数据
  private static void searchData(Reader reader) throws IOException {
    TypeDescription schema = reader.getSchema();
    VectorizedRowBatch batch = schema.createRowBatch(128);

    Reader.Options options = reader.options()
            .include(new boolean[]{false, false, false, false, false, true, false, false, false, false, true, true, false, false})
            .schema(schema)
            .useZeroCopy(true);

    RecordReader rows = reader.rows(options);

    LongColumnVector col1 = (LongColumnVector) batch.cols[0];
    TimestampColumnVector col2 = (TimestampColumnVector) batch.cols[1];
    TimestampColumnVector col3 = (TimestampColumnVector) batch.cols[2];

    int count = 0;
    while (rows.nextBatch(batch)){
      for (int row = 0; row < batch.size; ++row) {
        long sno = col1.vector[row];
        long create_date = col2.time[row];
        long modify_date = col3.time[row];
        System.out.println("sno : " + sno + "," + "create_date: " + create_date  + "," + "modify_date: " + modify_date);
        count = count + 1;
      }
    }
    System.out.println("count : " + count);

  }

  public static void main(String[] args) throws IOException {

    Configuration conf = new Configuration();




    // 创建一个 ORC Reader 类的对象
    Reader reader = OrcFile.createReader(
            new Path("file:///Users/lan/tmp/data-42dbe704-f196-4b82-ade6-52078d2ee434-0.orc"),
            OrcFile.readerOptions(conf)
    );

    getMetaData(reader);

//    long startTime = System.currentTimeMillis();
    searchData(reader);
//    long stopTime = System.currentTimeMillis();



//    System.out.println("time : " + (stopTime - startTime));

//

  }
}
