package org.apache.paimon.mytest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
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

    System.out.println(schema.toJson());
    System.out.println("rows : " + reader.getNumberOfRows());

    for (StripeStatistics stripeStatistic : reader.getStripeStatistics()) {
      // strip
      System.out.println("==============================================");
      for (ColumnStatistics columnStatistic : stripeStatistic.getColumnStatistics()) {
        if (columnStatistic instanceof IntegerColumnStatistics){
          long minValue = ((IntegerColumnStatistics) columnStatistic).getMinimum();
          long maxValue = ((IntegerColumnStatistics) columnStatistic).getMaximum();
          System.out.println(minValue + "--" + maxValue);
        } else if (columnStatistic instanceof BinaryColumnStatistics) {

        }else if (columnStatistic instanceof StringColumnStatistics){
          String minValue = ((StringColumnStatistics) columnStatistic).getMinimum();
          String maxValue = ((StringColumnStatistics) columnStatistic).getMaximum();
          System.out.println(minValue + "--" + maxValue);
        }
      }
    }
  }

  // 读取数据
  private static void searchData(Reader reader) throws IOException {
    TypeDescription schema = reader.getSchema();
    VectorizedRowBatch batch = schema.createRowBatch(128);
    Random random = new Random();
    int searchKey = random.nextInt(1947611);
    System.out.println("search : " + searchKey);

    SearchArgument searchArgument = SearchArgumentFactory.newBuilder()
            .equals("_KEY_f0", PredicateLeaf.Type.LONG, (long)searchKey)
            .build();

    Reader.Options options = reader.options()
            .schema(schema)
            .useZeroCopy(true)
            .searchArgument(searchArgument, new String[]{});

    RecordReader rows = reader.rows(options);

    LongColumnVector x = (LongColumnVector) batch.cols[0];
    LongColumnVector y = (LongColumnVector) batch.cols[1];

    int count = 0;
    while (rows.nextBatch(batch)){
      for (int row = 0; row < batch.size; ++row) {
        long key = x.vector[row];
        long value = y.vector[row];
        System.out.println("search : " + key + "," + value);
        count = count + 1;
      }
    }
    System.out.println("count : " + count);

  }

  public static void main(String[] args) throws IOException {

    Configuration conf = new Configuration();

    // 创建一个 ORC Reader 类的对象
    Reader reader = OrcFile.createReader(
            new Path("file:///Users/lan/tmp/paimon-catalog/my_db.db/orc/data-4661743d-9cdd-4656-8eeb-150c9f258dbf-0.orc"),
            OrcFile.readerOptions(conf)
    );

    long startTime = System.currentTimeMillis();
    searchData(reader);
    long stopTime = System.currentTimeMillis();



    System.out.println("time : " + (stopTime - startTime));

    getMetaData(reader);

  }
}
