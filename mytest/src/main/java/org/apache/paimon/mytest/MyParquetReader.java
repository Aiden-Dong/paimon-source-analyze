package org.apache.paimon.mytest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.*;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : lan                                                                                *
 * @date : 2024/11/20                                                                                *
 * ============================================================================================== */
public class MyParquetReader {

  public static void main(String[] args) throws IOException {
    // 确定 Parquet 文件的路径
    //String parquetFilePath = "file:///Users/lan/tmp/paimon-catalog/my_db.db/parquet/data-88800912-ec20-4661-af6c-f25857e9f7ec-0.parquet";
    String parquetFilePath = "file:///Users/lan/tmp/part-00038-26cbc036-a790-4c31-83b3-a153fdbdaab1-c000";

    // 创建 Hadoop 配置和文件系统对象
    Configuration configuration = new Configuration();
    FileSystem fs = FileSystem.get(configuration);

    // 读取 Parquet 文件的元数据
    Path path = new Path(parquetFilePath);

    Random random = new Random();
    int key = random.nextInt(1947611);

//    FilterPredicate filter = FilterApi.eq(FilterApi.intColumn("_KEY_f0"), key);

    ParquetReadOptions options = ParquetReadOptions.builder()
            .useStatsFilter(true)
            .useRecordFilter(true)
            .useColumnIndexFilter(true)
//            .withRecordFilter(FilterCompat.get(filter))
            .build();

    InputFile fin = HadoopInputFile.fromPath(path, configuration);

    long startTime = System.currentTimeMillis();


    try(ParquetFileReader reader = new ParquetFileReader(fin, options)){
      FileMetaData fileMetaData = reader.getFileMetaData();
      MessageType schema = fileMetaData.getSchema();
      System.out.println(schema.toString());

      List<BlockMetaData> rowGroups = reader.getRowGroups();
      System.out.println("Number of Row Groups: " + rowGroups.size());

//      for (BlockMetaData rowGroup : rowGroups) {
////        System.out.println("===========================");
////        List<ColumnChunkMetaData> columns = rowGroup.getColumns();
////        for (ColumnChunkMetaData column : columns) {
////          System.out.println(column.getStatistics());
////        }
////      }


      PageReadStore rowGroup;
      int rowGroupIndex = 0;

      while ((rowGroup = reader.readNextRowGroup()) != null){
        long rowCount = rowGroup.getRowCount();

        ColumnDescriptor keyColumn = schema.getColumns().get(0);

        System.out.println(keyColumn);

        PageReader pageReader = rowGroup.getPageReader(keyColumn);
        DataPage page;

        while((page = pageReader.readPage()) != null){
            if (page instanceof DataPageV1){
              Statistics<?> statistics = ((DataPageV1) page).getStatistics();
              System.out.println("V1 : " + statistics);
            }else if(page instanceof DataPageV2){
              Statistics<?> statistics = ((DataPageV2) page).getStatistics();
              System.out.println("V2 : " + statistics);
            }
        }
        break;



//        for (ColumnDescriptor column : schema.get()) {
//          PageReader pageReader = rowGroup.getPageReader(column);
//          DataPage page;
//          while((page = pageReader.readPage()) != null){
//            if ()
//          }
//        }

        // 构建 Column IO
//        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
//
//        // 创建 Record Reader
//        RecordReader<Group> recordReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));
//
//        // 遍历记录
//        for (int i = 0; i < rowCount; i++) {
//          Group group = recordReader.read();
//          System.out.println("Record " + i + ": " + group.toString().replaceAll("\n", ","));
//        }

//        System.out.println("Reading Row Group #" + rowGroupIndex);
//        System.out.println("Row Group Row Count: " + rowCount);
//        rowGroupIndex++;
      }
    }
    long stopTime = System.currentTimeMillis();

    System.out.println("time : " + (stopTime - startTime));
  }
}
