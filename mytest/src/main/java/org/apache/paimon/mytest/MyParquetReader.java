package org.apache.paimon.mytest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.paimon.format.parquet.ParquetInputFile;
import org.apache.paimon.format.parquet.reader.LongColumnReader;
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
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : lan                                                                                    *
 * @date : 2024/11/20                                                                             *
 * ============================================================================================== */
public class MyParquetReader {

  public static void main(String[] args) throws IOException {
    Path path = new Path("file:///Users/lan/tmp/paimon-catalog/my_db.db/my_table/bucket-0/data-a486283f-101e-406b-8f12-c4cc23f9fed6-0.parquet");

    Configuration configuration = new Configuration();

    FilterPredicate f0 = FilterApi.eq(FilterApi.longColumn("f0"), (long)380000);
//
//    ParquetInputFormat.setFilterPredicate(configuration, f0);

    ParquetReadOptions options = ParquetReadOptions.builder()
            .withRecordFilter(FilterCompat.get(f0))
            .build();

    long startTime = System.currentTimeMillis();


    ParquetFileReader parquetFileReader = new ParquetFileReader(
            HadoopInputFile.fromPath(path, configuration),
            options);

    MessageType schema = parquetFileReader.getFooter().getFileMetaData().getSchema();

    System.out.println(schema);

    List<ColumnDescriptor> columns = schema.getColumns();

    List<BlockMetaData> blocks = parquetFileReader.getFooter().getBlocks();

    PageReadStore rowGroup;
    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
    while ((rowGroup = parquetFileReader.readNextFilteredRowGroup()) != null){
      System.out.println("=======================Row-GROUP=======================");
      long rowCount = rowGroup.getRowCount();
      System.out.println("row-count : " + rowCount);
      System.out.println("Row-group index : " + rowGroup.getRowIndexOffset());
      Optional<PrimitiveIterator.OfLong> rowIndexes1 = rowGroup.getRowIndexes();

      if (rowIndexes1.isPresent()){
        while (rowIndexes1.get().hasNext()){
          System.out.print(rowIndexes1.get().next() + " ");
        }
      }

      System.out.println("indexs : " + rowIndexes1);
      DataPage dataPage;
      long pageCount = 0;
      PageReader pageReader = rowGroup.getPageReader(columns.get(0));
//      while ((dataPage = pageReader.readPage()) != null){
//          pageCount = pageCount + 1;
//
//
//          System.out.println("page - " + pageCount + " : " + dataPage);
//          System.out.println("page - " + pageCount + " : " + dataPage.getFirstRowIndex());
//
//        }

//
//      for (ColumnDescriptor column : columns) {
//        System.out.println("----------column : "+ column.toString() + "------------");
//        PageReader pageReader = rowGroup.getPageReader(column);
//        DataPage dataPage;
//        long pageCount = 0;
//
//        while ((dataPage = pageReader.readPage()) != null){
//          pageCount = pageCount + 1;
//
//
//          System.out.println("page - " + pageCount + " : " + dataPage);
//          System.out.println("page - " + pageCount + " : " + dataPage.getFirstRowIndex());
//
//        }
//        System.out.println("----------------------------------------------");
//      }

      RecordReader<Group> recordReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));
//
//      // 遍历记录
      for (int i = 0; i < rowCount; i++) {
        Group group = recordReader.read();
        System.out.println("Record " + i + ": " + group.toString().replaceAll("\n", ","));
      }

      System.out.println("==================================================");
    }

    long stopTime = System.currentTimeMillis();

    System.out.println("time : " + (stopTime - startTime));

  }


}
