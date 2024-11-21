package org.apache.paimon.mytest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.util.Map;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : norma                                                                                  *
 * @date : 2024/11/21                                                                             *
 *================================================================================================*/
public class MyParquetWriter {

  public static void main(String[] args) throws Exception {
    String outputPath = "output/demo.parquet"; // Parquet 文件输出路径

    // 定义 Parquet 文件的 Schema
    String schemaString = "message schema { "
            + "required int32 id; "
            + "required binary name (UTF8); "
            + "optional double salary; "
            + "}";
    MessageType schema = MessageTypeParser.parseMessageType(schemaString);

    // 设置 Hadoop 配置
    Configuration conf = new Configuration();
    conf.set("parquet.enable.summary-metadata", "true");
    conf.set("parquet.summary.metadata.level", "ALL");
    conf.set("parquet.write.statistics", "true");
    GroupWriteSupport.setSchema(schema, conf);


    // 创建 ParquetWriter
    Path path = new Path(outputPath);

    try (ParquetWriter<Group> writer = new ParquetWriter<Group>(
            path,
            new GroupWriteSupport(),
            ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
            1024 * 1024,    // 块大小
            1024,           // 页大小
            512,
            true,           // 启用字典编码
            true,           // 启用验证
            ParquetProperties.WriterVersion.PARQUET_1_0,
            conf)) {

      // 写入数据
      for (int i = 0; i < 1000; i++) {
        Group group = new SimpleGroup(schema);
        group.add("id", i);
        group.add("name", "name_" + i);
        if (i % 2 == 0) {
          group.add("salary", i * 1000.0); // 可选列
        }
        writer.write(group);
      }

      System.out.println("Parquet file written to: " + outputPath);
    }
  }
}
