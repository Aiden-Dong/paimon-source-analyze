/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.format.parquet.writer;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import javax.annotation.Nullable;

import java.io.IOException;

/** A {@link ParquetBuilder} for {@link InternalRow}. */
public class RowDataParquetBuilder implements ParquetBuilder<InternalRow> {

    private final RowType rowType;        // schema 类型
    private final Configuration conf;     // hadoop 配置  -- 来自 option 选项

    public RowDataParquetBuilder(RowType rowType, Options options) {
        this.rowType = rowType;
        this.conf = new Configuration(false);
        options.toMap().forEach(conf::set);
    }

    // Parquet 写入构建工具
    @Override
    public ParquetWriter<InternalRow> createWriter(OutputFile out, String compression)
            throws IOException {

        // 自定义 parquet write builder
        return new ParquetRowDataBuilder(out, rowType)
                .withConf(conf)
                // 数据区的压缩类型
                .withCompressionCodec(CompressionCodecName.fromConf(getCompression(compression)))
                // row group 的大小 默认 128M
                .withRowGroupSize(conf.getLong(ParquetOutputFormat.BLOCK_SIZE, ParquetWriter.DEFAULT_BLOCK_SIZE))   // parquet.block.size
                // page 大小， 默认1M 一个
                .withPageSize(conf.getInt(ParquetOutputFormat.PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE))          // parquet.page.size
                // 字典编码页面大小 默认 1M一个
                .withDictionaryPageSize(conf.getInt(ParquetOutputFormat.DICTIONARY_PAGE_SIZE, ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE)) // parquet.dictionary.page.size
                .withMaxPaddingSize(conf.getInt(ParquetOutputFormat.MAX_PADDING_BYTES, ParquetWriter.MAX_PADDING_SIZE_DEFAULT))  // parquet.writer.max-padding
                // 是否开启字典编码类型
                .withDictionaryEncoding(conf.getBoolean(ParquetOutputFormat.ENABLE_DICTIONARY, ParquetProperties.DEFAULT_IS_DICTIONARY_ENABLED))  // parquet.enable.dictionary
                .withValidation(conf.getBoolean(ParquetOutputFormat.VALIDATION, false))  // parquet.validation
                .withWriterVersion(                                                                  // parquet.writer.version
                        ParquetProperties.WriterVersion.fromString(
                                conf.get(ParquetOutputFormat.WRITER_VERSION,
                                        ParquetProperties.DEFAULT_WRITER_VERSION.toString())))
                .build();
    }

    public String getCompression(@Nullable String compression) {
        String compressName;
        if (null != compression) {
            compressName = compression;
        } else {
            compressName =
                    conf.get(ParquetOutputFormat.COMPRESSION, CompressionCodecName.SNAPPY.name());
        }
        return compressName;
    }
}
