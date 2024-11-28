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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.data.columnar.writable.WritableIntVector;

import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.*;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;

/**
 * 抽象的 {@link ColumnReader}。参见 {@link ColumnReaderImpl}，
 * 部分代码参考自 Apache Spark 和 Apache Parquet
 */
public abstract class AbstractColumnReader<VECTOR extends WritableColumnVector>
        implements ColumnReader<VECTOR> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractColumnReader.class);

    private final PageReader pageReader;

    /** The dictionary, if this column has dictionary encoding. */
    protected final Dictionary dictionary;

    /** Maximum definition level for this column. */
    protected final int maxDefLevel;

    protected final ColumnDescriptor descriptor;

//    // 当前页面总的数据量
//    private int pageValueCount;
//
//    // 当前 ColumnChunk 中已经消费了的数据量
//    private long valuesRead;
//
//    /**
//     * value that indicates the end of the current page. That is, if valuesRead == endOfPageValueCount, we are at the end of the page.
//     * 表示当前页面结束的值。
//     * 也就是说，如果 valuesRead == endOfPageValueCount，则表示我们处于页面的末尾。
//     *
//     * 当前 ColumnChunk 中可消费数据量，包含已消费数据量(valuesRead) + 当前 page 中未消费数据量(pageValueCount)
//     */
//    private long endOfPageValueCount;


    // 当前页面是否使用字典编码的标识
    private boolean isCurrentPageDictionaryEncoded;

    private long pageFirstRowIndex;
    /**
     * Helper struct to track intermediate states while reading Parquet pages in the column chunk.
     * 用于在读取列块中的 Parquet 页面时跟踪中间状态的辅助结构体。
     */
    private final ParquetReadState readState;

    /*
     * Input streams:
     * 1.运行长度编码流 (Run length stream)： 用于编码数据，该流包含了每个数据的运行长度信息。
     * 2.包含实际数据，也可能包含字典 ID，需要通过字典进行解码才能得到实际数据。
     *
     * Run length stream ------>  数据流
     *                  |
     *                   ------> 字典 ID 流
     */

    /** Run length decoder for data and dictionary. */
    protected RunLengthDecoder runLenDecoder;

    /** Data input stream. */
    ByteBufferInputStream dataInputStream;

    /** 字典解码器，用于处理字典 ID 输入流. */
    private RunLengthDecoder dictionaryIdsDecoder;

    public AbstractColumnReader(ColumnDescriptor descriptor, PageReadStore pageReadStore)
            throws IOException {
        this.descriptor = descriptor;
        this.pageReader = pageReadStore.getPageReader(descriptor);
        this.maxDefLevel = descriptor.getMaxDefinitionLevel();

        this.readState = new ParquetReadState(pageReadStore.getRowIndexes().orElse(null));

        DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
        if (dictionaryPage != null) {
            try {
                this.dictionary = dictionaryPage.getEncoding().initDictionary(descriptor, dictionaryPage);
                this.isCurrentPageDictionaryEncoded = true;
            } catch (IOException e) {
                throw new IOException("could not decode the dictionary for " + descriptor, e);
            }
        } else {
            this.dictionary = null;
            this.isCurrentPageDictionaryEncoded = false;
        }
        // 获取当前 RowGroup 中总的可读数据量
        long totalValueCount = pageReader.getTotalValueCount();
        if (totalValueCount == 0) {
            throw new IOException("totalValueCount == 0");
        }
    }

    protected void checkTypeName(PrimitiveType.PrimitiveTypeName expectedName) {
        PrimitiveType.PrimitiveTypeName actualName = descriptor.getPrimitiveType().getPrimitiveTypeName();

        Preconditions.checkArgument(
                actualName == expectedName,
                "Expected type name: %s, actual type name: %s",
                expectedName,
                actualName);
    }

    /**
     * 从该 ColumnReader 读取 `total` 个值到 column 中.
     * @param readNumber 要读取的数据量
     * @param vector     数据保存位置
     * */
    @Override
    public  void readToVector(int readNumber, VECTOR vector) throws IOException {
        int rowId = 0;
        WritableIntVector dictionaryIds = null;
        if (dictionary != null) {
            dictionaryIds = vector.reserveDictionaryIds(readNumber);
        }

        readState.resetForNewBatch(readNumber);   // 读状态初始化

        while (readState.rowsToReadInBatch  > 0) {

//            System.out.println(String.format("[%s] : %d - %d",this.descriptor, readState.rowsToReadInBatch, readState.valuesToReadInPage));

            // 标识当前列中没有数据了, 需要读取一个新的 page 进来
            if (readState.valuesToReadInPage == 0) {
                int pageValueCount = readPage();
                // 返回当前 page 的数据量
                if (pageValueCount < 0) {
                    // we've read all the pages; this could happen when we're reading a repeated list and we
                    // don't know where the list will end until we've seen all the pages.
                    break;
                }
                readState.resetForNewPage(pageValueCount, pageFirstRowIndex);
            }


            long pageRowId = readState.rowId;
            int leftInBatch = readState.rowsToReadInBatch;  // 当前批次要读取的数据量
            int leftInPage = readState.valuesToReadInPage;  // 当前Page的可读数据量

            int readBatch = Math.min(leftInBatch, leftInPage);  // 记录当前一次性能读取的数据量

            // 获取当前数据范围的起始位置
            long rangeStart = readState.currentRangeStart();
            // 获取当前数据范围的结束位置
            long rangeEnd = readState.currentRangeEnd();

            if(pageRowId > rangeEnd){ // 数据范围无效，获取下一个数据范围
                readState.nextRange();
            }else if (pageRowId < rangeStart){  // 有无效数据
                int toSkip = (int)(rangeStart - pageRowId);

                if (toSkip >= leftInPage) {  // 所有数据都抛弃
                    pageRowId += leftInPage;
                    leftInPage = 0;
                }else {
                    if (isCurrentPageDictionaryEncoded) {
                        runLenDecoder.skipDictionaryIds(toSkip, maxDefLevel, this.dictionaryIdsDecoder);    // 跳过无效数据
                        pageRowId += toSkip;
                        leftInPage -= toSkip;
                    }else{
                        skipBatch(toSkip);    // 跳过无效数据
                        pageRowId += toSkip;
                        leftInPage -= toSkip;
                    }
                }
            }else{
                // 重新定义当前批次的数据读取范围
                long start = pageRowId;       // 数据读取的有效开始位置
                long end = Math.min(rangeEnd, pageRowId + readBatch -1);   // 数据读取的有效结束位置
                int num  = (int) (end - start +1);


                try {
                    if (isCurrentPageDictionaryEncoded) {
                        if (num > 0){
                            // Read and decode dictionary ids.
                            runLenDecoder.readDictionaryIds(
                                    num, dictionaryIds, vector, rowId, maxDefLevel, this.dictionaryIdsDecoder);

//                            readBatchFromDictionaryIds(rowId, num, vector, dictionaryIds);

                            if (vector.hasDictionary() || (rowId == 0 && supportLazyDecode())) {
                                // 列向量支持懒加载字典值，因此只需设置字典即可。
                                // 如果 rowId != 0 且该列没有字典（即已经添加了一些非字典编码的值），则无法执行此操作。
                                vector.setDictionary(new ParquetDictionary(dictionary));
                            } else {
                                readBatchFromDictionaryIds(rowId, num, vector, dictionaryIds);
                            }
                        }

                    } else {
                        if (num > 0){
                            if (vector.hasDictionary() && rowId != 0) {
                                // This batch already has dictionary encoded values but this new page is not.
                                // The batch does not support a mix of dictionary and not so we will decode the dictionary.
                                readBatchFromDictionaryIds(0, rowId, vector, vector.getDictionaryIds());
                            }
                            vector.setDictionary(null);
                            readBatch(rowId, num, vector);
                        }

                    }
                }catch (Exception e){
                    throw new IOException(e);
                }

                leftInBatch -= num;
                pageRowId += num;
                leftInPage -= num;
                rowId += num;
            }

            readState.rowsToReadInBatch = leftInBatch;
            readState.valuesToReadInPage = leftInPage;
            readState.rowId = pageRowId;
        }
    }

    private int readPage() {
        DataPage page = pageReader.readPage();
        if (page == null) {
            return -1;
        }
        this.pageFirstRowIndex = page.getFirstRowIndex().orElse(0L);

        return page.accept(new DataPage.Visitor<Integer>() {
            @Override
            public Integer visit(DataPageV1 dataPageV1) {
                try {
                    return readPageV1(dataPageV1);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Integer visit(DataPageV2 dataPageV2) {
                try {
                    return readPageV2(dataPageV2);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private int readPageV1(DataPageV1 page) throws IOException {
        int pageValueCount = page.getValueCount();  // 重新计算当前 pageValue 的数量

        ValuesReader rlReader = page.getRlEncoding().getValuesReader(descriptor, REPETITION_LEVEL);

        // Initialize the decoders.
        if (page.getDlEncoding() != Encoding.RLE && descriptor.getMaxDefinitionLevel() != 0) {
            throw new UnsupportedOperationException(
                    "Unsupported encoding: " + page.getDlEncoding());
        }
        int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
        this.runLenDecoder = new RunLengthDecoder(bitWidth);
        try {
            BytesInput bytes = page.getBytes();
            ByteBufferInputStream in = bytes.toInputStream();
            rlReader.initFromPage(pageValueCount, in);
            this.runLenDecoder.initFromStream(pageValueCount, in);
            prepareNewPage(page.getValueEncoding(), in, pageValueCount);
            return pageValueCount;
        } catch (IOException e) {
            throw new IOException("could not read page " + page + " in col " + descriptor, e);
        }
    }

    private int readPageV2(DataPageV2 page) throws IOException {
        int pageValueCount = page.getValueCount();

        int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
        // do not read the length from the stream. v2 pages handle dividing the page bytes.
        this.runLenDecoder = new RunLengthDecoder(bitWidth, false);
        this.runLenDecoder.initFromStream(
                pageValueCount, page.getDefinitionLevels().toInputStream());
        try {
            prepareNewPage(page.getDataEncoding(), page.getData().toInputStream(), pageValueCount);
            return pageValueCount;
        } catch (IOException e) {
            throw new IOException("could not read page " + page + " in col " + descriptor, e);
        }
    }

    private void prepareNewPage(Encoding dataEncoding, ByteBufferInputStream in, int pageValueCount)
            throws IOException {

        if (dataEncoding.usesDictionary()) {   // 如果是使用了字典编码
            if (dictionary == null) {
                throw new IOException("could not read page in col " + descriptor + " as the dictionary was missing for encoding " + dataEncoding);
            }
            @SuppressWarnings("deprecation")
            Encoding plainDict = Encoding.PLAIN_DICTIONARY; // var to allow warning suppression
            if (dataEncoding != plainDict && dataEncoding != Encoding.RLE_DICTIONARY) {
                throw new UnsupportedOperationException("Unsupported encoding: " + dataEncoding);
            }
            this.dataInputStream = null;
            this.dictionaryIdsDecoder = new RunLengthDecoder();
            try {
                this.dictionaryIdsDecoder.initFromStream(pageValueCount, in);
            } catch (IOException e) {
                throw new IOException("could not read dictionary in col " + descriptor, e);
            }
            this.isCurrentPageDictionaryEncoded = true;
        } else {   // 如果没有使用字典编码
            if (dataEncoding != Encoding.PLAIN) {
                throw new UnsupportedOperationException("Unsupported encoding: " + dataEncoding);
            }
            this.dictionaryIdsDecoder = null;
            LOG.debug("init from page at offset {} for length {}", in.position(), in.available());
            this.dataInputStream = in.remainingStream();
            this.isCurrentPageDictionaryEncoded = false;
        }

        afterReadPage();
    }

    final ByteBuffer readDataBuffer(int length) {
        try {
            return dataInputStream.slice(length).order(ByteOrder.LITTLE_ENDIAN);
        } catch (IOException e) {
            throw new ParquetDecodingException("Failed to read " + length + " bytes", e);
        }
    }

    final void skipDataBuffer(int length){
        try {
            dataInputStream.skipFully(length);
        } catch (IOException e) {
            throw new ParquetDecodingException("Failed to read " + length + " bytes", e);
        }
    }

    /** After read a page, we may need some initialization. */
    protected void afterReadPage() {}

    /**
     * Support lazy dictionary ids decode. See more in {@link ParquetDictionary}. If return false,
     * we will decode all the data first.
     */
    protected boolean supportLazyDecode() {
        return true;
    }

    /** Read batch from {@link #runLenDecoder} and {@link #dataInputStream}. */
    protected abstract void readBatch(int rowId, int num, VECTOR column);

    protected abstract void skipBatch(int num);

    /**
     * Decode dictionary ids to data. From {@link #runLenDecoder} and {@link #dictionaryIdsDecoder}.
     */
    protected abstract void readBatchFromDictionaryIds(
            int rowId, int num, VECTOR column, WritableIntVector dictionaryIds);
}
