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
import org.apache.paimon.types.DataType;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.*;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.apache.parquet.column.ValuesType.DEFINITION_LEVEL;
import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;
import static org.apache.parquet.column.ValuesType.VALUES;

/**
 * 这是一种列级 Parquet 阅读器，用于读取一批列的记录。部分代码参考自 Apache Hive 和 Apache Parquet。
 */
public abstract class BaseVectorizedColumnReader implements ColumnReader<WritableColumnVector> {

    private static final Logger LOG = LoggerFactory.getLogger(BaseVectorizedColumnReader.class);

    protected boolean isUtcTimestamp;


    /** The dictionary, if this column has dictionary encoding. */
    protected final ParquetDataColumnReader dictionary;

    /** If true, the current page is dictionary encoded. */
    protected boolean isCurrentPageDictionaryEncoded;

    /** Maximum definition level for this column. */
    protected final int maxDefLevel;

    protected int definitionLevel;
    protected int repetitionLevel;

    /** Repetition/Definition/Value readers. */
    protected IntIterator repetitionLevelColumn;

    protected IntIterator definitionLevelColumn;
    protected ParquetDataColumnReader dataColumn;

    /** Total values in the current page. */
    protected int pageValueCount;

    /**
     * Helper struct to track intermediate states while reading Parquet pages in the column chunk.
     * 用于在读取列块中的 Parquet 页面时跟踪中间状态的辅助结构体。
     */
    protected final ParquetReadState readState;

    protected final PageReader pageReader;
    protected final ColumnDescriptor descriptor;
    protected final Type type;
    protected final DataType dataType;

    public BaseVectorizedColumnReader(
            ColumnDescriptor descriptor,
            PageReadStore pageReadStore,
            boolean isUtcTimestamp,
            Type parquetType,
            DataType dataType)
            throws IOException {
        this.descriptor = descriptor;
        this.type = parquetType;
        this.pageReader = pageReadStore.getPageReader(descriptor);
        this.maxDefLevel = descriptor.getMaxDefinitionLevel();
        this.isUtcTimestamp = isUtcTimestamp;
        this.dataType = dataType;

        this.readState = new ParquetReadState(pageReadStore.getRowIndexes().orElse(null));

        DictionaryPage dictionaryPage = pageReader.readDictionaryPage();  // 初始化字典编码
        if (dictionaryPage != null) {
            try {
                this.dictionary =
                        ParquetDataColumnReaderFactory.getDataColumnReaderByTypeOnDictionary(
                                parquetType.asPrimitiveType(),
                                dictionaryPage
                                        .getEncoding()
                                        .initDictionary(descriptor, dictionaryPage),
                                isUtcTimestamp);
                this.isCurrentPageDictionaryEncoded = true;
            } catch (IOException e) {
                throw new IOException(
                        String.format("Could not decode the dictionary for %s", descriptor), e);
            }
        } else {
            this.dictionary = null;
            this.isCurrentPageDictionaryEncoded = false;
        }
    }

    protected void readRepetitionAndDefinitionLevels() {
        repetitionLevel = repetitionLevelColumn.nextInt();
        definitionLevel = definitionLevelColumn.nextInt();
    }

    protected int readPage() {
        DataPage page = pageReader.readPage();

        if (page == null) {
            return -1;
        }

        long pageFirstRowIndex = page.getFirstRowIndex().orElse(0L);

        if (pageFirstRowIndex == -1){
            System.out.println("=====");
        }

        int pageValueCount =  page.accept(
                new DataPage.Visitor<Integer>() {
                    @Override
                    public Integer visit(DataPageV1 dataPageV1) {
                        return readPageV1(dataPageV1);
                    }
                    @Override
                    public Integer visit(DataPageV2 dataPageV2) {
                        return readPageV2(dataPageV2);
                    }
                });

        readState.resetForNewPage(pageValueCount, pageFirstRowIndex);

        return pageValueCount;
    }

    private void initDataReader(Encoding dataEncoding, ByteBufferInputStream in)
            throws IOException {

//        this.endOfPageValueCount = valuesRead + pageValueCount;
        if (dataEncoding.usesDictionary()) {
            this.dataColumn = null;
            if (dictionary == null) {
                throw new IOException(
                        String.format(
                                "Could not read page in col %s because the dictionary was missing for encoding %s.",
                                descriptor, dataEncoding));
            }
            dataColumn =
                    ParquetDataColumnReaderFactory.getDataColumnReaderByType(
                            type.asPrimitiveType(),
                            dataEncoding.getDictionaryBasedValuesReader(
                                    descriptor, VALUES, dictionary.getDictionary()),
                            isUtcTimestamp);
            this.isCurrentPageDictionaryEncoded = true;
        } else {
            dataColumn =
                    ParquetDataColumnReaderFactory.getDataColumnReaderByType(
                            type.asPrimitiveType(),
                            dataEncoding.getValuesReader(descriptor, VALUES),
                            isUtcTimestamp);
            this.isCurrentPageDictionaryEncoded = false;
        }

        try {
            dataColumn.initFromPage(pageValueCount, in);
        } catch (IOException e) {
            throw new IOException(String.format("Could not read page in col %s.", descriptor), e);
        }
    }

    private int readPageV1(DataPageV1 page) {
        int pageValueCount = page.getValueCount();
        ValuesReader rlReader = page.getRlEncoding().getValuesReader(descriptor, REPETITION_LEVEL);
        ValuesReader dlReader = page.getDlEncoding().getValuesReader(descriptor, DEFINITION_LEVEL);
        this.repetitionLevelColumn = new ValuesReaderIntIterator(rlReader);
        this.definitionLevelColumn = new ValuesReaderIntIterator(dlReader);
        try {
            BytesInput bytes = page.getBytes();
            LOG.debug("Page size {}  bytes and {} records.", bytes.size(), pageValueCount);
            ByteBufferInputStream in = bytes.toInputStream();
            LOG.debug("Reading repetition levels at {}.", in.position());
            rlReader.initFromPage(pageValueCount, in);
            LOG.debug("Reading definition levels at {}.", in.position());
            dlReader.initFromPage(pageValueCount, in);
            LOG.debug("Reading data at {}.", in.position());
            initDataReader(page.getValueEncoding(), in);
            return pageValueCount;
        } catch (IOException e) {
            throw new ParquetDecodingException(
                    String.format("Could not read page %s in col %s.", page, descriptor), e);
        }
    }

    private int readPageV2(DataPageV2 page) {
        int pageValueCount = page.getValueCount();
        this.repetitionLevelColumn =
                newRLEIterator(descriptor.getMaxRepetitionLevel(), page.getRepetitionLevels());
        this.definitionLevelColumn =
                newRLEIterator(descriptor.getMaxDefinitionLevel(), page.getDefinitionLevels());
        try {
            LOG.debug(
                    "Page data size {} bytes and {} records.",
                    page.getData().size(),
                    pageValueCount);
            initDataReader(page.getDataEncoding(), page.getData().toInputStream());
            return pageValueCount;
        } catch (IOException e) {
            throw new ParquetDecodingException(
                    String.format("Could not read page %s in col %s.", page, descriptor), e);
        }
    }

    private IntIterator newRLEIterator(int maxLevel, BytesInput bytes) {
        try {
            if (maxLevel == 0) {
                return new NullIntIterator();
            }
            return new RLEIntIterator(
                    new RunLengthBitPackingHybridDecoder(
                            BytesUtils.getWidthFromMaxInt(maxLevel),
                            new ByteArrayInputStream(bytes.toByteArray())));
        } catch (IOException e) {
            throw new ParquetDecodingException(
                    String.format("Could not read levels in page for col %s.", descriptor), e);
        }
    }

    /** Utility interface to abstract over different way to read ints with different encodings. */
    interface IntIterator {
        int nextInt();
    }

    /** Reading int from {@link ValuesReader}. */
    protected static final class ValuesReaderIntIterator implements IntIterator {
        ValuesReader delegate;

        public ValuesReaderIntIterator(ValuesReader delegate) {
            this.delegate = delegate;
        }

        @Override
        public int nextInt() {
            return delegate.readInteger();
        }
    }

    /** Reading int from {@link RunLengthBitPackingHybridDecoder}. */
    protected static final class RLEIntIterator implements IntIterator {
        RunLengthBitPackingHybridDecoder delegate;

        public RLEIntIterator(RunLengthBitPackingHybridDecoder delegate) {
            this.delegate = delegate;
        }

        @Override
        public int nextInt() {
            try {
                return delegate.readInt();
            } catch (IOException e) {
                throw new ParquetDecodingException(e);
            }
        }
    }

    /** Reading zero always. */
    protected static final class NullIntIterator implements IntIterator {
        @Override
        public int nextInt() {
            return 0;
        }
    }
}
