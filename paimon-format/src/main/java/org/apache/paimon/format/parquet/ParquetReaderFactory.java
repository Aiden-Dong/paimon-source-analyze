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

package org.apache.paimon.format.parquet;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.data.columnar.ColumnarRowIterator;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.parquet.reader.ColumnReader;
import org.apache.paimon.format.parquet.reader.ParquetDecimalVector;
import org.apache.paimon.format.parquet.reader.ParquetTimestampVector;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReader.RecordIterator;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pool;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.format.parquet.reader.ParquetSplitReaderUtil.createColumnReader;
import static org.apache.paimon.format.parquet.reader.ParquetSplitReaderUtil.createWritableColumnVector;
import static org.apache.parquet.hadoop.UnmaterializableRecordCounter.BAD_RECORD_THRESHOLD_CONF_KEY;

/**
 * Parquet {@link FormatReaderFactory}，用于以矢量化模式从文件读取数据到 {@link VectorizedColumnBatch}。
 */
public class ParquetReaderFactory implements FormatReaderFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetReaderFactory.class);

    private static final long serialVersionUID = 1L;

    private static final String ALLOCATION_SIZE = "parquet.read.allocation.size";

    private final Options conf;
    private final String[] projectedFields;
    private final DataType[] projectedTypes;
    private final int batchSize;
    private final Set<Integer> unknownFieldsIndices = new HashSet<>();

    private final FilterCompat.Filter filter;

    public ParquetReaderFactory(Options conf, RowType projectedType, int batchSize, FilterCompat.Filter filter) {
        this.conf = conf;
        this.projectedFields = projectedType.getFieldNames().toArray(new String[0]);
        this.projectedTypes = projectedType.getFieldTypes().toArray(new DataType[0]);
        this.batchSize = batchSize;
        this.filter = filter;
    }

    @Override
    public ParquetReader createReader(FormatReaderFactory.Context context) throws IOException {
        // Paruqet Reader 配置初始化
        ParquetReadOptions.Builder builder =
                ParquetReadOptions.builder().withRange(0, context.fileSize());

        setReadOptions(builder);

        // 构建 parquet reader
        ParquetFileReader reader = new ParquetFileReader(
                        ParquetInputFile.fromPath(context.fileIO(), context.filePath()),
                        builder.build());


        MessageType fileSchema = reader.getFileMetaData().getSchema();   // 获取文件元信息
        MessageType requestedSchema = clipParquetSchema(fileSchema);     // 只从文件中获取到查询的列
        reader.setRequestedSchema(requestedSchema);  // 将查询列应用到 parquet reader 中

        checkSchema(fileSchema, requestedSchema);

        Pool<ParquetReaderBatch> poolOfBatches =
                createPoolOfBatches(context.filePath(), requestedSchema);

        return new ParquetReader(reader, requestedSchema, reader.getFilteredRecordCount(), poolOfBatches);
    }

    private void setReadOptions(ParquetReadOptions.Builder builder) {
        builder.useSignedStringMinMax(
                conf.getBoolean("parquet.strings.signed-min-max.enabled", false));
        builder.useDictionaryFilter(
                conf.getBoolean(ParquetInputFormat.DICTIONARY_FILTERING_ENABLED, true));
        builder.useStatsFilter(conf.getBoolean(ParquetInputFormat.STATS_FILTERING_ENABLED, true));
        builder.useRecordFilter(conf.getBoolean(ParquetInputFormat.RECORD_FILTERING_ENABLED, true));
        builder.useColumnIndexFilter(
                conf.getBoolean(ParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED, true));
        builder.usePageChecksumVerification(
                conf.getBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, false));
        builder.useBloomFilter(conf.getBoolean(ParquetInputFormat.BLOOM_FILTERING_ENABLED, true));
        builder.withMaxAllocationInBytes(conf.getInteger(ALLOCATION_SIZE, 8388608));
        String badRecordThresh = conf.getString(BAD_RECORD_THRESHOLD_CONF_KEY, null);
        if (badRecordThresh != null) {
            builder.set(BAD_RECORD_THRESHOLD_CONF_KEY, badRecordThresh);
        }

        builder.withRecordFilter(this.filter);
    }

    /** Clips `parquetSchema` according to `fieldNames`. */
    private MessageType clipParquetSchema(GroupType parquetSchema) {
        Type[] types = new Type[projectedFields.length];
        for (int i = 0; i < projectedFields.length; ++i) {
            String fieldName = projectedFields[i];
            if (!parquetSchema.containsField(fieldName)) {
                LOG.warn("{} does not exist in {}, will fill the field with null.", fieldName, parquetSchema);
                types[i] = ParquetSchemaConverter.convertToParquetType(fieldName, projectedTypes[i]);
                unknownFieldsIndices.add(i);
            } else {
                types[i] = parquetSchema.getType(fieldName);
            }
        }

        return Types.buildMessage().addFields(types).named("paimon-parquet");
    }

    private void checkSchema(MessageType fileSchema, MessageType requestedSchema)
            throws IOException, UnsupportedOperationException {

        if (projectedFields.length != requestedSchema.getFieldCount()) {
            throw new RuntimeException("The quality of field type is incompatible with the request schema!");
        }

        /*
         * Check that the requested schema is supported.
         */
        for (int i = 0; i < requestedSchema.getFieldCount(); ++i) {
            String[] colPath = requestedSchema.getPaths().get(i);
            if (fileSchema.containsPath(colPath)) {
                ColumnDescriptor fd = fileSchema.getColumnDescription(colPath);
                if (!fd.equals(requestedSchema.getColumns().get(i))) {
                    throw new UnsupportedOperationException("Schema evolution not supported.");
                }
            } else {
                if (requestedSchema.getColumns().get(i).getMaxDefinitionLevel() == 0) {
                    // Column is missing in data but the required data is non-nullable. This file is
                    // invalid.
                    throw new IOException("Required column is missing in data file. Col: " + Arrays.toString(colPath));
                }
            }
        }
    }

    private Pool<ParquetReaderBatch> createPoolOfBatches(Path filePath, MessageType requestedSchema) {
        // In a VectorizedColumnBatch, the dictionary will be lazied deserialized.
        // If there are multiple batches at the same time, there may be thread safety problems,
        // because the deserialization of the dictionary depends on some internal structures.
        // We need set poolCapacity to 1.
        Pool<ParquetReaderBatch> pool = new Pool<>(1);
        pool.add(createReaderBatch(filePath, requestedSchema, pool.recycler()));
        return pool;
    }

    private ParquetReaderBatch createReaderBatch(Path filePath, MessageType requestedSchema, Pool.Recycler<ParquetReaderBatch> recycler) {

        WritableColumnVector[] writableVectors = createWritableVectors(requestedSchema);
        VectorizedColumnBatch columnarBatch = createVectorizedColumnBatch(writableVectors);
        return createReaderBatch(filePath, writableVectors, columnarBatch, recycler);
    }

    private WritableColumnVector[] createWritableVectors(MessageType requestedSchema) {
        WritableColumnVector[] columns = new WritableColumnVector[projectedTypes.length];
        List<Type> types = requestedSchema.getFields();

        for (int i = 0; i < projectedTypes.length; i++) {
            columns[i] = createWritableColumnVector(batchSize, projectedTypes[i], types.get(i), requestedSchema.getColumns(), 0);
        }

        return columns;
    }

    /**
     * Create readable vectors from writable vectors. Especially for decimal, see {@link
     * ParquetDecimalVector}.
     */
    private VectorizedColumnBatch createVectorizedColumnBatch(
            WritableColumnVector[] writableVectors) {
        ColumnVector[] vectors = new ColumnVector[writableVectors.length];
        for (int i = 0; i < writableVectors.length; i++) {
            switch (projectedTypes[i].getTypeRoot()) {
                case DECIMAL:
                    vectors[i] = new ParquetDecimalVector(writableVectors[i]);
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    vectors[i] = new ParquetTimestampVector(writableVectors[i]);
                    break;
                default:
                    vectors[i] = writableVectors[i];
            }
        }

        return new VectorizedColumnBatch(vectors);
    }

    private class ParquetReader implements RecordReader<InternalRow> {

        private ParquetFileReader reader;             // Parquet 文件读取工具,原生
        private final MessageType requestedSchema;    // 用于读取列的 Schema

        // FileReader 总行数。
        private final long totalRowCount;

        private final Pool<ParquetReaderBatch> pool;    // 将parquet 数据转成批读的工具

        // 已经返回的行数
        private long rowsReturned;

        // 已读取的行数，包括当前正在处理的行组。
        private long totalCountLoadedSoFar;

        // 当前行的文件位置。
        private long currentRowPosition;

        // 对于每个请求列，读取此列的读取器。
        // 如果此列在文件中缺失，则为 NULL，在这种情况下，我们将使用 NULL 填充属性。
        @SuppressWarnings("rawtypes")
        private ColumnReader[] columnReaders;

        private ParquetReader(
                ParquetFileReader reader,
                MessageType requestedSchema,
                long totalRowCount,
                Pool<ParquetReaderBatch> pool) {
            this.reader = reader;
            this.requestedSchema = requestedSchema;
            this.totalRowCount = totalRowCount;
            this.pool = pool;
            this.rowsReturned = 0;
            this.totalCountLoadedSoFar = 0;
            this.currentRowPosition = 0;
        }

        @Nullable
        @Override
        public RecordIterator<InternalRow> readBatch() throws IOException {
            final ParquetReaderBatch batch = getCachedEntry();

            long rowNumber = currentRowPosition;
            if (!nextBatch(batch)) {
                batch.recycle();
                return null;
            }

            return batch.convertAndGetIterator(rowNumber);
        }

        /** Advances to the next batch of rows. Returns false if there are no more. */
        private boolean nextBatch(ParquetReaderBatch batch) throws IOException {
            for (WritableColumnVector v : batch.writableVectors) {
                v.reset();
            }
            batch.columnarBatch.setNumRows(0);

            if (rowsReturned >= totalRowCount) {
                return false;
            }

            if (rowsReturned == totalCountLoadedSoFar) {
                readNextRowGroup();
            }


            // 可能收到 batchSize 的影响，会读取多次出来
            int num = (int) Math.min(batchSize, totalCountLoadedSoFar - rowsReturned);

            for (int i = 0; i < columnReaders.length; ++i) {
                if (columnReaders[i] == null) {
                    batch.writableVectors[i].fillWithNulls();
                } else {
                    //noinspection unchecked
                    columnReaders[i].readToVector(num, batch.writableVectors[i]);
                }
            }

            rowsReturned += num;
            currentRowPosition += num;
            batch.columnarBatch.setNumRows(num);
            return true;
        }

        private void readNextRowGroup() throws IOException {
            PageReadStore pages = reader.readNextFilteredRowGroup();

            if (pages == null) {
                throw new IOException("expecting more rows but reached last block. Read " + rowsReturned + " out of " + totalRowCount);
            }

            List<Type> types = requestedSchema.getFields();
            columnReaders = new ColumnReader[types.size()];
            for (int i = 0; i < types.size(); ++i) {
                if (!unknownFieldsIndices.contains(i)) {
                    columnReaders[i] = createColumnReader(projectedTypes[i], types.get(i), requestedSchema.getColumns(), pages, 0);
                }
            }
            totalCountLoadedSoFar += pages.getRowCount();
        }

        private ParquetReaderBatch getCachedEntry() throws IOException {
            try {
                return pool.pollEntry();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted");
            }
        }

        @Override
        public void close() throws IOException {
            if (reader != null) {
                reader.close();
                reader = null;
            }
        }
    }

    private ParquetReaderBatch createReaderBatch(
            Path filePath,
            WritableColumnVector[] writableVectors,
            VectorizedColumnBatch columnarBatch,
            Pool.Recycler<ParquetReaderBatch> recycler) {
        return new ParquetReaderBatch(filePath, writableVectors, columnarBatch, recycler);
    }

    private static class ParquetReaderBatch {

        private final WritableColumnVector[] writableVectors;
        protected final VectorizedColumnBatch columnarBatch;
        private final Pool.Recycler<ParquetReaderBatch> recycler;

        private final ColumnarRowIterator result;

        protected ParquetReaderBatch(Path filePath,
                WritableColumnVector[] writableVectors,
                VectorizedColumnBatch columnarBatch,
                Pool.Recycler<ParquetReaderBatch> recycler) {
            this.writableVectors = writableVectors;
            this.columnarBatch = columnarBatch;
            this.recycler = recycler;
            this.result = new ColumnarRowIterator(filePath, new ColumnarRow(columnarBatch), this::recycle);
        }

        public void recycle() {
            recycler.recycle(this);
        }

        public RecordIterator<InternalRow> convertAndGetIterator(long rowNumber) {
            result.reset(rowNumber);
            return result;
        }
    }
}
