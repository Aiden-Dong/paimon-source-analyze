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

package org.apache.paimon.io;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.ApplyDeletionVectorReader;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatKey;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.OrcFormatReaderContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.AsyncRecordReader;
import org.apache.paimon.utils.BulkFormatMapping;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Projection;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/****
 * 工厂，用于创建 {@link RecordReader} 以读取 {@link KeyValue} 文件。
 **/
public class KeyValueFileReaderFactory implements FileReaderFactory<KeyValue> {

    private final FileIO fileIO;                   // 文件操作句柄 IO
    private final SchemaManager schemaManager;     // 表结构管理
    private final TableSchema schema;              // 表结构
    private final RowType keyType;                 // key 字段类型
    private final RowType valueType;               // value 字段类型

    private final BulkFormatMapping.BulkFormatMappingBuilder bulkFormatMappingBuilder;
    private final DataFilePathFactory pathFactory;
    private final long asyncThreshold;

    private final Map<FormatKey, BulkFormatMapping> bulkFormatMappings;
    private final BinaryRow partition;
    private final DeletionVector.Factory dvFactory;

    private KeyValueFileReaderFactory(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            RowType keyType,
            RowType valueType,
            BulkFormatMapping.BulkFormatMappingBuilder bulkFormatMappingBuilder,
            DataFilePathFactory pathFactory,
            long asyncThreshold,
            BinaryRow partition,
            DeletionVector.Factory dvFactory) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.keyType = keyType;
        this.valueType = valueType;
        this.bulkFormatMappingBuilder = bulkFormatMappingBuilder;
        this.pathFactory = pathFactory;
        this.asyncThreshold = asyncThreshold;
        this.partition = partition;
        this.bulkFormatMappings = new HashMap<>();
        this.dvFactory = dvFactory;
    }

    @Override
    public RecordReader<KeyValue> createRecordReader(DataFileMeta file) throws IOException {
        return createRecordReader(file.schemaId(), file.fileName(), file.fileSize(), file.level());
    }

    public RecordReader<KeyValue> createRecordReader(long schemaId, String fileName, long fileSize, int level) throws IOException {
        // orc 文件读取方式
        if (fileSize >= asyncThreshold && fileName.endsWith("orc")) {
            return new AsyncRecordReader<>(
                    () -> createRecordReader(schemaId, fileName, level, false, 2, fileSize));
        }

        return createRecordReader(schemaId, fileName, level, true, null, fileSize);
    }

    private RecordReader<KeyValue> createRecordReader(
            long schemaId,
            String fileName,
            int level,
            boolean reuseFormat,
            @Nullable Integer orcPoolSize,
            long fileSize)
            throws IOException {

        // 文件格式标记
        String formatIdentifier = DataFilePathFactory.formatIdentifier(fileName);

        Supplier<BulkFormatMapping> formatSupplier =
                () ->
                        bulkFormatMappingBuilder.build(
                                formatIdentifier,
                                schema,
                                schemaId == schema.id() ? schema : schemaManager.schema(schemaId));

        BulkFormatMapping bulkFormatMapping = reuseFormat
                        ? bulkFormatMappings.computeIfAbsent(
                                new FormatKey(schemaId, formatIdentifier),
                                key -> formatSupplier.get())
                        : formatSupplier.get();
        Path filePath = pathFactory.toPath(fileName);

        RecordReader<InternalRow> fileRecordReader = new FileRecordReader(
                        bulkFormatMapping.getReaderFactory(),
                        orcPoolSize == null
                                ? new FormatReaderContext(fileIO, filePath, fileSize)
                                : new OrcFormatReaderContext(fileIO, filePath, fileSize, orcPoolSize),
                        bulkFormatMapping.getIndexMapping(),
                        bulkFormatMapping.getCastMapping(),
                        PartitionUtils.create(bulkFormatMapping.getPartitionPair(), partition));

        Optional<DeletionVector> deletionVector = dvFactory.create(fileName);
        if (deletionVector.isPresent() && !deletionVector.get().isEmpty()) {
            fileRecordReader = new ApplyDeletionVectorReader<>(fileRecordReader, deletionVector.get());
        }

        return new KeyValueDataFileRecordReader(fileRecordReader, keyType, valueType, level);
    }

    public static Builder builder(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            RowType keyType,
            RowType valueType,
            FileFormatDiscover formatDiscover,
            FileStorePathFactory pathFactory,
            KeyValueFieldsExtractor extractor,
            CoreOptions options) {
        return new Builder(
                fileIO,
                schemaManager,
                schema,
                keyType,
                valueType,
                formatDiscover,
                pathFactory,
                extractor,
                options);
    }

    /** Builder for {@link KeyValueFileReaderFactory}. */
    public static class Builder {

        private final FileIO fileIO;
        private final SchemaManager schemaManager;
        private final TableSchema schema;
        private final RowType keyType;
        private final RowType valueType;
        private final FileFormatDiscover formatDiscover;
        private final FileStorePathFactory pathFactory;
        private final KeyValueFieldsExtractor extractor;
        private final int[][] fullKeyProjection;
        private final CoreOptions options;

        private int[][] keyProjection;
        private int[][] valueProjection;
        private RowType projectedKeyType;
        private RowType projectedValueType;

        private Builder(
                FileIO fileIO,
                SchemaManager schemaManager,
                TableSchema schema,
                RowType keyType,
                RowType valueType,
                FileFormatDiscover formatDiscover,
                FileStorePathFactory pathFactory,
                KeyValueFieldsExtractor extractor,
                CoreOptions options) {
            this.fileIO = fileIO;
            this.schemaManager = schemaManager;
            this.schema = schema;
            this.keyType = keyType;
            this.valueType = valueType;
            this.formatDiscover = formatDiscover;
            this.pathFactory = pathFactory;
            this.extractor = extractor;

            this.fullKeyProjection = Projection.range(0, keyType.getFieldCount()).toNestedIndexes();
            this.options = options;
            this.keyProjection = fullKeyProjection;
            this.valueProjection = Projection.range(0, valueType.getFieldCount()).toNestedIndexes();
            applyProjection();
        }

        public Builder copyWithoutProjection() {
            return new Builder(
                    fileIO,
                    schemaManager,
                    schema,
                    keyType,
                    valueType,
                    formatDiscover,
                    pathFactory,
                    extractor,
                    options);
        }

        public Builder withKeyProjection(int[][] projection) {
            keyProjection = projection;
            applyProjection();
            return this;
        }

        public Builder withValueProjection(int[][] projection) {
            valueProjection = projection;
            applyProjection();
            return this;
        }

        public RowType keyType() {
            return keyType;
        }

        public RowType projectedValueType() {
            return projectedValueType;
        }

        public KeyValueFileReaderFactory build(
                BinaryRow partition, int bucket, DeletionVector.Factory dvFactory) {
            return build(partition, bucket, dvFactory, true, Collections.emptyList());
        }

        public KeyValueFileReaderFactory build(
                BinaryRow partition,
                int bucket,
                DeletionVector.Factory dvFactory,
                boolean projectKeys,
                @Nullable List<Predicate> filters) {
            int[][] keyProjection = projectKeys ? this.keyProjection : fullKeyProjection;
            RowType projectedKeyType = projectKeys ? this.projectedKeyType : keyType;

            return new KeyValueFileReaderFactory(
                    fileIO,
                    schemaManager,
                    schema,
                    projectedKeyType,
                    projectedValueType,
                    BulkFormatMapping.newBuilder(formatDiscover, extractor, keyProjection, valueProjection, filters),
                    pathFactory.createDataFilePathFactory(partition, bucket),
                    options.fileReaderAsyncThreshold().getBytes(),
                    partition,
                    dvFactory);
        }

        private void applyProjection() {
            projectedKeyType = Projection.of(keyProjection).project(keyType);
            projectedValueType = Projection.of(valueProjection).project(valueType);
        }

        public FileIO fileIO() {
            return fileIO;
        }
    }
}
