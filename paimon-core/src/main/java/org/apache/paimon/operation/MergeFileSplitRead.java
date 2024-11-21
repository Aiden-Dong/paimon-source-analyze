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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.mergetree.DropDeleteReader;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.MergeTreeReaders;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.mergetree.compact.ConcatRecordReader.ReaderSupplier;
import org.apache.paimon.mergetree.compact.IntervalPartition;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory.AdjustedProjection;
import org.apache.paimon.mergetree.compact.MergeFunctionWrapper;
import org.apache.paimon.mergetree.compact.ReducerMergeFunctionWrapper;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Projection;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.io.DataFilePathFactory.CHANGELOG_FILE_PREFIX;
import static org.apache.paimon.predicate.PredicateBuilder.containsFields;
import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;

/**
 * {@link KeyValueFileStore} 的实现，该类处理 LSM 合并和变更日志行类型的操作
 * 它将强制读取如 sequence 和 row_kind 等字段。
 *
 * @see RawFileSplitRead 如果处于批处理模式并且读取原始文件，推荐使用这种读取方式。
 */
public class MergeFileSplitRead implements SplitRead<KeyValue> {

    private final TableSchema tableSchema;           // 当前表的 Schema 信息

    private final FileIO fileIO;                     // 用于读写的文件操作句柄

    private final KeyValueFileReaderFactory.Builder readerFactoryBuilder;
    private final Comparator<InternalRow> keyComparator;                    // Key 比较器 RecordComparator
    private final MergeFunctionFactory<KeyValue> mfFactory;                 // 合并函数工厂

    // 合并排序器，用于对具有键重叠的读取器进行排序和合并。
    private final MergeSorter mergeSorter;

    // sequence.field  - 生成主键表序列号的字段,序列号决定了哪个数据是最新的
    private final List<String> sequenceFields;

    @Nullable private int[][] keyProjectedFields;

    @Nullable private List<Predicate> filtersForKeys;          // key 过滤器
    @Nullable private List<Predicate> filtersForAll;           // key value 过滤器

    @Nullable private int[][] pushdownProjection;
    @Nullable private int[][] outerProjection;

    private boolean forceKeepDelete = false;

    public MergeFileSplitRead(
            CoreOptions options,
            TableSchema schema,
            RowType keyType,
            RowType valueType,
            Comparator<InternalRow> keyComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            KeyValueFileReaderFactory.Builder readerFactoryBuilder) {
        this.tableSchema = schema;
        this.readerFactoryBuilder = readerFactoryBuilder;
        this.fileIO = readerFactoryBuilder.fileIO();
        this.keyComparator = keyComparator;
        this.mfFactory = mfFactory;

        this.mergeSorter = new MergeSorter(CoreOptions.fromMap(tableSchema.options()), keyType, valueType, null);
        this.sequenceFields = options.sequenceField();
    }

    public MergeFileSplitRead withKeyProjection(@Nullable int[][] projectedFields) {
        readerFactoryBuilder.withKeyProjection(projectedFields);
        this.keyProjectedFields = projectedFields;
        return this;
    }

    public Comparator<InternalRow> keyComparator() {
        return keyComparator;
    }

    public MergeSorter mergeSorter() {
        return mergeSorter;
    }

    @Override
    public MergeFileSplitRead withProjection(@Nullable int[][] projectedFields) {
        if (projectedFields == null) return this;

        int[][] newProjectedFields = projectedFields;

        // 如果改表有序列字段， 要确保所有的序列字段都在映射字段中
        if (sequenceFields.size() > 0) {
            // make sure projection contains sequence fields
            // // 确保投影包含序列字段
            List<String> fieldNames = tableSchema.fieldNames();
            // 基于 Schema 获取到 projectFields 字段名
            List<String> projectedNames = Projection.of(projectedFields).project(fieldNames);
            // 返回没有包含的 sequenceFields 的字段索引
            int[] lackFields = sequenceFields.stream()
                            .filter(f -> !projectedNames.contains(f))
                            .mapToInt(fieldNames::indexOf)
                            .toArray();

            if (lackFields.length > 0) {
                newProjectedFields = Arrays.copyOf(projectedFields, projectedFields.length + lackFields.length);
                for (int i = 0; i < lackFields.length; i++) {
                    newProjectedFields[projectedFields.length + i] = new int[] {lackFields[i]};
                }
            }
        }

        AdjustedProjection projection = mfFactory.adjustProjection(newProjectedFields);
        this.pushdownProjection = projection.pushdownProjection;
        this.outerProjection = projection.outerProjection;

        if (pushdownProjection != null) {
            readerFactoryBuilder.withValueProjection(pushdownProjection);
            mergeSorter.setProjectedValueType(readerFactoryBuilder.projectedValueType());
        }

        if (newProjectedFields != projectedFields) {
            // Discard the completed sequence fields
            if (outerProjection == null) {
                outerProjection = Projection.range(0, projectedFields.length).toNestedIndexes();
            } else {
                outerProjection = Arrays.copyOf(outerProjection, projectedFields.length);
            }
        }

        return this;
    }

    @Override
    public MergeFileSplitRead withIOManager(IOManager ioManager) {
        this.mergeSorter.setIOManager(ioManager);
        return this;
    }

    @Override
    public MergeFileSplitRead forceKeepDelete() {
        this.forceKeepDelete = true;
        return this;
    }

    @Override
    public MergeFileSplitRead withFilter(Predicate predicate) {
        if (predicate == null) {
            return this;
        }

        List<Predicate> allFilters = new ArrayList<>();
        List<Predicate> pkFilters = null;
        List<String> primaryKeys = tableSchema.trimmedPrimaryKeys();
        Set<String> nonPrimaryKeys = tableSchema.fieldNames().stream()
                        .filter(name -> !primaryKeys.contains(name))
                        .collect(Collectors.toSet());

        for (Predicate sub : splitAnd(predicate)) {
            allFilters.add(sub);
            if (!containsFields(sub, nonPrimaryKeys)) {
                if (pkFilters == null) {
                    pkFilters = new ArrayList<>();
                }
                // TODO Actually, the index is wrong, but it is OK.
                //  The orc filter just use name instead of index.
                pkFilters.add(sub);
            }
        }
        // 考虑以下情况：
        // 将 (seqNumber, key, value) 表示为一条记录。在一个分区中，我们有两个重叠的运行：
        //   * 第一个运行： (1, k1, 100), (2, k2, 200)
        //   * 第二个运行： (3, k1, 10), (4, k2, 20)
        // 如果我们为该分区下推过滤器 "value >= 100"，则只会读取第一个运行，
        // 而第二个运行将丢失。这将产生不正确的结果。

        // 因此，对于具有重叠运行的分区，我们仅下推键过滤器。
        // 对于只有一个运行的分区，由于每个键只出现一次，因此可以下推值过滤器。
        filtersForAll = allFilters;
        filtersForKeys = pkFilters;
        return this;
    }

    @Override
    public RecordReader<KeyValue> createReader(DataSplit split) throws IOException {
        if (!split.beforeFiles().isEmpty()) {
            throw new IllegalArgumentException("This read cannot accept split with before files.");
        }

        if (split.isStreaming()) {
            return createNoMergeReader(
                    split.partition(),
                    split.bucket(),
                    split.dataFiles(),
                    split.deletionFiles().orElse(null),
                    split.isStreaming());
        } else {
            return createMergeReader(
                    split.partition(),
                    split.bucket(),
                    split.dataFiles(),
                    split.deletionFiles().orElse(null),
                    forceKeepDelete);
        }
    }

    public RecordReader<KeyValue> createMergeReader(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            @Nullable List<DeletionFile> deletionFiles,
            boolean keepDelete)
            throws IOException {
        // 部分由 SortMergeReader 读取，SortMergeReader 按键排序和合并记录。
        // 所以我们不能投影键，否则排序将不正确。

        // DeletionVector 可以高效地记录文件中已删除行的位置，然后在处理文件时用于过滤掉已删除的行。
        DeletionVector.Factory dvFactory = DeletionVector.factory(fileIO, files, deletionFiles);

        // 按 key 过滤器创建读取工厂
        KeyValueFileReaderFactory overlappedSectionFactory = readerFactoryBuilder.build(partition, bucket, dvFactory, false, filtersForKeys);

        // 按照 key-value 过滤器创建 读取工厂
        KeyValueFileReaderFactory nonOverlappedSectionFactory = readerFactoryBuilder.build(partition, bucket, dvFactory, false, filtersForAll);

        List<ReaderSupplier<KeyValue>> sectionReaders = new ArrayList<>();
        MergeFunctionWrapper<KeyValue> mergeFuncWrapper = new ReducerMergeFunctionWrapper(mfFactory.create(pushdownProjection));

        for (List<SortedRun> section : new IntervalPartition(files, keyComparator).partition()) {
            // 每个 List<SortedRun> 集合 之间， key 范围相互不重叠
            sectionReaders.add(
                    () ->
                            MergeTreeReaders.readerForSection(
                                    section,
                                    section.size() > 1 ?
                                            overlappedSectionFactory :    // 如果 section > 1 使用 key 过滤器，因为涉及到不同的 sortedRun 之间的数据有新旧版本的问题，所以 value 过滤器没有意义
                                            nonOverlappedSectionFactory,  // 如果 section == 1 则可以 keyvalue 都参与过滤， 加速查询
                                    keyComparator,
                                    createUdsComparator(),
                                    mergeFuncWrapper,
                                    mergeSorter));
        }
        RecordReader<KeyValue> reader = ConcatRecordReader.create(sectionReaders);

        if (!keepDelete) {
            reader = new DropDeleteReader(reader);
        }

        return projectOuter(projectKey(reader));
    }

    public RecordReader<KeyValue> createNoMergeReader(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> files,
            @Nullable List<DeletionFile> deletionFiles,
            boolean onlyFilterKey) throws IOException {

        KeyValueFileReaderFactory readerFactory = readerFactoryBuilder.build(
                        partition,
                        bucket,
                        DeletionVector.factory(fileIO, files, deletionFiles),
                        true,
                        onlyFilterKey ? filtersForKeys : filtersForAll);

        List<ReaderSupplier<KeyValue>> suppliers = new ArrayList<>();
        for (DataFileMeta file : files) {
            suppliers.add(
                    () -> {
                        // We need to check extraFiles to be compatible with Paimon 0.2.
                        // See comments on DataFileMeta#extraFiles.
                        String fileName = changelogFile(file).orElse(file.fileName());
                        return readerFactory.createRecordReader(file.schemaId(), fileName, file.fileSize(), file.level());
                    });
        }

        return projectOuter(ConcatRecordReader.create(suppliers));
    }

    private Optional<String> changelogFile(DataFileMeta fileMeta) {
        for (String file : fileMeta.extraFiles()) {
            if (file.startsWith(CHANGELOG_FILE_PREFIX)) {
                return Optional.of(file);
            }
        }
        return Optional.empty();
    }

    private RecordReader<KeyValue> projectKey(RecordReader<KeyValue> reader) {
        if (keyProjectedFields == null) {
            return reader;
        }

        ProjectedRow projectedRow = ProjectedRow.from(keyProjectedFields);
        return reader.transform(kv -> kv.replaceKey(projectedRow.replaceRow(kv.key())));
    }

    private RecordReader<KeyValue> projectOuter(RecordReader<KeyValue> reader) {
        if (outerProjection != null) {
            ProjectedRow projectedRow = ProjectedRow.from(outerProjection);
            reader = reader.transform(kv -> kv.replaceValue(projectedRow.replaceRow(kv.value())));
        }
        return reader;
    }

    @Nullable
    public UserDefinedSeqComparator createUdsComparator() {
        return UserDefinedSeqComparator.create(
                readerFactoryBuilder.projectedValueType(), sequenceFields);
    }
}
