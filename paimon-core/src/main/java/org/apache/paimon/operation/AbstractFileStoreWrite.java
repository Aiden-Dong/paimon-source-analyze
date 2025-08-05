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

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.IndexIncrement;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.paimon.io.DataFileMeta.getMaxSequenceNumber;

/**
 * Base {@link FileStoreWrite} implementation.
 *
 * @param <T> type of record to write.
 */
public abstract class AbstractFileStoreWrite<T> implements FileStoreWrite<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractFileStoreWrite.class);

    private final String commitUser;

    protected final SnapshotManager snapshotManager;  // 用来管理当前的快照信息
    private final FileStoreScan scan;
    private final int writerNumberMax;
    @Nullable private final IndexMaintainer.Factory<T> indexFactory;
    @Nullable private final DeletionVectorsMaintainer.Factory deletionVectorsMaintainerFactory;

    @Nullable protected IOManager ioManager;

    // 每个分区，分桶 下面对应一个 WriterContainer
    protected final Map<BinaryRow, Map<Integer, WriterContainer<T>>> writers;

    private ExecutorService lazyCompactExecutor;
    private boolean closeCompactExecutorWhenLeaving = true;
    private boolean ignorePreviousFiles = false;
    protected boolean isStreamingMode = false;

    protected CompactionMetrics compactionMetrics = null;
    protected final String tableName;

    protected AbstractFileStoreWrite(
            String commitUser,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            @Nullable IndexMaintainer.Factory<T> indexFactory,
            @Nullable DeletionVectorsMaintainer.Factory deletionVectorsMaintainerFactory,
            String tableName,
            int writerNumberMax) {
        this.commitUser = commitUser;
        this.snapshotManager = snapshotManager;
        this.scan = scan;
        this.indexFactory = indexFactory;
        this.deletionVectorsMaintainerFactory = deletionVectorsMaintainerFactory;
        this.writers = new HashMap<>();
        this.tableName = tableName;
        this.writerNumberMax = writerNumberMax;
    }

    @Override
    public FileStoreWrite<T> withIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
        return this;
    }

    @Override
    public FileStoreWrite<T> withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory) {
        return this;
    }

    @Override
    public void withIgnorePreviousFiles(boolean ignorePreviousFiles) {
        this.ignorePreviousFiles = ignorePreviousFiles;
    }

    @Override
    public void withCompactExecutor(ExecutorService compactExecutor) {
        this.lazyCompactExecutor = compactExecutor;
        this.closeCompactExecutorWhenLeaving = false;
    }

    @Override
    public void write(BinaryRow partition, int bucket, T data) throws Exception {
        WriterContainer<T> container = getWriterWrapper(partition, bucket);
        container.writer.write(data);
        if (container.indexMaintainer != null) {
            container.indexMaintainer.notifyNewRecord(data);
        }
    }

    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        getWriterWrapper(partition, bucket).writer.compact(fullCompaction);
    }

    @Override
    public void notifyNewFiles(
            long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files) {
        WriterContainer<T> writerContainer = getWriterWrapper(partition, bucket);
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Get extra compact files for partition {}, bucket {}. Extra snapshot {}, base snapshot {}.\nFiles: {}",
                    partition,
                    bucket,
                    snapshotId,
                    writerContainer.baseSnapshotId,
                    files);
        }
        if (snapshotId > writerContainer.baseSnapshotId) {
            writerContainer.writer.addNewFiles(files);
        }
    }

    /***
     * 将数据落盘， 并统计每个 bucket 的提交信息
     * @param waitCompaction if this method need to wait for current compaction to complete
     * @param commitIdentifier identifier of the commit being prepared
     * @return
     * @throws Exception
     */
    @Override
    public List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
            throws Exception {

        // 获取提交时间
        long latestCommittedIdentifier;
        if (writers.values().stream()
                        .map(Map::values)
                        .flatMap(Collection::stream)
                        .mapToLong(w -> w.lastModifiedCommitIdentifier)
                        .max()
                        .orElse(Long.MIN_VALUE) == Long.MIN_VALUE) {

            // 首次提交的优化。
            // 如果这是首次提交，则没有写入者之前修改过提交，因此 `latestCommittedIdentifier` 的值无关紧要。
            // 如果没有此优化，可能需要扫描所有快照，只是为了发现该用户没有之前的快照，这样效率非常低。
            latestCommittedIdentifier = Long.MIN_VALUE;
        } else {
            // 获取当前用户的最后提交时间
            latestCommittedIdentifier = snapshotManager
                            .latestSnapshotOfUser(commitUser)
                            .map(Snapshot::commitIdentifier)
                            .orElse(Long.MIN_VALUE);
        }

        List<CommitMessage> result = new ArrayList<>();

        Iterator<Map.Entry<BinaryRow, Map<Integer, WriterContainer<T>>>> partIter =
                writers.entrySet().iterator();

        while (partIter.hasNext()) {
            // 迭代每个分区
            Map.Entry<BinaryRow, Map<Integer, WriterContainer<T>>> partEntry = partIter.next();
            BinaryRow partition = partEntry.getKey();       // 获取分区信息
            Iterator<Map.Entry<Integer, WriterContainer<T>>> bucketIter = partEntry.getValue().entrySet().iterator();  // 获取bucket 迭代器

            while (bucketIter.hasNext()) {
                // 迭代每个 bucket
                Map.Entry<Integer, WriterContainer<T>> entry = bucketIter.next();
                int bucket = entry.getKey();                             // bucketID
                WriterContainer<T> writerContainer = entry.getValue();   //

                // 调用write 的预提交过程---数据刷写磁盘(如果有需要则提交 compaction, 返回影响文件元信息)
                CommitIncrement increment = writerContainer.writer.prepareCommit(waitCompaction);
                List<IndexFileMeta> newIndexFiles = new ArrayList<>();

                if (writerContainer.indexMaintainer != null) {
                    newIndexFiles.addAll(writerContainer.indexMaintainer.prepareCommit());
                }

                if (writerContainer.deletionVectorsMaintainer != null) {
                    newIndexFiles.addAll(writerContainer.deletionVectorsMaintainer.prepareCommit());
                }

                CommitMessageImpl committable = new CommitMessageImpl(
                                partition,
                                bucket,
                                increment.newFilesIncrement(),        // 变更文件信息
                                increment.compactIncrement(),         // compaction 的前后记录信息
                                new IndexIncrement(newIndexFiles));   // 新增索引文件

                result.add(committable);

                if (committable.isEmpty()) {
                    // 条件 1：没有更多的记录等待提交。注意此条件是 <（而不是 <=），因为每个提交标识符可能有多个快照。
                    // 我们必须确保该标识符的所有快照都已提交。
                    // 条件 2：没有压缩操作正在进行，即不会再生成变更日志。
                    if (writerContainer.lastModifiedCommitIdentifier < latestCommittedIdentifier
                            && !writerContainer.writer.isCompacting()) {
                        // Clear writer if no update, and if its latest modification has committed.
                        //
                        // We need a mechanism to clear writers, otherwise there will be more and
                        // more such as yesterday's partition that no longer needs to be written.
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "Closing writer for partition {}, bucket {}. "
                                            + "Writer's last modified identifier is {}, "
                                            + "while latest committed identifier is {}, "
                                            + "current commit identifier is {}.",
                                    partition,
                                    bucket,
                                    writerContainer.lastModifiedCommitIdentifier,
                                    latestCommittedIdentifier,
                                    commitIdentifier);
                        }
                        writerContainer.writer.close();
                        bucketIter.remove();
                    }
                } else {
                    writerContainer.lastModifiedCommitIdentifier = commitIdentifier;
                }
            }

            if (partEntry.getValue().isEmpty()) {
                partIter.remove();
            }
        }

        return result;
    }

    @Override
    public void close() throws Exception {
        for (Map<Integer, WriterContainer<T>> bucketWriters : writers.values()) {
            for (WriterContainer<T> writerContainer : bucketWriters.values()) {
                writerContainer.writer.close();
            }
        }
        writers.clear();
        if (lazyCompactExecutor != null && closeCompactExecutorWhenLeaving) {
            lazyCompactExecutor.shutdownNow();
        }
        if (compactionMetrics != null) {
            compactionMetrics.close();
        }
    }

    @Override
    public List<State<T>> checkpoint() {
        List<State<T>> result = new ArrayList<>();

        for (Map.Entry<BinaryRow, Map<Integer, WriterContainer<T>>> partitionEntry :
                writers.entrySet()) {
            BinaryRow partition = partitionEntry.getKey();
            for (Map.Entry<Integer, WriterContainer<T>> bucketEntry :
                    partitionEntry.getValue().entrySet()) {
                int bucket = bucketEntry.getKey();
                WriterContainer<T> writerContainer = bucketEntry.getValue();

                CommitIncrement increment;
                try {
                    increment = writerContainer.writer.prepareCommit(false);
                } catch (Exception e) {
                    throw new RuntimeException(
                            "Failed to extract state from writer of partition "
                                    + partition
                                    + " bucket "
                                    + bucket,
                            e);
                }
                // writer.allFiles() must be fetched after writer.prepareCommit(), because
                // compaction result might be updated during prepareCommit
                Collection<DataFileMeta> dataFiles = writerContainer.writer.dataFiles();
                result.add(
                        new State<>(
                                partition,
                                bucket,
                                writerContainer.baseSnapshotId,
                                writerContainer.lastModifiedCommitIdentifier,
                                dataFiles,
                                writerContainer.writer.maxSequenceNumber(),
                                writerContainer.indexMaintainer,
                                writerContainer.deletionVectorsMaintainer,
                                increment));
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Extracted state " + result);
        }
        return result;
    }

    @Override
    public void restore(List<State<T>> states) {
        for (State<T> state : states) {
            RecordWriter<T> writer =
                    createWriter(
                            state.partition,
                            state.bucket,
                            state.dataFiles,
                            state.maxSequenceNumber,
                            state.commitIncrement,
                            compactExecutor(),
                            state.deletionVectorsMaintainer);
            notifyNewWriter(writer);
            WriterContainer<T> writerContainer =
                    new WriterContainer<>(
                            writer,
                            state.indexMaintainer,
                            state.deletionVectorsMaintainer,
                            state.baseSnapshotId);
            writerContainer.lastModifiedCommitIdentifier = state.lastModifiedCommitIdentifier;
            writers.computeIfAbsent(state.partition, k -> new HashMap<>())
                    .put(state.bucket, writerContainer);
        }
    }

    private WriterContainer<T> getWriterWrapper(BinaryRow partition, int bucket) {
        Map<Integer, WriterContainer<T>> buckets = writers.get(partition);
        if (buckets == null) {
            buckets = new HashMap<>();
            writers.put(partition.copy(), buckets);
        }
        return buckets.computeIfAbsent(
                bucket, k -> createWriterContainer(partition.copy(), bucket, ignorePreviousFiles));
    }

    private long writerNumber() {
        return writers.values().stream().mapToLong(e -> e.values().size()).sum();
    }

    /**
     * 对于每一个 指定的 bucket 用于初始化一个 WriterContainer
     */
    @VisibleForTesting
    public WriterContainer<T> createWriterContainer(BinaryRow partition, int bucket, boolean ignorePreviousFiles) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating writer for partition {}, bucket {}", partition, bucket);
        }

        // 如果当前 batch 模式， 并且 writer 个数太多，需要进行数据刷盘操作，用来释放 writer
        if (!isStreamingMode && writerNumber() >= writerNumberMax) {
            try {
                forceBufferSpill();
            } catch (Exception e) {
                throw new RuntimeException("Error happens while force buffer spill", e);
            }
        }

        // 获取当前 Table 的最新的 SnapshotId
        Long latestSnapshotId = snapshotManager.latestSnapshotId();

        // 获取当前表已存在的文件信息
        List<DataFileMeta> restoreFiles = new ArrayList<>();
        if (!ignorePreviousFiles && latestSnapshotId != null) {
            restoreFiles = scanExistingFileMetas(latestSnapshotId, partition, bucket);
        }

        IndexMaintainer<T> indexMaintainer = indexFactory == null ?
                null
                : indexFactory.createOrRestore(ignorePreviousFiles ? null : latestSnapshotId, partition, bucket);

        DeletionVectorsMaintainer deletionVectorsMaintainer = deletionVectorsMaintainerFactory == null ?
                null
                : deletionVectorsMaintainerFactory.createOrRestore(ignorePreviousFiles ? null : latestSnapshotId, partition, bucket);

        RecordWriter<T> writer =
                createWriter(
                        partition.copy(),
                        bucket,
                        restoreFiles,
                        getMaxSequenceNumber(restoreFiles),
                        null,
                        compactExecutor(),
                        deletionVectorsMaintainer);
        notifyNewWriter(writer);
        return new WriterContainer<>(
                writer, indexMaintainer, deletionVectorsMaintainer, latestSnapshotId);
    }

    @Override
    public void withExecutionMode(boolean isStreamingMode) {
        this.isStreamingMode = isStreamingMode;
    }

    @Override
    public FileStoreWrite<T> withMetricRegistry(MetricRegistry metricRegistry) {
        this.compactionMetrics = new CompactionMetrics(metricRegistry, tableName);
        return this;
    }

    private List<DataFileMeta> scanExistingFileMetas(
            long snapshotId, BinaryRow partition, int bucket) {
        List<DataFileMeta> existingFileMetas = new ArrayList<>();
        // Concat all the DataFileMeta of existing files into existingFileMetas.
        scan.withSnapshot(snapshotId).withPartitionBucket(partition, bucket).plan().files().stream()
                .map(ManifestEntry::file)
                .forEach(existingFileMetas::add);
        return existingFileMetas;
    }

    private ExecutorService compactExecutor() {
        if (lazyCompactExecutor == null) {
            lazyCompactExecutor =
                    Executors.newSingleThreadScheduledExecutor(
                            new ExecutorThreadFactory(
                                    Thread.currentThread().getName() + "-compaction"));
        }
        return lazyCompactExecutor;
    }

    @VisibleForTesting
    public ExecutorService getCompactExecutor() {
        return lazyCompactExecutor;
    }

    protected void notifyNewWriter(RecordWriter<T> writer) {}

    protected abstract RecordWriter<T> createWriter(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoreFiles,
            long restoredMaxSeqNumber,
            @Nullable CommitIncrement restoreIncrement,
            ExecutorService compactExecutor,
            @Nullable DeletionVectorsMaintainer deletionVectorsMaintainer);

    // force buffer spill to avoid out of memory in batch mode
    protected void forceBufferSpill() throws Exception {}

    /**
     * {@link RecordWriter} with the snapshot id it is created upon and the identifier of its last
     * modified commit.
     */
    @VisibleForTesting
    public static class WriterContainer<T> {
        public final RecordWriter<T> writer;
        @Nullable public final IndexMaintainer<T> indexMaintainer;
        @Nullable public final DeletionVectorsMaintainer deletionVectorsMaintainer;
        protected final long baseSnapshotId;
        protected long lastModifiedCommitIdentifier;

        protected WriterContainer(
                RecordWriter<T> writer,
                @Nullable IndexMaintainer<T> indexMaintainer,
                @Nullable DeletionVectorsMaintainer deletionVectorsMaintainer,
                Long baseSnapshotId) {
            this.writer = writer;
            this.indexMaintainer = indexMaintainer;
            this.deletionVectorsMaintainer = deletionVectorsMaintainer;
            this.baseSnapshotId =
                    baseSnapshotId == null ? Snapshot.FIRST_SNAPSHOT_ID - 1 : baseSnapshotId;
            this.lastModifiedCommitIdentifier = Long.MIN_VALUE;
        }
    }

    @VisibleForTesting
    Map<BinaryRow, Map<Integer, WriterContainer<T>>> writers() {
        return writers;
    }
}
