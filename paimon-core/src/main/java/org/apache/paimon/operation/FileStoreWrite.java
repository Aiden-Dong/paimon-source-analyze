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

import org.apache.paimon.FileStore;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVectorsMaintainer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.IndexMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.Restorable;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Writer 操作，提供 {@link RecordWriter} 的创建，并将 {@link SinkRecord} 写入 {@link FileStore}。
 *
 * @param <T> 要写入的记录类型。
 */
public interface FileStoreWrite<T> extends Restorable<List<FileStoreWrite.State<T>>> {

    FileStoreWrite<T> withIOManager(IOManager ioManager);

    /**
     * 使用内存池进行当前文件存储写入。
     *
     * @param memoryPool 给定的内存池。
     */
    default FileStoreWrite<T> withMemoryPool(MemorySegmentPool memoryPool) {
        return withMemoryPoolFactory(new MemoryPoolFactory(memoryPool));
    }

    /**
     * 使用内存池工厂进行当前文件存储写入。
     *
     * @param memoryPoolFactory 给定的内存池工厂。
     */
    FileStoreWrite<T> withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory);

    /**
     * 设置写入操作是否应该忽略先前存储的文件。
     *
     * @param ignorePreviousFiles 写入操作是否应该忽略先前存储的文件。
     */
    void withIgnorePreviousFiles(boolean ignorePreviousFiles);

    /**
     * 我们检测是否处于批处理模式，如果是，则进行一些优化。
     *
     * @param isStreamingMode 是否处于流模式
     */
    void withExecutionMode(boolean isStreamingMode);

    /** 使用指标来衡量压缩。 */
    FileStoreWrite<T> withMetricRegistry(MetricRegistry metricRegistry);

    // 异步压缩使用线程
    void withCompactExecutor(ExecutorService compactExecutor);

    /**
     * 根据分区和桶将数据写入存储。
     * @param partition 数据的分区
     * @param bucket 数据的桶 ID
     * @param data 给定的数据
     * @throws Exception 写入记录时抛出的异常
     */
    void write(BinaryRow partition, int bucket, T data) throws Exception;

    /**
     * 压缩存储在给定分区和桶中的数据。请注意，压缩过程仅提交，在方法返回时可能尚未完成。
     *
     * @param partition 要压缩的分区
     * @param bucket 要压缩的桶
     * @param fullCompaction 是否触发完全压缩或仅正常压缩
     * @throws Exception 压缩记录时抛出的异常
     */
    void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception;

    /**
     * 通知在给定快照的给定桶中创建了一些新文件。
     * <p>很可能这些文件是由另一个作业创建的。目前，此方法仅由专用的压缩作业用于查看由写入作业创建的文件。
     *
     * @param snapshotId 创建新文件快照的 ID
     * @param partition 创建新文件的分区
     * @param bucket 创建新文件的桶
     * @param files 新文件本身
     */
    void notifyNewFiles(long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files);

    /**
     * 准备写入中的提交。
     *
     * @param waitCompaction 此方法是否需要等待当前压缩完成
     * @param commitIdentifier 正在准备提交的标识符
     * @return 文件提交列表
     * @throws Exception 抛出的异常
     */
    List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
            throws Exception;

    /**
     * 关闭写入器。
     *
     * @throws Exception 抛出的异常
     */
    void close() throws Exception;

    /** Recoverable state of {@link FileStoreWrite}. */
    class State<T> {

        protected final BinaryRow partition;
        protected final int bucket;

        protected final long baseSnapshotId;
        protected final long lastModifiedCommitIdentifier;
        protected final List<DataFileMeta> dataFiles;
        protected final long maxSequenceNumber;
        @Nullable protected final IndexMaintainer<T> indexMaintainer;
        @Nullable protected final DeletionVectorsMaintainer deletionVectorsMaintainer;
        protected final CommitIncrement commitIncrement;

        protected State(
                BinaryRow partition,
                int bucket,
                long baseSnapshotId,
                long lastModifiedCommitIdentifier,
                Collection<DataFileMeta> dataFiles,
                long maxSequenceNumber,
                @Nullable IndexMaintainer<T> indexMaintainer,
                @Nullable DeletionVectorsMaintainer deletionVectorsMaintainer,
                CommitIncrement commitIncrement) {
            this.partition = partition;
            this.bucket = bucket;
            this.baseSnapshotId = baseSnapshotId;
            this.lastModifiedCommitIdentifier = lastModifiedCommitIdentifier;
            this.dataFiles = new ArrayList<>(dataFiles);
            this.maxSequenceNumber = maxSequenceNumber;
            this.indexMaintainer = indexMaintainer;
            this.deletionVectorsMaintainer = deletionVectorsMaintainer;
            this.commitIncrement = commitIncrement;
        }

        @Override
        public String toString() {
            return String.format(
                    "{%s, %d, %d, %d, %s, %d, %s, %s, %s}",
                    partition,
                    bucket,
                    baseSnapshotId,
                    lastModifiedCommitIdentifier,
                    dataFiles,
                    maxSequenceNumber,
                    indexMaintainer,
                    deletionVectorsMaintainer,
                    commitIncrement);
        }
    }
}
