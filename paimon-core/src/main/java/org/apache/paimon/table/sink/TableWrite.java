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

package org.apache.paimon.table.sink;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.table.Table;

/**
 * Write of {@link Table} to provide {@link InternalRow} writing.
 *
 * @since 0.4.0
 */
@Public
public interface TableWrite extends AutoCloseable {

    /** With {@link IOManager}, this is needed if 'write-buffer-spillable' is set to true. */
    TableWrite withIOManager(IOManager ioManager);

    /** With {@link MemorySegmentPool} for the current table write. */
    TableWrite withMemoryPool(MemorySegmentPool memoryPool);

    /** Calculate which partition {@code row} belongs to. */
    BinaryRow getPartition(InternalRow row);

    /** Calculate which bucket {@code row} belongs to. */
    int getBucket(InternalRow row);

    /** Write a row to the writer. */
    void write(InternalRow row) throws Exception;

    /** Write a row with bucket. */
    void write(InternalRow row, int bucket) throws Exception;

    /**
     * 压缩分区的一个桶。默认情况下，它将根据 'num-sorted-run.compaction-trigger' 选项来决定是否执行压缩。
     * 如果 fullCompaction 为 true，则会强制执行全面压缩，这会消耗大量资源。
     *
     * <p>注意：在 Java API 中，不会自动执行全面压缩。如果将 'changelog-producer' 设置为 'full-compaction'，
     * 请定期执行此方法以生成变更日志。
     */
    void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception;

    /** With metrics to measure compaction. */
    TableWrite withMetricRegistry(MetricRegistry registry);
}
