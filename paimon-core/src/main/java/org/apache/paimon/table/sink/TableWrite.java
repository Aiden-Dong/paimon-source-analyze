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
 * 将 {@link InternalRow} 写入 {@link Table} 中。
 *
 * @since 0.4.0
 */
@Public
public interface TableWrite extends AutoCloseable {

    /** 如果将 'write-buffer-spillable' 设置为 true，则需要使用 {@link IOManager}。 */
    TableWrite withIOManager(IOManager ioManager);

    /** 使用 {@link MemorySegmentPool} 进行当前表的写入。 */
    TableWrite withMemoryPool(MemorySegmentPool memoryPool);

    /** 计算 {@code row} 属于哪个分区。 */
    BinaryRow getPartition(InternalRow row);

    /** 计算 {@code row} 属于哪个bucket。 */
    int getBucket(InternalRow row);

    /** 将一行写入写入器。 */
    void write(InternalRow row) throws Exception;

    /** 指定 bucket 写入一行数据。 */
    void write(InternalRow row, int bucket) throws Exception;

    /**
     * 压缩分区的一个桶。默认情况下，它将根据 'num-sorted-run.compaction-trigger' 选项来决定是否执行压缩。
     * 如果 fullCompaction 为 true，则会强制执行全面压缩，这会消耗大量资源。
     *
     * <p>注意：在 Java API 中，不会自动执行全面压缩。如果将 'changelog-producer' 设置为 'full-compaction'，
     * 请定期执行此方法以生成变更日志。
     */
    void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception;

    /** 使用指标来衡量压缩。 */
    TableWrite withMetricRegistry(MetricRegistry registry);
}
