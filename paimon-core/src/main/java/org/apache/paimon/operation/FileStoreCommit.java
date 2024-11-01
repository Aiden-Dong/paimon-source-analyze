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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.operation.metrics.CommitMetrics;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.utils.FileStorePathFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * 提供提交和覆盖功能的提交操作。
 **/
public interface FileStoreCommit {

    FileStoreCommit withLock(Lock lock);                                           // 全局锁
    FileStoreCommit ignoreEmptyCommit(boolean ignoreEmptyCommit);                  // 忽略空提交
    Set<Long> filterCommitted(Set<Long> commitIdentifiers);                        // 找出从失败中恢复时需要重试的提交标识符。

    void commit(ManifestCommittable committable, Map<String, String> properties);   // 从可提交的清单中提交。
    void commit(ManifestCommittable committable, Map<String, String> properties,     // 从可提交的清单中提交，并检查附加文件。
            boolean checkAppendFiles);

    /**
     * 从可提交的清单和分区中覆盖。
     *
     * @param partition 一个单一分区，将每个分区键映射到一个分区值。根据用户定义的语句，分区可能不包含所有分区键。
     * 请注意，该分区不一定等于新添加的键值的分区。这只是要清理的分区。
     */
    void overwrite(Map<String, String> partition, ManifestCommittable committable, Map<String, String> properties);

    /**
     * 删除多个分区。生成的快照的 {@link Snapshot.CommitKind} 为 {@link Snapshot.CommitKind#OVERWRITE}。
     *
     * @param partitions 一个包含多个分区的 {@link Map} 列表。注意：不能为空！
     */
    void dropPartitions(List<Map<String, String>> partitions, long commitIdentifier);

    void truncateTable(long commitIdentifier);

    /**
     * 终止未成功的提交。数据文件将被删除。
     **/
    void abort(List<CommitMessage> commitMessages);

    // 带有用于测量提交的指标。
    FileStoreCommit withMetrics(CommitMetrics metrics);

    /*******
     * 提交新的统计信息。生成的快照的 {@link Snapshot.CommitKind} 是 {@link
     * Snapshot.CommitKind#ANALYZE}。
     */
    void commitStatistics(Statistics stats, long commitIdentifier);

    FileStorePathFactory pathFactory();

    FileIO fileIO();
}
