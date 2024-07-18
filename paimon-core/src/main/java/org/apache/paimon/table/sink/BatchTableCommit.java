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

import java.util.List;

/**
 * A {@link TableCommit} for batch processing. Recommended for one-time committing.
 *
 * @since 0.4.0
 */
@Public
public interface BatchTableCommit extends TableCommit {


    /**
     * 创建一个新的提交。一个提交可能会生成最多两个snapshot，一个用于添加新文件，另一个用于压缩。
     * 提交后将有一些过期策略：
     *
     * <p>1. 快照过期可能会根据三个选项发生：
     *
     * <ul>
     *   <li>'snapshot.time-retained': 保留已完成快照的最长时间。
     *   <li>'snapshot.num-retained.min': 保留已完成快照的最少数量。
     *   <li>'snapshot.num-retained.max': 保留已完成快照的最多数量。
     * </ul>
     *
     * <p>2. 分区过期可能会根据 'partition.expiration-time' 发生。分区检查成本高，因此每次调用此方法时并不会检查所有分区。
     * 检查频率由 'partition.expiration-check-interval' 控制。分区过期将创建一个 'OVERWRITE' 快照。
     *
     * @param commitMessages 表写入的提交消息
     */
    void commit(List<CommitMessage> commitMessages);

    /**
     * Truncate table, like normal {@link #commit}, files are not immediately deleted, they are only
     * logically deleted and will be deleted after the snapshot expires.
     */
    void truncateTable();
}
