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

package org.apache.paimon.table;

import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.annotation.Public;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.RowType;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 表提供了表类型、表扫描和表读取的基本抽象。
 *
 * @since 0.4.0
 */
@Public
public interface Table extends Serializable {

    // ================== Table Metadata =====================

    String name();                                    // 表名
    RowType rowType();                                // 表 Schema
    List<String> partitionKeys();                     // 分区字段
    List<String> primaryKeys();                       // 主键
    Map<String, String> options();                    // 表选项
    Optional<String> comment();                       // 表注释
    @Experimental Optional<Statistics> statistics();  // 表指标统计信息

    // ================= Table Operations ====================

    Table copy(Map<String, String> dynamicOptions);                              // 复制此表并添加动态选项
    @Experimental void rollbackTo(long snapshotId);                              // 将表的状态回滚到特定快照。
    @Experimental void createTag(String tagName, long fromSnapshotId);           // 从给定快照创建Tag。
    @Experimental void createTag(String tagName, long fromSnapshotId, Duration timeRetained);
    @Experimental void createTag(String tagName);                                // 从最后一个 snapshot 中创建 tag
    @Experimental void createTag(String tagName, Duration timeRetained);
    @Experimental void deleteTag(String tagName);                                // 删除一个 Tag
    @Experimental void rollbackTo(String tagName);                               // 将表状态回滚到特定 tag
    @Experimental void createBranch(String branchName);                          // 创建一个空的 branch
    @Experimental void createBranch(String branchName, long snapshotId);         // 从给定 snapshot 创建 branch
    @Experimental void createBranch(String branchName, String tagName);          // 从给定 tag 中创建 branch
    @Experimental void deleteBranch(String branchName);                          // 删除一个 branch

    /** 手动过期快照，参数可以独立于表选项进行控制. */
    @Experimental ExpireSnapshots newExpireSnapshots();
    @Experimental ExpireSnapshots newExpireChangelog();

    // =============== Read & Write Operations ==================
    ReadBuilder newReadBuilder();                // ReadBuilder
    BatchWriteBuilder newBatchWriteBuilder();    // BatchWriteBuilder
    StreamWriteBuilder newStreamWriteBuilder();  // StreamWriteBuilder
}
