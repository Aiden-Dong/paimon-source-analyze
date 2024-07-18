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

package org.apache.paimon;

import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.operation.TagDeletion;
import org.apache.paimon.service.ServiceManager;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.tag.TagAutoManager;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

/**
 * 文件存储接口
 *
 * @param <T> 读取和写入的记录类型 .
 */
public interface FileStore<T> extends Serializable {

    FileStorePathFactory pathFactory();   // manifest 清单工厂

    SnapshotManager snapshotManager();   // snapshot 管理器

    RowType partitionType();  // 分区类型

    CoreOptions options();   // 配置项

    BucketMode bucketMode();  // bucket 模式

    FileStoreScan newScan();

    FileStoreScan newScan(String branchName);

    ManifestList.Factory manifestListFactory();   // manifest list 工厂

    ManifestFile.Factory manifestFileFactory();   // manifest 文件 工厂

    IndexFileHandler newIndexFileHandler();       // 索引文件句柄

    StatsFileHandler newStatsFileHandler();       // 状态统计信息

    SplitRead<T> newRead();                           // 文件读取工具

    FileStoreWrite<T> newWrite(String commitUser);    // 文件写入工具

    FileStoreWrite<T> newWrite(String commitUser, ManifestCacheFilter manifestFilter);  // 文件写入工具

    FileStoreCommit newCommit(String commitUser);

    FileStoreCommit newCommit(String commitUser, String branchName);

    SnapshotDeletion newSnapshotDeletion();

    TagManager newTagManager();             // Tag

    TagDeletion newTagDeletion();

    @Nullable
    PartitionExpire newPartitionExpire(String commitUser);

    TagAutoManager newTagCreationManager();

    ServiceManager newServiceManager();

    boolean mergeSchema(RowType rowType, boolean allowExplicitCast);

    List<TagCallback> createTagCallbacks();
}
