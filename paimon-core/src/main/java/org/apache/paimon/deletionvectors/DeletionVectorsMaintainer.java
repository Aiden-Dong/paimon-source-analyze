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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;

/**
 * 删除向量索引的维护者.
 * 每个 bucket 对应一个删除向量维护者
 * */
public class DeletionVectorsMaintainer {

    private final IndexFileHandler indexFileHandler;              // 索引文件的句柄

    // <产生删除事件的文件名, 该文件上发生的删除事件向量>
    private final Map<String, DeletionVector> deletionVectors;    // 删除向量集合，
    private boolean modified;                                     // 本次写入是否发生过删除行为

    private DeletionVectorsMaintainer(
            IndexFileHandler fileHandler,
            @Nullable Long snapshotId,
            BinaryRow partition,
            int bucket) {
        this.indexFileHandler = fileHandler;
        IndexFileMeta indexFile = snapshotId == null ?
                null : fileHandler.scan(snapshotId, DELETION_VECTORS_INDEX, partition, bucket).orElse(null);

        this.deletionVectors = indexFile == null ? new HashMap<>()
                        : new HashMap<>(indexFileHandler.readAllDeletionVectors(indexFile));
        this.modified = false;
    }

    /**
     * 通知一个新的删除操作，该操作将指定的文件中的指定行位置标记为已删除。
     *
     * @param fileName 发生删除操作的文件名。
     * @param position 文件中已被删除的行位置。
     */
    public void notifyNewDeletion(String fileName, long position) {
        DeletionVector deletionVector =
                deletionVectors.computeIfAbsent(fileName, k -> new BitmapDeletionVector());
        if (deletionVector.checkedDelete(position)) {  // 添加删除操作
            modified = true;
        }
    }

    /**
     * 移除指定文件的删除向量，此方法通常用于在压缩之前移除文件的删除向量。
     *
     * @param fileName 应移除其删除向量的文件名。
     */
    public void removeDeletionVectorOf(String fileName) {
        if (deletionVectors.containsKey(fileName)) {
            deletionVectors.remove(fileName);
            modified = true;
        }
    }

    /**
     * 准备提交：如果进行了任何修改，则写入新的删除向量索引文件。
     *
     * @return 包含删除向量索引文件元数据的列表，如果没有需要提交的更改，则为空列表。
     */
    public List<IndexFileMeta> prepareCommit() {
        if (modified) {
            IndexFileMeta entry = indexFileHandler.writeDeletionVectorsIndex(deletionVectors);
            modified = false;
            return Collections.singletonList(entry);
        }
        return Collections.emptyList();
    }

    /**
     * 检索与指定文件名关联的删除向量
     *
     * @param fileName 请求删除向量的文件名。
     * @return 如果存在则包含删除向量的 {@code Optional}，否则为空 {@code Optional}。
     */
    public Optional<DeletionVector> deletionVectorOf(String fileName) {
        return Optional.ofNullable(deletionVectors.get(fileName));
    }

    @VisibleForTesting
    public Map<String, DeletionVector> deletionVectors() {
        return deletionVectors;
    }

    /** Factory to restore {@link DeletionVectorsMaintainer}. */
    public static class Factory {

        private final IndexFileHandler handler;

        public Factory(IndexFileHandler handler) {
            this.handler = handler;
        }

        public DeletionVectorsMaintainer createOrRestore(
                @Nullable Long snapshotId, BinaryRow partition, int bucket) {
            return new DeletionVectorsMaintainer(handler, snapshotId, partition, bucket);
        }
    }
}
