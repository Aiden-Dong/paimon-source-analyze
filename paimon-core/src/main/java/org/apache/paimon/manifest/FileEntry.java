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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.FileUtils;
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/** Entry representing a file. */
public interface FileEntry {

    FileKind kind();

    BinaryRow partition();

    int bucket();

    int level();

    String fileName();

    Identifier identifier();

    BinaryRow minKey();

    BinaryRow maxKey();

    /**
     * The same {@link Identifier} indicates that the {@link ManifestEntry} refers to the same data
     * file.
     */
    class Identifier {
        public final BinaryRow partition;
        public final int bucket;
        public final int level;
        public final String fileName;

        /* Cache the hash code for the string */
        private Integer hash;

        public Identifier(BinaryRow partition, int bucket, int level, String fileName) {
            this.partition = partition;
            this.bucket = bucket;
            this.level = level;
            this.fileName = fileName;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Identifier)) {
                return false;
            }
            Identifier that = (Identifier) o;
            return Objects.equals(partition, that.partition)
                    && bucket == that.bucket
                    && level == that.level
                    && Objects.equals(fileName, that.fileName);
        }

        @Override
        public int hashCode() {
            if (hash == null) {
                hash = Objects.hash(partition, bucket, level, fileName);
            }
            return hash;
        }

        @Override
        public String toString() {
            return String.format("{%s, %d, %d, %s}", partition, bucket, level, fileName);
        }

        public String toString(FileStorePathFactory pathFactory) {
            return pathFactory.getPartitionString(partition)
                    + ", bucket "
                    + bucket
                    + ", level "
                    + level
                    + ", file "
                    + fileName;
        }
    }

    static <T extends FileEntry> Collection<T> mergeEntries(Iterable<T> entries) {
        LinkedHashMap<Identifier, T> map = new LinkedHashMap<>();
        mergeEntries(entries, map);
        return map.values();
    }

    /***
     *
     * @param manifestFile
     * @param manifestFiles
     * @param map
     */
    static void mergeEntries(ManifestFile manifestFile,
            List<ManifestFileMeta> manifestFiles,
            Map<Identifier, ManifestEntry> map) {

        // 异步读取所有的 manifest 文件， 加载所有有效的 data entry 信息
        List<Supplier<List<ManifestEntry>>> manifestReadFutures = readManifestEntries(manifestFile, manifestFiles);

        // 读取所有的文件元信息并执行合并操作， 目的是去除 delete 文件
        // 注意的是,如果 文件只有 delete 标记，没找到 add 标记， 则保留 delete 标记文件， 因为他的 add 在之前的 manifest 里
        for (Supplier<List<ManifestEntry>> taskResult : manifestReadFutures) {
            mergeEntries(taskResult.get(), map);
        }
    }

    static <T extends FileEntry> void mergeEntries(Iterable<T> entries, Map<Identifier, T> map) {
        for (T entry : entries) {
            // 获取当前 enry 所属的 identifier (partition-bucket-level-filename), 唯一标识改数据文件
            Identifier identifier = entry.identifier();
            switch (entry.kind()) {
                case ADD:
                    // 检查之前是否已经存在
                    Preconditions.checkState(!map.containsKey(identifier), "Trying to add file %s which is already added.", identifier);

                    map.put(identifier, entry);
                    break;
                case DELETE:
                    // 每个数据文件只会添加一次和删除一次，
                    // 如果我们知道它是在之前添加的，那么添加和删除条目都可以删除，
                    // 因为不会对该文件进行进一步操作， 否则我们必须保留删除条目，因为添加条目必须在之前的清单文件中
                    if (map.containsKey(identifier)) {
                        map.remove(identifier);
                    } else {
                        map.put(identifier, entry);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown value kind " + entry.kind().name());
            }
        }
    }

    /**
     * 基于 manifest 元信息数据， 异步读取 manifest 文件
     * @param manifestFile  mainfest handler
     * @param manifestFiles  manifest 文件元信息
     */
    static List<Supplier<List<ManifestEntry>>> readManifestEntries(ManifestFile manifestFile, List<ManifestFileMeta> manifestFiles) {
        List<Supplier<List<ManifestEntry>>> result = new ArrayList<>();
        for (ManifestFileMeta file : manifestFiles) {


            Future<List<ManifestEntry>> future = CompletableFuture.supplyAsync(
                    () -> manifestFile.read(file.fileName(), file.fileSize()),
                    FileUtils.COMMON_IO_FORK_JOIN_POOL
            );

            result.add(
                    () -> {
                        try {
                            return future.get();
                        } catch (ExecutionException | InterruptedException e) {
                            throw new RuntimeException("Failed to read manifest file.", e);
                        }
                    });
        }
        return result;
    }



    static <T extends FileEntry> void assertNoDelete(Collection<T> entries) {
        for (T entry : entries) {
            Preconditions.checkState(
                    entry.kind() != FileKind.DELETE,
                    "Trying to delete file %s which is not previously added.",
                    entry.fileName());
        }
    }
}
