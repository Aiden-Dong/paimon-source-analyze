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
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileEntry.Identifier;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.stats.BinaryTableStats;
import org.apache.paimon.stats.FieldStatsArraySerializer;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * manifest 文件元信息, 记录在 manifest-list entry 项中
 **/
public class ManifestFileMeta {

    private static final Logger LOG = LoggerFactory.getLogger(ManifestFileMeta.class);

    private final String fileName;
    private final long fileSize;
    private final long numAddedFiles;
    private final long numDeletedFiles;
    private final BinaryTableStats partitionStats;
    private final long schemaId;

    public ManifestFileMeta(
            String fileName,
            long fileSize,
            long numAddedFiles,
            long numDeletedFiles,
            BinaryTableStats partitionStats,
            long schemaId) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.numAddedFiles = numAddedFiles;
        this.numDeletedFiles = numDeletedFiles;
        this.partitionStats = partitionStats;
        this.schemaId = schemaId;
    }

    public String fileName() {
        return fileName;
    }

    public long fileSize() {
        return fileSize;
    }

    public long numAddedFiles() {
        return numAddedFiles;
    }

    public long numDeletedFiles() {
        return numDeletedFiles;
    }

    public BinaryTableStats partitionStats() {
        return partitionStats;
    }

    public long schemaId() {
        return schemaId;
    }

    public static RowType schema() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "_FILE_NAME", new VarCharType(false, Integer.MAX_VALUE)));
        fields.add(new DataField(1, "_FILE_SIZE", new BigIntType(false)));
        fields.add(new DataField(2, "_NUM_ADDED_FILES", new BigIntType(false)));
        fields.add(new DataField(3, "_NUM_DELETED_FILES", new BigIntType(false)));
        fields.add(new DataField(4, "_PARTITION_STATS", FieldStatsArraySerializer.schema()));
        fields.add(new DataField(5, "_SCHEMA_ID", new BigIntType(false)));
        return new RowType(fields);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ManifestFileMeta)) {
            return false;
        }
        ManifestFileMeta that = (ManifestFileMeta) o;
        return Objects.equals(fileName, that.fileName)
                && fileSize == that.fileSize
                && numAddedFiles == that.numAddedFiles
                && numDeletedFiles == that.numDeletedFiles
                && Objects.equals(partitionStats, that.partitionStats)
                && schemaId == that.schemaId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                fileName, fileSize, numAddedFiles, numDeletedFiles, partitionStats, schemaId);
    }

    @Override
    public String toString() {
        return String.format(
                "{%s, %d, %d, %d, %s, %d}",
                fileName, fileSize, numAddedFiles, numDeletedFiles, partitionStats, schemaId);
    }

    /**
     *  合并多个 {@link ManifestFileMeta}。
     *  表示先添加然后删除相同数据文件的 {@link ManifestEntry} 会相互抵消。
     *
     * <p>NOTE: This method is atomic.
     */
    public static List<ManifestFileMeta> merge(
            List<ManifestFileMeta> input,      // 历史 manifest 文件集合
            ManifestFile manifestFile,         // manifest  handler
            long suggestedMetaSize,            // manifest.target-file-size (8M)
            int suggestedMinMetaCount,         // manifest.merge-min-count (30)
            long manifestFullCompactionSize,   // manifest.full-compaction-threshold-size (16M)
            RowType partitionType) {
        // these are the newly created manifest files, clean them up if exception occurs
        List<ManifestFileMeta> newMetas = new ArrayList<>();

        try {
            Optional<List<ManifestFileMeta>> fullCompacted = tryFullCompaction(
                            input,                     // manifest 文件集合
                            newMetas,                   // 输出文件集合
                            manifestFile,               // manifest  handler
                            suggestedMetaSize,          // manifest.target-file-size (8M)
                            manifestFullCompactionSize, // manifest.full-compaction-threshold-size (16M)
                            partitionType);

            // manifest minor compaction, 只做小 manifest 文件合并
            return fullCompacted.orElseGet(() -> tryMinorCompaction(
                                    input,
                                    newMetas,
                                    manifestFile,
                                    suggestedMetaSize,
                                    suggestedMinMetaCount));

        } catch (Throwable e) {
            // exception occurs, clean up and rethrow
            for (ManifestFileMeta manifest : newMetas) {
                manifestFile.delete(manifest.fileName);
            }
            throw new RuntimeException(e);
        }
    }

    private static List<ManifestFileMeta> tryMinorCompaction(
                            List<ManifestFileMeta> input,
                            List<ManifestFileMeta> newMetas,
                            ManifestFile manifestFile,
                            long suggestedMetaSize,
                            int suggestedMinMetaCount) {

        List<ManifestFileMeta> result = new ArrayList<>();
        List<ManifestFileMeta> candidates = new ArrayList<>();
        long totalSize = 0;

        // merge existing small manifest files
        for (ManifestFileMeta manifest : input) {
            totalSize += manifest.fileSize;
            candidates.add(manifest);
            if (totalSize >= suggestedMetaSize) {
                // 达到建议的文件大小，执行合并并生成新文件
                mergeCandidates(candidates, manifestFile, result, newMetas);
                candidates.clear();
                totalSize = 0;
            }
        }

        // 如果文件不大但是超过 {manifest.merge-min-count:30} 则也会触发合并
        if (candidates.size() >= suggestedMinMetaCount) {
            mergeCandidates(candidates, manifestFile, result, newMetas);
        } else {
            result.addAll(candidates);
        }
        return result;
    }

    private static void mergeCandidates(
            List<ManifestFileMeta> candidates,
            ManifestFile manifestFile,
            List<ManifestFileMeta> result,
            List<ManifestFileMeta> newMetas) {

        if (candidates.size() == 1) {
            result.add(candidates.get(0));
            return;
        }

        // 合并小的 Manifest 文件， 生成一个大的 manifest 文件
        Map<Identifier, ManifestEntry> map = new LinkedHashMap<>();
        FileEntry.mergeEntries(manifestFile, candidates, map);
        if (!map.isEmpty()) {
            List<ManifestFileMeta> merged = manifestFile.write(new ArrayList<>(map.values()));
            result.addAll(merged);
            newMetas.addAll(merged);
        }
    }

    // 新版逻辑
//    public static Optional<List<ManifestFileMeta>> tryFullCompaction(
//            List<ManifestFileMeta> inputs,
//            List<ManifestFileMeta> newFilesForAbort,
//            ManifestFile manifestFile,
//            long suggestedMetaSize,
//            long sizeTrigger,
//            RowType partitionType,
//            @Nullable Integer manifestReadParallelism)
//            throws Exception {
//        checkArgument(sizeTrigger > 0, "Manifest full compaction size trigger cannot be zero.");
//
//        // TODO - 1 : 从所有的 mainfest 文件集中找到待合并的 manifest
//
//        Filter<ManifestFileMeta> mustChange = file -> file.numDeletedFiles() > 0 || file.fileSize() < suggestedMetaSize;
//
//        long totalManifestSize = 0;   // 总的 manifest 文件大小
//        long deltaDeleteFileNum = 0;  // 总的待删除文件数量
//        long totalDeltaFileSize = 0;  // 待合并的增量文件大小
//
//        for (ManifestFileMeta file : inputs) {
//            totalManifestSize += file.fileSize();
//            if (mustChange.test(file)) {
//                totalDeltaFileSize += file.fileSize();
//                deltaDeleteFileNum += file.numDeletedFiles();
//            }
//        }
//        // TODO - 1.1 : 如果待合并的 manifest 文件集大小小于 {manifest.target-file-size} 则放弃合并
//        if (totalDeltaFileSize < sizeTrigger) {
//            return Optional.empty();
//        }
//
//        // TODO - 2 : full-compaction
//
//        // TODO - 2.1 : 从中读取所有的待删除的文件元信息
//        Set<FileEntry.Identifier> deleteEntries = FileEntry.readDeletedEntries(manifestFile, inputs, manifestReadParallelism);
//
//
//        List<ManifestFileMeta> result = new ArrayList<>();     // compaction 后的结果集
//
//        List<ManifestFileMeta> toBeMerged = new LinkedList<>(inputs);
//
//        // 2.2. 如果是分区表，则基于分区先跳过不用合并的分区记录的
//        if (partitionType.getFieldCount() > 0) {
//
//            // 拿到删除文件集所在的分区
//            Set<BinaryRow> deletePartitions = computeDeletePartitions(deleteEntries);
//            PartitionPredicate predicate = PartitionPredicate.fromMultiple(partitionType, deletePartitions);
//            if (predicate != null) {
//                Iterator<ManifestFileMeta> iterator = toBeMerged.iterator();
//                while (iterator.hasNext()) {
//                    ManifestFileMeta file = iterator.next();
//
//                    // 如果是待压缩文件， 则跳过检验
//                    if (mustChange.test(file)) {
//                        continue;
//                    }
//
//                    // 如果是大文件 (超过{manifest.target-file-size}), 并且 mainifest 所有文件元信息集合不在
//                    if (!predicate.test(file.numAddedFiles() + file.numDeletedFiles(),
//                            file.partitionStats().minValues(),
//                            file.partitionStats().maxValues(),
//                            file.partitionStats().nullCounts())) {
//                        iterator.remove();
//                        result.add(file);
//                    }
//                }
//            }
//        }
//
//        // TODO - 2.2 : 执行合并操作， 读取所有的未决文件
//        //              如果目标mainfest中的所有文件元信息都没有被打上 delete 标签，并且 大小超过   {manifest.target-file-size}，则不在重写压缩
//        //              否则将文件重写
//
//        if (toBeMerged.size() <= 1) {
//            return Optional.empty();
//        }
//
//        // {manifest.target-file-size} 8M 一个大小
//        RollingFileWriter<ManifestEntry, ManifestFileMeta> writer = manifestFile.createRollingWriter();
//
//        Exception exception = null;
//        try {
//            for (ManifestFileMeta file : toBeMerged) {             // 遍历所有的 manifest 文件集
//                List<ManifestEntry> entries = new ArrayList<>();
//                boolean requireChange = mustChange.test(file);     // 校验是否必须合并
//
//                for (ManifestEntry entry : manifestFile.read(file.fileName(), file.fileSize())) {  // 读取当前 Manifest 的所有的 entry
//                    if (entry.kind() == FileKind.DELETE) continue;  // 如果是删除记录， 则直接跳过
//
//                    if (deleteEntries.contains(entry.identifier())) {  // 如果当前记录已经打上删除标记
//                        requireChange = true;
//                    } else {
//                        entries.add(entry);
//                    }
//                }
//
//                if (requireChange) {  //  标识是小文件，或者是记录有删除标记，则重新压缩写
//                    writer.write(entries);
//                } else {
//                    result.add(file);   // 否则只移动，不写入
//                }
//            }
//        } catch (Exception e) {
//            exception = e;
//        } finally {
//            if (exception != null) {
//                writer.abort();
//                throw exception;
//            }
//            writer.close();
//        }
//
//        List<ManifestFileMeta> merged = writer.result();   // 拿到所有新生成的 manifest 文件信息
//        result.addAll(merged);
//        newFilesForAbort.addAll(merged);
//        return Optional.of(result);
//    }

    // 合并 manifest 文件 ，降低 Manifest-list 压力
    public static Optional<List<ManifestFileMeta>> tryFullCompaction(
            List<ManifestFileMeta> inputs,             // manifest 文件集合
            List<ManifestFileMeta> newMetas,           // 输出文件集合
            ManifestFile manifestFile,                 // manifest  handler
            long suggestedMetaSize,                    // manifest.target-file-size (8M)
            long sizeTrigger,                          // manifest.full-compaction-threshold-size (16M)
            RowType partitionType) throws Exception {

        List<ManifestFileMeta> base = new ArrayList<>();
        int totalManifestSize = 0;
        int i = 0;

        // TODO - 1 : 从初始位置开始遍历 manifest 文件，过滤掉之前合并好的 mainfest 文件
        //            文件大小超过 {manifest.target-file-size}, 并且没有 delete 文件
        for (; i < inputs.size(); i++) {
            ManifestFileMeta file = inputs.get(i);

            // 如果当前 manifest 文件过大，并且没有 delete
            if (file.numDeletedFiles == 0 && file.fileSize >= suggestedMetaSize) {
                base.add(file);
                totalManifestSize += file.fileSize;
            } else {
                break;
            }
        }

        // TODO - 2 : 从第一个文件过小(不足 {manifest.target-file-size} )，或者存在 delete 记录处开始记录增量文件
        List<ManifestFileMeta> delta = new ArrayList<>();
        long deltaDeleteFileNum = 0;        // 记录删除的文件数量
        long totalDeltaFileSize = 0;        // 记录总的新增文件记录大小
        for (; i < inputs.size(); i++) {
            ManifestFileMeta file = inputs.get(i);
            delta.add(file);
            totalManifestSize += file.fileSize;
            deltaDeleteFileNum += file.numDeletedFiles();
            totalDeltaFileSize += file.fileSize();
        }

        //  TODO - 3 : 如果所有增量的 manifest 文件大小不足 {manifest.full-compaction-threshold-size}, 则不触发 full-compaction
        if (totalDeltaFileSize < sizeTrigger) {
            return Optional.empty();
        }

        //  TODO - 5 : 读取拿到所有的增量部分新增文件
        //              如果存在删除记录，并且删除来自 base 集， 则预先保留此部分
        // 读取增量部分的有效的 数据文件元信息 (Manifest-entry)
        // 旨在合并 delete 文件记录， 如果delete 来自之前合并过的 base, 则咱是保留
        Map<Identifier, ManifestEntry> deltaMerged = new LinkedHashMap<>();   // 增量文件集合
        FileEntry.mergeEntries(manifestFile, delta, deltaMerged);

        List<ManifestFileMeta> result = new ArrayList<>();                     // 最终合并完成的 manifest 文件集

        int j = 0;

        // 如果此表是分区表， 则基于遗留的 delete 记录， 找出部分未涉及到的分区，
        // 将部分未涉及到 delete 的 base 集合 manifest 放入 result 中
        if (partitionType.getFieldCount() > 0) {
            // 分区处理,
            // 基于分区判断，从delete entry 中找到delete  涉及到的分区
            // 将分区范围不在系列的 manifest 文件先添加到  result 中

            Set<BinaryRow> deletePartitions = computeDeletePartitions(deltaMerged);   // 记录出现 delete 文件的分区
            // 分区判断
            Optional<Predicate> predicateOpt = convertPartitionToPredicate(partitionType, deletePartitions);
            if (predicateOpt.isPresent()) {
                Predicate predicate = predicateOpt.get();
                for (; j < base.size(); j++) {
                    // TODO: optimize this to binary search.
                    ManifestFileMeta file = base.get(j);
                    if (predicate.test(file.numAddedFiles + file.numDeletedFiles,
                            file.partitionStats.minValues(),
                            file.partitionStats.maxValues(),
                            file.partitionStats.nullCounts())) {
                        break;
                    } else {  // 基于分区判断
                        result.add(file);
                    }
                }
            } else {
                // Delta 中没有 DELETE 条目，Base 无需压缩
                j = base.size();
                result.addAll(base);
            }
        }

        // 拿到所有的 delete Entry 记录
        Set<Identifier> deleteEntries = new HashSet<>();
        deltaMerged.forEach(
                (k, v) -> {
                    if (v.kind() == FileKind.DELETE) {
                        deleteEntries.add(k);
                    }
                }
        );

        List<ManifestEntry> mergedEntries = new ArrayList<>();

        for (; j < base.size(); j++) {   // 遍历所有未决的 base manifest

            ManifestFileMeta file = base.get(j);
            boolean contains = false;
            for (ManifestEntry entry : manifestFile.read(file.fileName, file.fileSize)) {  // 读取当前 manifest 的所有 entry
                checkArgument(entry.kind() == FileKind.ADD);
                if (deleteEntries.contains(entry.identifier())) {  //
                    contains = true;
                } else {
                    mergedEntries.add(entry);
                }
            }
            if (contains) {
                // already read this file into fullMerged
                j++;
                break;
            } else {   // 如果不包含， 该文件直接放入到结果集中不需要压缩
                mergedEntries.clear();
                result.add(file);
            }
        }

        // 2.3. 合并操作

        RollingFileWriter<ManifestEntry, ManifestFileMeta> writer = manifestFile.createRollingWriter();
        Exception exception = null;
        try {

            // 2.3.1 merge mergedEntries
            for (ManifestEntry entry : mergedEntries) {
                writer.write(entry);
            }

            // 2.3.2 merge base files, 写入不在 delete 文件中的新增文间
            for (Supplier<List<ManifestEntry>> reader : FileEntry.readManifestEntries(manifestFile, base.subList(j, base.size()))) {
                for (ManifestEntry entry : reader.get()) {
                    checkArgument(entry.kind() == FileKind.ADD);
                    if (!deleteEntries.contains(entry.identifier())) {
                        writer.write(entry);
                    }
                }
            }

            // 2.3.3 合并 delta 文件
            for (ManifestEntry entry : deltaMerged.values()) {
                if (entry.kind() == FileKind.ADD) {
                    writer.write(entry);
                }
            }
        } catch (Exception e) {
            exception = e;
        } finally {
            if (exception != null) {
                IOUtils.closeQuietly(writer);
                throw exception;
            }
            writer.close();
        }

        // 返回新落地的 manifest 集合
        List<ManifestFileMeta> merged = writer.result();
        result.addAll(merged);
        newMetas.addAll(merged);
        return Optional.of(result);
    }

    private static Set<BinaryRow> computeDeletePartitions(Map<Identifier, ManifestEntry> deltaMerged) {
        Set<BinaryRow> partitions = new HashSet<>();
        for (ManifestEntry manifestEntry : deltaMerged.values()) {
            if (manifestEntry.kind() == FileKind.DELETE) {
                BinaryRow partition = manifestEntry.partition();
                partitions.add(partition);
            }
        }
        return partitions;
    }

    private static Optional<Predicate> convertPartitionToPredicate(RowType partitionType, Set<BinaryRow> partitions) {

        Optional<Predicate> predicateOpt;

        if (!partitions.isEmpty()) {
            RowDataToObjectArrayConverter rowArrayConverter = new RowDataToObjectArrayConverter(partitionType);

            List<Predicate> predicateList = partitions.stream()
                            .map(rowArrayConverter::convert)
                            .map(values -> createPartitionPredicate(partitionType, values))
                            .collect(Collectors.toList());

            predicateOpt = Optional.of(PredicateBuilder.or(predicateList));
        } else {
            predicateOpt = Optional.empty();
        }
        return predicateOpt;
    }
}
