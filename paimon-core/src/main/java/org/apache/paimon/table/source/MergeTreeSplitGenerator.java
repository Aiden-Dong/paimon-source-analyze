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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.IntervalPartition;
import org.apache.paimon.utils.BinPacking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.MergeEngine.FIRST_ROW;

/** Merge tree implementation of {@link SplitGenerator}. */
public class MergeTreeSplitGenerator implements SplitGenerator {

    private final Comparator<InternalRow> keyComparator;

    private final long targetSplitSize;

    private final long openFileCost;

    private final boolean deletionVectorsEnabled;

    private final MergeEngine mergeEngine;

    public MergeTreeSplitGenerator(
            Comparator<InternalRow> keyComparator,
            long targetSplitSize,
            long openFileCost,
            boolean deletionVectorsEnabled,
            MergeEngine mergeEngine) {
        this.keyComparator = keyComparator;
        this.targetSplitSize = targetSplitSize;
        this.openFileCost = openFileCost;
        this.deletionVectorsEnabled = deletionVectorsEnabled;
        this.mergeEngine = mergeEngine;
    }

    /***
     * 对当前读取计划中的 同一个 partition, bucket 下面的 所有文件进行划分 split 组
     * @param files 待
     * @return
     */
    @Override
    public List<SplitGroup> splitForBatch(List<DataFileMeta> files) {

        // 该文件集合中没有 level 0 文件，并且所有文件中没有 delete 行记录
        boolean rawConvertible =
                files.stream().allMatch(file -> file.level() != 0 && withoutDeleteRow(file));

        // 所有文件只有一层
        boolean oneLevel =
                files.stream().map(DataFileMeta::level).collect(Collectors.toSet()).size() == 1;

        if (rawConvertible && (deletionVectorsEnabled || mergeEngine == FIRST_ROW || oneLevel)) {

            Function<DataFileMeta, Long> weightFunc = file -> Math.max(file.fileSize(), openFileCost);
            // 按照 128M 一个文件组, 如果文件太大不会切割
            return BinPacking.packForOrdered(files, weightFunc, targetSplitSize).stream()
                    .map(SplitGroup::rawConvertibleGroup)
                    .collect(Collectors.toList());
        }

        /**
         * 生成器旨在通过将每个桶的文件切片成多个拆分来并行化扫描执行。
         * 生成有一个约束：具有相交键范围的文件（在一个分区内）必须进入同一个拆分。
         * 因此，文件首先通过区间分区算法生成分区，然后通过有序打包算法。
         * 请注意，这里要打包的项是每个分区，容量表示为目标拆分大小，最终的箱子数量是生成的拆分数量。
         * 例如，有文件：[1, 2] [3, 4] [5, 180] [5, 190] [200, 600] [210, 700]，目标拆分大小为 128M。经过区间分区后，有四个分区：
         * - 分区1：[1, 2]
         * - 分区2：[3, 4]
         * - 分区3：[5, 180]，[5, 190]
         * - 分区4：[200, 600]，[210, 700]
         * 经过有序打包后，分区1和分区2将放入一个箱子（拆分），因此最终结果将是：
         * - 拆分1：[1, 2] [3, 4]
         * - 拆分2：[5, 180] [5,190]
         * - 拆分3：[200, 600] [210, 700]
         */
        List<List<DataFileMeta>> sections = new IntervalPartition(files, keyComparator)
                        .partition().stream().map(this::flatRun).collect(Collectors.toList());

        return packSplits(sections).stream()
                .map(f ->
                        f.size() == 1 && withoutDeleteRow(f.get(0))
                                ? SplitGroup.rawConvertibleGroup(f)
                                : SplitGroup.nonRawConvertibleGroup(f))
                .collect(Collectors.toList());
    }

    @Override
    public List<SplitGroup> splitForStreaming(List<DataFileMeta> files) {
        // We don't split streaming scan files
        return Collections.singletonList(SplitGroup.rawConvertibleGroup(files));
    }

    private List<List<DataFileMeta>> packSplits(List<List<DataFileMeta>> sections) {
        Function<List<DataFileMeta>, Long> weightFunc =
                file -> Math.max(totalSize(file), openFileCost);
        return BinPacking.packForOrdered(sections, weightFunc, targetSplitSize).stream()
                .map(this::flatFiles)
                .collect(Collectors.toList());
    }

    private long totalSize(List<DataFileMeta> section) {
        long size = 0L;
        for (DataFileMeta file : section) {
            size += file.fileSize();
        }
        return size;
    }

    private List<DataFileMeta> flatRun(List<SortedRun> section) {
        List<DataFileMeta> files = new ArrayList<>();
        section.forEach(run -> files.addAll(run.files()));
        return files;
    }

    private List<DataFileMeta> flatFiles(List<List<DataFileMeta>> section) {
        List<DataFileMeta> files = new ArrayList<>();
        section.forEach(files::addAll);
        return files;
    }

    private boolean withoutDeleteRow(DataFileMeta dataFileMeta) {
        // null to true to be compatible with old version
        return dataFileMeta.deleteRowCount().map(count -> count == 0L).orElse(true);
    }
}
