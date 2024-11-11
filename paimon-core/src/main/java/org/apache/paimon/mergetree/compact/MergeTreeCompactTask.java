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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.operation.metrics.CompactionMetrics;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static java.util.Collections.singletonList;

/**
 * 合并树压缩的压缩任务。
 **/
public class MergeTreeCompactTask extends CompactTask {

    private final long minFileSize;                      // target-file-size
    private final CompactRewriter rewriter;
    private final int outputLevel;                        // 输出的 level 层

    // 待合并的文件集合
    // 每个 List<SortedRun> 之间数据是不重叠的
    //     List<SortedRun> 内部， 不同的 SortedRun 之间数据可能会重叠
    private final List<List<SortedRun>> partitioned;
    private final boolean dropDelete;                     // 是否删除 delete 数据
    private final int maxLevel;

    // metric
    private int upgradeFilesNum;

    public MergeTreeCompactTask(
            Comparator<InternalRow> keyComparator,   // b
            long minFileSize,
            CompactRewriter rewriter,
            CompactUnit unit,
            boolean dropDelete,
            int maxLevel,
            @Nullable CompactionMetrics.Reporter metricsReporter) {
        super(metricsReporter);
        this.minFileSize = minFileSize;
        this.rewriter = rewriter;
        this.outputLevel = unit.outputLevel();
        this.partitioned = new IntervalPartition(unit.files(), keyComparator).partition();
        this.dropDelete = dropDelete;
        this.maxLevel = maxLevel;

        this.upgradeFilesNum = 0;
    }

    @Override
    protected CompactResult doCompact() throws Exception {
        List<List<SortedRun>> candidate = new ArrayList<>();
        CompactResult result = new CompactResult();

        // 检查顺序并压缩相邻且连续的文件
        // 注意：不能跳过中间文件进行压缩，这会破坏整体有序性
        for (List<SortedRun> section : partitioned) {
            if (section.size() > 1) {  // 表示有重叠数据，需要进行压缩处理
                candidate.add(section);
            } else {
                // 无重叠：
                // 我们可以只升级大文件，只需更改级别，而无需重写
                // 但对于小文件，我们会尝试压缩它
                SortedRun run = section.get(0);

                for (DataFileMeta file : run.files()) {
                    if (file.fileSize() < minFileSize) {                           // 文件太小了，需要合并
                        candidate.add(singletonList(SortedRun.fromSingle(file)));  // 小文件将与之前的文件一起重写
                    } else {
                        // 大文件出现，重写之前的文件并升级它, 必须要将
                        rewrite(candidate, result);
                        upgrade(file, result);
                    }
                }
            }
        }
        rewrite(candidate, result);
        return result;
    }

    @Override
    protected String logMetric(
            long startMillis, List<DataFileMeta> compactBefore, List<DataFileMeta> compactAfter) {
        return String.format(
                "%s, upgrade file num = %d",
                super.logMetric(startMillis, compactBefore, compactAfter), upgradeFilesNum);
    }

    private void upgrade(DataFileMeta file, CompactResult toUpdate) throws Exception {
        if (file.level() == outputLevel) {
            return;
        }

        if (outputLevel != maxLevel || file.deleteRowCount().map(d -> d == 0).orElse(false)) {
            CompactResult upgradeResult = rewriter.upgrade(outputLevel, file);
            toUpdate.merge(upgradeResult);
            upgradeFilesNum++;
        } else {
            // files with delete records should not be upgraded directly to max level
            List<List<SortedRun>> candidate = new ArrayList<>();
            candidate.add(new ArrayList<>());
            candidate.get(0).add(SortedRun.fromSingle(file));
            rewriteImpl(candidate, toUpdate);
        }
    }

    /***
     * 需要将当前的文件重新合并压缩， 写入到新的文件中
     *
     * @param candidate 待压缩的文件集合
     * @param toUpdate  用于收集的压缩结果信息
     */
    private void rewrite(List<List<SortedRun>> candidate, CompactResult toUpdate) throws Exception {
        if (candidate.isEmpty()) {
            return;
        }
        if (candidate.size() == 1) {
            List<SortedRun> section = candidate.get(0);
            if (section.size() == 0) {
                return;
            } else if (section.size() == 1) {   // 如果待压缩的 sortrun 只有一个， 则直接更新
                for (DataFileMeta file : section.get(0).files()) {
                    upgrade(file, toUpdate);
                }
                candidate.clear();
                return;
            }
        }
        rewriteImpl(candidate, toUpdate);
    }

    /***
     * 需要将当前的文件重新合并压缩， 写入到新的文件中
     *
     * @param candidate 待压缩的文件集合
     * @param toUpdate  用于收集的压缩结果信息
     */
    private void rewriteImpl(List<List<SortedRun>> candidate, CompactResult toUpdate)
            throws Exception {
        CompactResult rewriteResult = rewriter.rewrite(outputLevel, dropDelete, candidate);
        toUpdate.merge(rewriteResult);
        candidate.clear();
    }
}
