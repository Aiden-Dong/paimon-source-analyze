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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.SortedRun;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * 通用压缩风格是一种压缩风格，适用于需要较低写放大的用例，同时在一定程度上牺牲读取放大和空间放大。
 *
 * <p>参考 RocksDb 通用压缩:
 * https://github.com/facebook/rocksdb/wiki/Universal-Compaction.
 */
public class UniversalCompaction implements CompactStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(UniversalCompaction.class);

    private final int maxSizeAmp;                         // ${compaction.max-size-amplification-percent:200}
    private final int sizeRatio;                          // ${compaction.size-ratio:1}

    // 基于 Sortted-Run 的 compacation 触发策略
    private final int numRunCompactionTrigger;            // ${num-sorted-run.compaction-trigger:5}

    // 控制执行 compaction 的时间间隔
    @Nullable private final Long opCompactionInterval;   // ${compaction.optimization-interval}
    @Nullable private Long lastOptimizedCompaction;

    public UniversalCompaction(int maxSizeAmp, int sizeRatio, int numRunCompactionTrigger) {
        this(maxSizeAmp, sizeRatio, numRunCompactionTrigger, null);
    }

    public UniversalCompaction(
            int maxSizeAmp,
            int sizeRatio,
            int numRunCompactionTrigger,
            @Nullable Duration opCompactionInterval) {
        this.maxSizeAmp = maxSizeAmp;
        this.sizeRatio = sizeRatio;
        this.numRunCompactionTrigger = numRunCompactionTrigger;
        this.opCompactionInterval =
                opCompactionInterval == null ? null : opCompactionInterval.toMillis();
    }

    /***
     * @param runs
     *                      // level-0 的文件，每个文件都是一个 sorted-run
     *                      // level>0 的文件，每层 所有文件对应一个 sorted-run
     **/
    @Override
    public Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs) {
        int maxLevel = numLevels - 1;

        // 如果是设置了执行 compaction 的时间范围， 则按时间进行 compaction
        if (opCompactionInterval != null) {
            if (lastOptimizedCompaction == null || currentTimeMillis() - lastOptimizedCompaction > opCompactionInterval) {
                LOG.debug("Universal compaction due to optimized compaction interval");
                updateLastOptimizedCompaction();
                return Optional.of(CompactUnit.fromLevelRuns(maxLevel, runs));
            }
        }

        // 1 checking for reducing size amplification
        // 判断是否有读放大问题
        CompactUnit unit = pickForSizeAmp(maxLevel, runs);
        if (unit != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to size amplification");
            }
            return Optional.of(unit);
        }

        // 2 checking for size ratio
        // 按照 sizeRatio 阈值， 合并较小的 sortted-run
        unit = pickForSizeRatio(maxLevel, runs);
        if (unit != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to size ratio");
            }
            return Optional.of(unit);
        }

        // 3 checking for file num
        if (runs.size() > numRunCompactionTrigger) {   // 标识 sortted-run 数量过多
            // compacting for file num
            int candidateCount = runs.size() - numRunCompactionTrigger + 1;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Universal compaction due to file num");
            }
            return Optional.ofNullable(pickForSizeRatio(maxLevel, runs, candidateCount));
        }

        return Optional.empty();
    }

    @VisibleForTesting
    CompactUnit pickForSizeAmp(int maxLevel, List<LevelSortedRun> runs) {
        if (runs.size() < numRunCompactionTrigger) {
            return null;
        }

        // 计算总的大小
        // level-0 .... level-max-1 (不包含最后一个 SortedRun)
        long candidateSize = runs.subList(0, runs.size() - 1).stream()
                        .map(LevelSortedRun::run)
                        .mapToLong(SortedRun::totalSize)
                        .sum();

        // 最后一个 level 的 size
        long earliestRunSize = runs.get(runs.size() - 1).run().totalSize();

        // size amplification = percentage of additional size
        // 表示总的 sortted-run 大小是最早的 sorttedRun 大小的2 倍以上(默认)
        // 标识有读放大问题， 则进行 full-compacation
        if (candidateSize * 100 > maxSizeAmp * earliestRunSize) {
            updateLastOptimizedCompaction();
            return CompactUnit.fromLevelRuns(maxLevel, runs);
        }

        return null;
    }

    @VisibleForTesting
    CompactUnit pickForSizeRatio(int maxLevel, List<LevelSortedRun> runs) {
        if (runs.size() < numRunCompactionTrigger) {
            return null;
        }

        return pickForSizeRatio(maxLevel, runs, 1);
    }

    private CompactUnit pickForSizeRatio(
            int maxLevel, List<LevelSortedRun> runs, int candidateCount) {
        return pickForSizeRatio(maxLevel, runs, candidateCount, false);
    }

    public CompactUnit pickForSizeRatio(
            int maxLevel, List<LevelSortedRun> runs, int candidateCount, boolean forcePick) {

        // 计算 [0-candidateCount) 的所有 sorted-run 的总大小
        long candidateSize = candidateSize(runs, candidateCount);  // 读取第一个 SortedRun 大小

        for (int i = candidateCount; i < runs.size(); i++) {
            LevelSortedRun next = runs.get(i);
            // {compaction.size-ratio:1}
            if (candidateSize * (100.0 + sizeRatio) / 100.0 < next.run().totalSize()) {
                break;
            }

            candidateSize += next.run().totalSize();
            candidateCount++;
        }

        if (forcePick || candidateCount > 1) {
            return createUnit(runs, maxLevel, candidateCount);
        }

        return null;
    }

    private long candidateSize(List<LevelSortedRun> runs, int candidateCount) {
        long size = 0;
        for (int i = 0; i < candidateCount; i++) {
            size += runs.get(i).run().totalSize();
        }
        return size;
    }

    @VisibleForTesting
    CompactUnit createUnit(List<LevelSortedRun> runs, int maxLevel, int runCount) {
        int outputLevel;
        if (runCount == runs.size()) {
            outputLevel = maxLevel;
        } else {
            // level of next run - 1
            outputLevel = Math.max(0, runs.get(runCount).level() - 1);
        }

        if (outputLevel == 0) {
            // do not output level 0
            for (int i = runCount; i < runs.size(); i++) {
                LevelSortedRun next = runs.get(i);
                runCount++;
                if (next.level() != 0) {
                    outputLevel = next.level();
                    break;
                }
            }
        }

        if (runCount == runs.size()) {
            updateLastOptimizedCompaction();
            outputLevel = maxLevel;
        }

        return CompactUnit.fromLevelRuns(outputLevel, runs.subList(0, runCount));
    }

    private void updateLastOptimizedCompaction() {
        lastOptimizedCompaction = currentTimeMillis();
    }

    long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}
