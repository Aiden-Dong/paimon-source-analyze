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

import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.compact.CompactFutureManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.Levels;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.operation.metrics.MetricUtils;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/** {@link KeyValueFileStore} 的压缩管理器。 */
public class MergeTreeCompactManager extends CompactFutureManager {

    private static final Logger LOG = LoggerFactory.getLogger(MergeTreeCompactManager.class);

    private final ExecutorService executor;
    private final Levels levels;
    private final CompactStrategy strategy;
    private final Comparator<InternalRow> keyComparator;
    private final long compactionFileSize;    // target-file-size
    private final int numSortedRunStopTrigger;
    private final CompactRewriter rewriter;

    @Nullable private final CompactionMetrics.Reporter metricsReporter;
    private final boolean deletionVectorsEnabled;

    public MergeTreeCompactManager(
            ExecutorService executor,
            Levels levels,
            CompactStrategy strategy,
            Comparator<InternalRow> keyComparator,
            long compactionFileSize,
            int numSortedRunStopTrigger,
            CompactRewriter rewriter,
            @Nullable CompactionMetrics.Reporter metricsReporter,
            boolean deletionVectorsEnabled) {
        this.executor = executor;
        this.levels = levels;
        this.strategy = strategy;
        this.compactionFileSize = compactionFileSize;
        this.numSortedRunStopTrigger = numSortedRunStopTrigger;
        this.keyComparator = keyComparator;
        this.rewriter = rewriter;
        this.metricsReporter = metricsReporter;
        this.deletionVectorsEnabled = deletionVectorsEnabled;

        MetricUtils.safeCall(this::reportLevel0FileCount, LOG);
    }

    @Override
    public boolean shouldWaitForLatestCompaction() {
        return levels.numberOfSortedRuns() > numSortedRunStopTrigger;
    }

    @Override
    public boolean shouldWaitForPreparingCheckpoint() {
        // cast to long to avoid Numeric overflow
        return levels.numberOfSortedRuns() > (long) numSortedRunStopTrigger + 1;
    }

    @Override
    public void addNewFile(DataFileMeta file) {
        levels.addLevel0File(file);
        MetricUtils.safeCall(this::reportLevel0FileCount, LOG);
    }

    @Override
    public List<DataFileMeta> allFiles() {
        return levels.allFiles();
    }

    @Override
    public void triggerCompaction(boolean fullCompaction) {
        Optional<CompactUnit> optionalUnit;

        // level-0 的文件，每个文件都是一个 sorted-run
        // level>0 的文件，每层 所有文件对应一个 sorted-run
        List<LevelSortedRun> runs = levels.levelSortedRuns();     // 获取包含 level0 在内的所有的待读取的 SortedRun

        // 计算需要参与 compaction 的文件集合
        if (fullCompaction) {   // full-compaction
            optionalUnit = CompactStrategy.pickFullCompaction(levels.numberOfLevels(), runs);

        } else {                // triger-compaction

            if (taskFuture != null) return;

            optionalUnit = strategy.pick(levels.numberOfLevels(), runs)
                            .filter(unit -> unit.files().size() > 0)
                            .filter(unit -> unit.files().size() > 1 || unit.files().get(0).level() != unit.outputLevel());
        }

        optionalUnit.ifPresent(
                unit -> {
                    /***
                     * 只要没有较旧的数据，我们就可以丢弃删除记录。
                     * 如果输出级别为 0，可能存在未参与压缩的较旧数据。
                     * 如果输出级别大于 0，只要当前级别中没有较旧数据，输出就是最旧的，因此可以丢弃删除记录。
                     * 参见 CompactStrategy.pick。
                     */
                    boolean dropDelete = unit.outputLevel() != 0
                                    && (unit.outputLevel() >= levels.nonEmptyHighestLevel() || deletionVectorsEnabled);

                    submitCompaction(unit, dropDelete);
                });
    }

    @VisibleForTesting
    public Levels levels() {
        return levels;
    }

    // 异步提交执行 Compaction 操作
    private void submitCompaction(CompactUnit unit, boolean dropDelete) {

        MergeTreeCompactTask task = new MergeTreeCompactTask(
                        keyComparator,
                        compactionFileSize,
                        rewriter,
                        unit,
                        dropDelete,
                        levels.maxLevel(),
                        metricsReporter);

        taskFuture = executor.submit(task);
    }

    /** Finish current task, and update result files to {@link Levels}. */
    @Override
    public Optional<CompactResult> getCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException {
        Optional<CompactResult> result = innerGetCompactionResult(blocking);
        result.ifPresent(
                r -> {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Update levels in compact manager with these changes:\nBefore:\n{}\nAfter:\n{}",
                                r.before(),
                                r.after());
                    }
                    levels.update(r.before(), r.after());
                    MetricUtils.safeCall(this::reportLevel0FileCount, LOG);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Levels in compact manager updated. Current runs are\n{}",
                                levels.levelSortedRuns());
                    }
                });
        return result;
    }

    private void reportLevel0FileCount() {
        if (metricsReporter != null) {
            metricsReporter.reportLevel0FileCount(levels.level0().size());
        }
    }

    @Override
    public void close() throws IOException {
        rewriter.close();
        if (metricsReporter != null) {
            MetricUtils.safeCall(metricsReporter::unregister, LOG);
        }
    }
}
