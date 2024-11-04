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

package org.apache.paimon.operation;

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.metrics.CommitMetrics;
import org.apache.paimon.operation.metrics.CommitStats;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.index.HashIndexFile.HASH_INDEX;
import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;

/**
 * {@link FileStoreCommit} 的默认实现。
 *
 * <p>此类为用户提供了原子提交方法。
 *
 * <ol>
 *   <li>在调用 {@link FileStoreCommitImpl#commit} 之前，如果用户不能确定此提交是否已完成，
 *       用户应首先调用 {@link FileStoreCommitImpl#filterCommitted}。
 *   <li>在提交之前，它会先通过检查所有要删除的文件是否存在，以及修改后的文件是否与现有文件有重叠的键范围来检查冲突。
 *   <li>之后，它使用外部的 {@link FileStoreCommitImpl#lock}（如果提供）或文件系统的原子重命名来确保原子性。
 *   <li>如果由于冲突或异常导致提交失败，它会尽最大努力清理并中止。
 *   <li>如果原子重命名失败，它将在步骤 2 读取最新快照后重试。
 * </ol>
 *
 * <p>注意：如果您想修改此类，提交期间的任何异常都不能被忽略。它们必须被抛出以重新启动作业。建议运行 FileStoreCommitTest 数千次以确保您的更改是正确的。
 */
public class FileStoreCommitImpl implements FileStoreCommit {

    private static final Logger LOG = LoggerFactory.getLogger(FileStoreCommitImpl.class);

    private final FileIO fileIO;
    private final SchemaManager schemaManager;
    private final String commitUser;
    private final RowType partitionType;
    private final String partitionDefaultName;
    private final FileStorePathFactory pathFactory;
    private final SnapshotManager snapshotManager;
    private final ManifestFile manifestFile;
    private final ManifestList manifestList;
    private final IndexManifestFile indexManifestFile;
    private final FileStoreScan scan;
    private final int numBucket;
    // manifest.target-file-size
    // 建议的manifest文件大小
    private final MemorySize manifestTargetSize;
    private final MemorySize manifestFullCompactionSize;
    // manifest.merge-min-count
    // 为了避免频繁的清单合并，此参数指定要合并的最小 ManifestFileMeta 数量
    private final int manifestMergeMinCount;
    private final boolean dynamicPartitionOverwrite;
    @Nullable private final Comparator<InternalRow> keyComparator;
    private final String branchName;

    @Nullable private Lock lock;
    private boolean ignoreEmptyCommit;

    private CommitMetrics commitMetrics;

    private final StatsFileHandler statsFileHandler;

    public FileStoreCommitImpl(
            FileIO fileIO,
            SchemaManager schemaManager,
            String commitUser,
            RowType partitionType,
            String partitionDefaultName,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory,
            IndexManifestFile.Factory indexManifestFileFactory,
            FileStoreScan scan,
            int numBucket,
            MemorySize manifestTargetSize,
            MemorySize manifestFullCompactionSize,
            int manifestMergeMinCount,
            boolean dynamicPartitionOverwrite,
            @Nullable Comparator<InternalRow> keyComparator,
            String branchName,
            StatsFileHandler statsFileHandler) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.commitUser = commitUser;
        this.partitionType = partitionType;
        this.partitionDefaultName = partitionDefaultName;
        this.pathFactory = pathFactory;
        this.snapshotManager = snapshotManager;
        this.manifestFile = manifestFileFactory.create();
        this.manifestList = manifestListFactory.create();
        this.indexManifestFile = indexManifestFileFactory.create();
        this.scan = scan;
        this.numBucket = numBucket;
        this.manifestTargetSize = manifestTargetSize;
        this.manifestFullCompactionSize = manifestFullCompactionSize;
        this.manifestMergeMinCount = manifestMergeMinCount;
        this.dynamicPartitionOverwrite = dynamicPartitionOverwrite;
        this.keyComparator = keyComparator;
        this.branchName = branchName;

        this.lock = null;
        this.ignoreEmptyCommit = true;
        this.commitMetrics = null;
        this.statsFileHandler = statsFileHandler;
    }

    @Override
    public FileStoreCommit withLock(Lock lock) {
        this.lock = lock;
        return this;
    }

    @Override
    public FileStoreCommit ignoreEmptyCommit(boolean ignoreEmptyCommit) {
        this.ignoreEmptyCommit = ignoreEmptyCommit;
        return this;
    }

    @Override
    public Set<Long> filterCommitted(Set<Long> commitIdentifiers) {
        // nothing to filter, fast exit
        if (commitIdentifiers.isEmpty()) {
            return commitIdentifiers;
        }

        Optional<Snapshot> latestSnapshot = snapshotManager.latestSnapshotOfUser(commitUser);
        if (latestSnapshot.isPresent()) {
            Set<Long> result = new HashSet<>();
            for (Long identifier : commitIdentifiers) {
                // if committable is newer than latest snapshot, then it hasn't been committed
                if (identifier > latestSnapshot.get().commitIdentifier()) {
                    result.add(identifier);
                }
            }
            return result;
        } else {
            // if there is no previous snapshots then nothing should be filtered
            return commitIdentifiers;
        }
    }

    @Override
    public void commit(ManifestCommittable committable, Map<String, String> properties) {
        commit(committable, properties, false);
    }

    @Override
    public void commit(ManifestCommittable committable,    // 数据变更记录
            Map<String, String> properties,
            boolean checkAppendFiles) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to commit\n" + committable.toString());
        }

        long started = System.nanoTime();
        int generatedSnapshot = 0;
        int attempts = 0;
        Snapshot latestSnapshot = null;
        Long safeLatestSnapshotId = null;    // 当前安全可用的 laters - snapshot (经过安全校验以后)
        List<SimpleFileEntry> baseEntries = new ArrayList<>();

        // 整理收集变更信息
        List<ManifestEntry> appendTableFiles = new ArrayList<>();
        List<ManifestEntry> appendChangelog = new ArrayList<>();
        List<ManifestEntry> compactTableFiles = new ArrayList<>();
        List<ManifestEntry> compactChangelog = new ArrayList<>();
        List<IndexManifestEntry> appendHashIndexFiles = new ArrayList<>();
        List<IndexManifestEntry> compactDvIndexFiles = new ArrayList<>();

        collectChanges(committable.fileCommittables(),
                appendTableFiles,
                appendChangelog,
                compactTableFiles,
                compactChangelog,
                appendHashIndexFiles,
                compactDvIndexFiles);

        try {
            // 当前新增的 SST 文件集合
            List<SimpleFileEntry> appendSimpleEntries = SimpleFileEntry.from(appendTableFiles);

            if (!ignoreEmptyCommit || !appendTableFiles.isEmpty()
                    || !appendChangelog.isEmpty()
                    || !appendHashIndexFiles.isEmpty()) {

                // 常见路径的优化。
                // 第一步：
                // 从更改的分区中读取 manifest entrie 并检查冲突。
                // 如果没有其他作业同时提交，
                // 我们可以在 tryCommit 方法中跳过冲突检查。
                // 此优化主要用于减少读取文件的次数。
                latestSnapshot = snapshotManager.latestSnapshot(branchName);  //  从 snapshot 目录 last 文件中获取最后一个 snapshot

                if (latestSnapshot != null && checkAppendFiles) {

                    // 获取最后一次 snapshot 当dui前变更分区下所有有效的SST文件信息
                    baseEntries.addAll(readAllEntriesFromChangedPartitions(latestSnapshot, appendTableFiles, compactTableFiles));

                    // 检查新增的 SST 文件是否跟base的SST文件冲突
                    noConflictsOrFail(latestSnapshot.commitUser(), baseEntries, appendSimpleEntries);

                    safeLatestSnapshotId = latestSnapshot.id();  // 当前安全可用的 laters - snapshot
                }

                attempts += tryCommit(
                                appendTableFiles,            // 新增 sst
                                appendChangelog,             // 新增 changelog
                                appendHashIndexFiles,        // 新增 index
                                committable.identifier(),    // 提交标识
                                committable.watermark(),
                                committable.logOffsets(),
                                Snapshot.CommitKind.APPEND,
                                noConflictCheck(),
                                branchName,
                                null);
                generatedSnapshot += 1;
            }

            if (!compactTableFiles.isEmpty()
                    || !compactChangelog.isEmpty()
                    || !compactDvIndexFiles.isEmpty()) {

                // 常见路径的优化。
                // 第二步：
                // 将 appendChanges 添加到上面读取的清单条目中，并检查冲突。
                // 如果没有其他作业同时提交，
                // 我们可以在 tryCommit 方法中跳过冲突检查。
                // 此优化主要用于减少读取文件的次数。
                if (safeLatestSnapshotId != null) {
                    baseEntries.addAll(appendSimpleEntries);  // 新增的SST 文件

                    // 检查 baseEntries 跟 compaction 是否冲突
                    noConflictsOrFail(latestSnapshot.commitUser(), baseEntries, SimpleFileEntry.from(compactTableFiles));
                    // assume this compact commit follows just after the append commit created above
                    safeLatestSnapshotId += 1;
                }

                // 重新提交compaction 类型
                attempts += tryCommit(
                    compactTableFiles,
                    compactChangelog,
                    compactDvIndexFiles,
                    committable.identifier(),
                    committable.watermark(),
                    committable.logOffsets(),
                    Snapshot.CommitKind.COMPACT,
                    hasConflictChecked(safeLatestSnapshotId),
                    branchName,
                    null);
                generatedSnapshot += 1;
            }

        } finally {
            long commitDuration = (System.nanoTime() - started) / 1_000_000;
            if (this.commitMetrics != null) {
                reportCommit(
                        appendTableFiles,
                        appendChangelog,
                        compactTableFiles,
                        compactChangelog,
                        commitDuration,
                        generatedSnapshot,
                        attempts);
            }
        }
    }

    private void reportCommit(
            List<ManifestEntry> appendTableFiles,
            List<ManifestEntry> appendChangelogFiles,
            List<ManifestEntry> compactTableFiles,
            List<ManifestEntry> compactChangelogFiles,
            long commitDuration,
            int generatedSnapshots,
            int attempts) {

        CommitStats commitStats = new CommitStats(
                        appendTableFiles,
                        appendChangelogFiles,
                        compactTableFiles,
                        compactChangelogFiles,
                        commitDuration,
                        generatedSnapshots,
                        attempts);
        commitMetrics.reportCommit(commitStats);
    }

    @Override
    public void overwrite(Map<String, String> partition, ManifestCommittable committable, Map<String, String> properties) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to overwrite partition {}\nManifestCommittable: {}\nProperties: {}",
                    partition, committable, properties);
        }

        long started = System.nanoTime();
        int generatedSnapshot = 0;
        int attempts = 0;

        List<ManifestEntry> appendTableFiles = new ArrayList<>();
        List<ManifestEntry> appendChangelog = new ArrayList<>();
        List<ManifestEntry> compactTableFiles = new ArrayList<>();
        List<ManifestEntry> compactChangelog = new ArrayList<>();
        List<IndexManifestEntry> appendHashIndexFiles = new ArrayList<>();
        List<IndexManifestEntry> compactDvIndexFiles = new ArrayList<>();

        // 将本次的数据分类收集
        collectChanges(
                committable.fileCommittables(),
                appendTableFiles,
                appendChangelog,
                compactTableFiles,
                compactChangelog,
                appendHashIndexFiles,
                compactDvIndexFiles);

        if (!appendChangelog.isEmpty() || !compactChangelog.isEmpty()) {
            StringBuilder warnMessage =
                    new StringBuilder(
                            "Overwrite mode currently does not commit any changelog.\n"
                                    + "Please make sure that the partition you're overwriting  is not being consumed by a streaming reader.\n"
                                    + "Ignored changelog files are:\n");

            for (ManifestEntry entry : appendChangelog) {
                warnMessage.append("  * ").append(entry.toString()).append("\n");
            }
            for (ManifestEntry entry : compactChangelog) {
                warnMessage.append("  * ").append(entry.toString()).append("\n");
            }
            LOG.warn(warnMessage.toString());
        }

        try {
            boolean skipOverwrite = false;
            // 分区过滤器是根据属性从静态或动态分区构建的
            Predicate partitionFilter = null;
            if (dynamicPartitionOverwrite) {
                //  如果是动态分区覆盖， 则从新增文件信息中获取到所有的覆盖分区
                if (appendTableFiles.isEmpty()) {
                    // in dynamic mode, if there is no changes to commit, no data will be deleted
                    skipOverwrite = true;
                } else {
                    partitionFilter = appendTableFiles.stream()
                                    .map(ManifestEntry::partition)
                                    .distinct()
                                    // partition filter is built from new data's partitions
                                    .map(p -> createPartitionPredicate(partitionType, p))
                                    .reduce(PredicateBuilder::or)
                                    .orElseThrow(
                                            () ->
                                                    new RuntimeException(
                                                            "Failed to get dynamic partition filter. This is unexpected."));
                }
            } else {
                // 如果是非动态分区覆盖， 则创建单一的静态过滤器
                partitionFilter = createPartitionPredicate(partition, partitionType, partitionDefaultName);
                // 校验写入文件是否符合分区条件
                if (partitionFilter != null) {
                    for (ManifestEntry entry : appendTableFiles) {
                        if (!partitionFilter.test(entry.partition())) {
                            throw new IllegalArgumentException(
                                    "Trying to overwrite partition " + partition + ", but the changes in "
                                            + pathFactory.getPartitionString(entry.partition()) + " does not belong to this partition");
                        }
                    }
                }
            }

            // 执行 overwrite 事物操作
            if (!skipOverwrite) {
                attempts += tryOverwrite(
                                partitionFilter,
                                appendTableFiles,
                                appendHashIndexFiles,
                                committable.identifier(),
                                committable.watermark(),
                                committable.logOffsets());

                generatedSnapshot += 1;
            }

            if (!compactTableFiles.isEmpty() || !compactDvIndexFiles.isEmpty()) {
                attempts +=
                        tryCommit(
                                compactTableFiles,
                                Collections.emptyList(),
                                compactDvIndexFiles,
                                committable.identifier(),
                                committable.watermark(),
                                committable.logOffsets(),
                                Snapshot.CommitKind.COMPACT,
                                mustConflictCheck(),
                                branchName,
                                null);
                generatedSnapshot += 1;
            }
        } finally {
            long commitDuration = (System.nanoTime() - started) / 1_000_000;
            if (this.commitMetrics != null) {
                reportCommit(
                        appendTableFiles,
                        Collections.emptyList(),
                        compactTableFiles,
                        Collections.emptyList(),
                        commitDuration,
                        generatedSnapshot,
                        attempts);
            }
        }
    }

    @Override
    public void dropPartitions(List<Map<String, String>> partitions, long commitIdentifier) {
        Preconditions.checkArgument(!partitions.isEmpty(), "Partitions list cannot be empty.");

        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to drop partitions {}",
                    partitions.stream().map(Objects::toString).collect(Collectors.joining(",")));
        }

        Predicate partitionFilter =
                partitions.stream()
                        .map(partition -> createPartitionPredicate(partition, partitionType, partitionDefaultName))
                        .reduce(PredicateBuilder::or)
                        .orElseThrow(() -> new RuntimeException("Failed to get partition filter."));

        tryOverwrite(
                partitionFilter,
                Collections.emptyList(),
                Collections.emptyList(),
                commitIdentifier,
                null,
                new HashMap<>());
    }

    @Override
    public void truncateTable(long commitIdentifier) {
        tryOverwrite(
                null,
                Collections.emptyList(),
                Collections.emptyList(),
                commitIdentifier,
                null,
                new HashMap<>());
    }

    @Override
    public void abort(List<CommitMessage> commitMessages) {
        Map<Pair<BinaryRow, Integer>, DataFilePathFactory> factoryMap = new HashMap<>();
        for (CommitMessage message : commitMessages) {
            DataFilePathFactory pathFactory =
                    factoryMap.computeIfAbsent(
                            Pair.of(message.partition(), message.bucket()),
                            k -> this.pathFactory.createDataFilePathFactory(k.getKey(), k.getValue()));
            CommitMessageImpl commitMessage = (CommitMessageImpl) message;
            List<DataFileMeta> toDelete = new ArrayList<>();
            toDelete.addAll(commitMessage.newFilesIncrement().newFiles());
            toDelete.addAll(commitMessage.newFilesIncrement().changelogFiles());
            toDelete.addAll(commitMessage.compactIncrement().compactAfter());
            toDelete.addAll(commitMessage.compactIncrement().changelogFiles());

            for (DataFileMeta file : toDelete) {
                fileIO.deleteQuietly(pathFactory.toPath(file.fileName()));
            }
        }
    }

    @Override
    public FileStoreCommit withMetrics(CommitMetrics metrics) {
        this.commitMetrics = metrics;
        return this;
    }

    @Override
    public void commitStatistics(Statistics stats, long commitIdentifier) {
        String statsFileName = statsFileHandler.writeStats(stats);
        tryCommit(
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                commitIdentifier,
                null,
                Collections.emptyMap(),
                Snapshot.CommitKind.ANALYZE,
                noConflictCheck(),
                branchName,
                statsFileName);
    }

    @Override
    public FileStorePathFactory pathFactory() {
        return pathFactory;
    }

    @Override
    public FileIO fileIO() {
        return fileIO;
    }

    // 分类收集本次变更的相关事件集合
    private void collectChanges(
            List<CommitMessage> commitMessages,
            List<ManifestEntry> appendTableFiles,
            List<ManifestEntry> appendChangelog,
            List<ManifestEntry> compactTableFiles,
            List<ManifestEntry> compactChangelog,
            List<IndexManifestEntry> appendHashIndexFiles,
            List<IndexManifestEntry> compactDvIndexFiles) {
        for (CommitMessage message : commitMessages) {
            CommitMessageImpl commitMessage = (CommitMessageImpl) message;

            commitMessage.newFilesIncrement()
                    .newFiles()
                    .forEach(m -> appendTableFiles.add(makeEntry(FileKind.ADD, commitMessage, m)));

            commitMessage
                    .newFilesIncrement()
                    .deletedFiles()
                    .forEach(m -> appendTableFiles.add(makeEntry(FileKind.DELETE, commitMessage, m)));

            commitMessage
                    .newFilesIncrement()
                    .changelogFiles()
                    .forEach(m -> appendChangelog.add(makeEntry(FileKind.ADD, commitMessage, m)));

            commitMessage
                    .compactIncrement()
                    .compactBefore()
                    .forEach(m -> compactTableFiles.add(makeEntry(FileKind.DELETE, commitMessage, m)));

            commitMessage
                    .compactIncrement()
                    .compactAfter()
                    .forEach(m -> compactTableFiles.add(makeEntry(FileKind.ADD, commitMessage, m)));

            commitMessage
                    .compactIncrement()
                    .changelogFiles()
                    .forEach(m -> compactChangelog.add(makeEntry(FileKind.ADD, commitMessage, m)));

            commitMessage
                    .indexIncrement()
                    .newIndexFiles()
                    .forEach(
                            f -> {
                                switch (f.indexType()) {
                                    case HASH_INDEX:
                                        appendHashIndexFiles.add(
                                                new IndexManifestEntry(
                                                        FileKind.ADD,
                                                        commitMessage.partition(),
                                                        commitMessage.bucket(),
                                                        f));
                                        break;
                                    case DELETION_VECTORS_INDEX:
                                        compactDvIndexFiles.add(
                                                new IndexManifestEntry(
                                                        FileKind.ADD,
                                                        commitMessage.partition(),
                                                        commitMessage.bucket(),
                                                        f));
                                        break;

                                    default:
                                        throw new RuntimeException("Unknown index type: " + f.indexType());
                                }
                            });
        }
    }

    private ManifestEntry makeEntry(FileKind kind, CommitMessage commitMessage, DataFileMeta file) {
        return new ManifestEntry(
                kind, commitMessage.partition(), commitMessage.bucket(), numBucket, file);
    }

    private int tryCommit(
            List<ManifestEntry> tableFiles,       // 新增 sst 文件
            List<ManifestEntry> changelogFiles,   // 新增 changelog 文件
            List<IndexManifestEntry> indexFiles,  // 新增 index 文件
            long identifier,                      // 提交标识
            @Nullable Long watermark,
            Map<Integer, Long> logOffsets,
            Snapshot.CommitKind commitKind,
            ConflictCheck conflictCheck,
            String branchName,
            @Nullable String statsFileName) {

        int cnt = 0;  // 重试次数

        while (true) {
            Snapshot latestSnapshot = snapshotManager.latestSnapshot(branchName);  // 获取最终的 snapshot id

            cnt++;

            if (tryCommitOnce(
                    tableFiles,
                    changelogFiles,
                    indexFiles,
                    identifier,
                    watermark,
                    logOffsets,
                    commitKind,
                    latestSnapshot,
                    conflictCheck,
                    branchName,
                    statsFileName)) {
                break;
            }
        }
        return cnt;
    }

    private int tryOverwrite(
            Predicate partitionFilter,
            List<ManifestEntry> changes,
            List<IndexManifestEntry> indexFiles,
            long identifier,
            @Nullable Long watermark,
            Map<Integer, Long> logOffsets) {
        int cnt = 0;
        while (true) {
            // 从当前文件中读取最新的 snapshot
            Snapshot latestSnapshot = snapshotManager.latestSnapshot();

            cnt++;
            List<ManifestEntry> changesWithOverwrite = new ArrayList<>();
            List<IndexManifestEntry> indexChangesWithOverwrite = new ArrayList<>();
            if (latestSnapshot != null) {
                // 读取最后一次snapshot 的当前分区所有有效的 manifest
                List<ManifestEntry> currentEntries =
                        scan.withSnapshot(latestSnapshot).withPartitionFilter(partitionFilter).plan().files();
                // 将匹配到的所有文件增加删除标志，标识文件被删除
                for (ManifestEntry entry : currentEntries) {
                    changesWithOverwrite.add(
                            new ManifestEntry(FileKind.DELETE, entry.partition(), entry.bucket(), entry.totalBuckets(), entry.file()));
                }

                // 同样的将索引文件增加 删除标识
                if (latestSnapshot.indexManifest() != null) {
                    List<IndexManifestEntry> entries =
                            indexManifestFile.read(latestSnapshot.indexManifest());
                    for (IndexManifestEntry entry : entries) {
                        if (partitionFilter == null || partitionFilter.test(entry.partition())) {
                            indexChangesWithOverwrite.add(entry.toDeleteEntry());
                        }
                    }
                }
            }

            // 整理收集当前的完整变更信息， 包含 overwrite 对历史文件的删除标记
            changesWithOverwrite.addAll(changes);
            indexChangesWithOverwrite.addAll(indexFiles);

            if (tryCommitOnce(
                    changesWithOverwrite,
                    Collections.emptyList(),
                    indexChangesWithOverwrite,
                    identifier,
                    watermark,
                    logOffsets,
                    Snapshot.CommitKind.OVERWRITE,
                    latestSnapshot,
                    mustConflictCheck(),
                    branchName,
                    null)) {
                break;
            }
        }
        return cnt;
    }

    @VisibleForTesting
    public boolean tryCommitOnce(
            List<ManifestEntry> tableFiles,
            List<ManifestEntry> changelogFiles,
            List<IndexManifestEntry> indexFiles,
            long identifier,
            @Nullable Long watermark,
            Map<Integer, Long> logOffsets,
            Snapshot.CommitKind commitKind,
            @Nullable Snapshot latestSnapshot,
            ConflictCheck conflictCheck,
            String branchName,
            @Nullable String newStatsFileName) {

        // TODO : 为待提交的 snapshot 生成 ID
        long newSnapshotId = latestSnapshot == null ? Snapshot.FIRST_SNAPSHOT_ID : latestSnapshot.id() + 1;

        // 基于最新的 snapshot ，确定 snapshot 存储位置
        Path newSnapshotPath = branchName.equals(DEFAULT_MAIN_BRANCH) ? snapshotManager.snapshotPath(newSnapshotId)
                        : snapshotManager.branchSnapshotPath(branchName, newSnapshotId);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to commit table files to snapshot #" + newSnapshotId);
            for (ManifestEntry entry : tableFiles) {
                LOG.debug("  * " + entry.toString());
            }
            LOG.debug("Ready to commit changelog to snapshot #" + newSnapshotId);
            for (ManifestEntry entry : changelogFiles) {
                LOG.debug("  * " + entry.toString());
            }
        }


        // TODO : 检查待提交的snapshot 跟 表中最新的 snapshot 是否有冲突
        if (latestSnapshot != null && conflictCheck.shouldCheck(latestSnapshot.id())) {
            // latestSnapshotId 与我们已检查冲突的snapshot ID 不同，因此我们必须再次检查
            noConflictsOrFail(latestSnapshot.commitUser(), latestSnapshot, tableFiles);
        }

        Snapshot newSnapshot;
        String previousChangesListName = null;
        String newChangesListName = null;
        String changelogListName = null;
        String newIndexManifest = null;
        List<ManifestFileMeta> oldMetas = new ArrayList<>();  // 历史 manifest 文件信息
        List<ManifestFileMeta> newMetas = new ArrayList<>();
        List<ManifestFileMeta> changelogMetas = new ArrayList<>();

        try {
            long previousTotalRecordCount = 0L;
            Long currentWatermark = watermark;
            String previousIndexManifest = null;

            // 记录历史 manifest 文件
            if (latestSnapshot != null) {
                // 获取提交之前的总的数据量
                previousTotalRecordCount = latestSnapshot.totalRecordCount(scan);

                // 读取上个版本的 manifest集合 - base + delta
                List<ManifestFileMeta> previousManifests = latestSnapshot.dataManifests(manifestList);

                // read all previous manifest files
                oldMetas.addAll(previousManifests);

                // 读取最后一个快照以补全 bucket 的偏移，当 logOffsets 未包含所有 bucket 时
                // log 文件
                Map<Integer, Long> latestLogOffsets = latestSnapshot.logOffsets();

                if (latestLogOffsets != null) {
                    latestLogOffsets.forEach(logOffsets::putIfAbsent);
                }

                Long latestWatermark = latestSnapshot.watermark();

                if (latestWatermark != null) {
                    currentWatermark = currentWatermark == null ? latestWatermark : Math.max(currentWatermark, latestWatermark);
                }

                // 索引 manifest
                previousIndexManifest = latestSnapshot.indexManifest();
            }


            // merge manifest files with changes
            // 合并历史 Manifest 文件与新的
            newMetas.addAll(
                    ManifestFileMeta.merge(
                            oldMetas,
                            manifestFile,
                            manifestTargetSize.getBytes(),
                            manifestMergeMinCount,
                            manifestFullCompactionSize.getBytes(),
                            partitionType));

            // 写入一个新的 manifest-list base 文件
            previousChangesListName = manifestList.write(newMetas);

            // 记录写入后的总的数据量
            long deltaRecordCount = Snapshot.recordCountAdd(tableFiles) - Snapshot.recordCountDelete(tableFiles);
            long totalRecordCount = previousTotalRecordCount + deltaRecordCount;

            // 对于当前未提交的 manifest , xieru
            List<ManifestFileMeta> newChangesManifests = manifestFile.write(tableFiles);
            newMetas.addAll(newChangesManifests);
            newChangesListName = manifestList.write(newChangesManifests);

            // write changelog into manifest files
            if (!changelogFiles.isEmpty()) {
                changelogMetas.addAll(manifestFile.write(changelogFiles));
                changelogListName = manifestList.write(changelogMetas);
            }

            // write new index manifest
            String indexManifest = indexManifestFile.merge(previousIndexManifest, indexFiles);
            if (!Objects.equals(indexManifest, previousIndexManifest)) {
                newIndexManifest = indexManifest;
            }

            // 获取schema信息
            long latestSchemaId = schemaManager.latest(branchName).get().id();

            // write new stats or inherit from the previous snapshot
            String statsFileName = null;
            if (newStatsFileName != null) {
                statsFileName = newStatsFileName;
            } else if (latestSnapshot != null) {
                Optional<Statistics> previousStatistic = statsFileHandler.readStats(latestSnapshot);
                if (previousStatistic.isPresent()) {
                    if (previousStatistic.get().schemaId() != latestSchemaId) {
                        LOG.warn("Schema changed, stats will not be inherited");
                    } else {
                        statsFileName = latestSnapshot.statistics();
                    }
                }
            }

            // 构建一个新的 Snaphsot
            newSnapshot = new Snapshot(
                newSnapshotId,
                latestSchemaId,
                previousChangesListName,
                newChangesListName,
                changelogListName,
                indexManifest,
                commitUser,
                identifier,
                commitKind,
                System.currentTimeMillis(),
                logOffsets,
                totalRecordCount,
                deltaRecordCount,
                Snapshot.recordCount(changelogFiles),
                currentWatermark,
                statsFileName);

        } catch (Throwable e) {
            // 如果提交过程中发生错误， 则清理 当前写入的中间 manifest 文件
            cleanUpTmpManifests(
                    previousChangesListName,
                    newChangesListName,
                    changelogListName,
                    newIndexManifest,
                    oldMetas,
                    newMetas,
                    changelogMetas);
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when preparing snapshot #%d (path %s) by user %s "
                                    + "with hash %s and kind %s. Clean up.",
                            newSnapshotId,
                            newSnapshotPath.toString(),
                            commitUser,
                            identifier,
                            commitKind.name()),
                    e);
        }

        // 记录 snapshot 信息
        boolean success;
        try {

            Callable<Boolean> callable = () -> {
                boolean committed = fileIO.writeFileUtf8(newSnapshotPath, newSnapshot.toJson());
                if (committed) {
                    snapshotManager.commitLatestHint(newSnapshotId, branchName);
                }
                return committed;
            };

            if (lock != null) {
                success = lock.runWithLock(
                                () ->
                                        // fs.rename 可能不会返回 false 即使目标文件已存在，或者操作甚至不是原子的
                                        // 由于我们依赖外部锁定，我们可以先检查文件是否存在，然后重命名以解决这种情况
                                        !fileIO.exists(newSnapshotPath) && callable.call());
            } else {
                success = callable.call();
            }
        } catch (Throwable e) {
            // exception when performing the atomic rename,
            // we cannot clean up because we can't determine the success
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when committing snapshot #%d (path %s) by user %s "
                                    + "with identifier %s and kind %s. "
                                    + "Cannot clean up because we can't determine the success.",
                            newSnapshotId,
                            newSnapshotPath,
                            commitUser,
                            identifier,
                            commitKind.name()),
                    e);
        }

        if (success) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        String.format(
                                "Successfully commit snapshot #%d (path %s) by user %s "
                                        + "with identifier %s and kind %s.",
                                newSnapshotId,
                                newSnapshotPath,
                                commitUser,
                                identifier,
                                commitKind.name()));
            }
            return true;
        }

        // atomic rename fails, clean up and try again
        LOG.warn(
                String.format(
                        "Atomic commit failed for snapshot #%d (path %s) by user %s "
                                + "with identifier %s and kind %s. "
                                + "Clean up and try again.",
                        newSnapshotId, newSnapshotPath, commitUser, identifier, commitKind.name()));
        cleanUpTmpManifests(
                previousChangesListName,
                newChangesListName,
                changelogListName,
                newIndexManifest,
                oldMetas,
                newMetas,
                changelogMetas);
        return false;
    }

    @SafeVarargs
    private final List<SimpleFileEntry> readAllEntriesFromChangedPartitions(Snapshot snapshot, List<ManifestEntry>... changes) {

        List<BinaryRow> changedPartitions =
                Arrays.stream(changes)
                        .flatMap(Collection::stream)
                        .map(ManifestEntry::partition)
                        .distinct()
                        .collect(Collectors.toList());
        try {
            return scan.withSnapshot(snapshot)  // 获取当前 snapshot
                    .withPartitionFilter(changedPartitions)  // 获取当前影响分区
                    .readSimpleEntries();
        } catch (Throwable e) {
            throw new RuntimeException("Cannot read manifest entries from changed partitions.", e);
        }
    }

    private void noConflictsOrFail(
            String baseCommitUser, Snapshot latestSnapshot, List<ManifestEntry> changes) {
        noConflictsOrFail(
                baseCommitUser,
                readAllEntriesFromChangedPartitions(latestSnapshot, changes),
                SimpleFileEntry.from(changes));
    }

    /***
     * 版本冲突性校验 ：
     *    1. 首先最新的待提交 snapshot 与 已提交最新的 snapshot 是否存在 manifest 冲突
     *    2. 检查 当待提交 snaphsot 加入进来以后，是否符合 lsm 规范
     *
     * @param baseEntries         当前表中最后一个 snapshot 的所有变更文件
     * @param changes             当前新增的 snapshot 的文件变更信息
     */
    private void noConflictsOrFail(
            String baseCommitUser,
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> changes) {

        // 当前所有的 entry 集合， 包含已提交的最后一个 snapshot 与未提交 snapshot 的 合并 entry 类型
        List<SimpleFileEntry> allEntries = new ArrayList<>(baseEntries);
        allEntries.addAll(changes);

        Collection<SimpleFileEntry> mergedEntries;

        // TODO-1 : 合并所有的 Entry 信息，包含当前所有有效的entry 与 新增的 Entry 事件
        //          返回当前有效的 entry 视图(去除已删除)
        //          合并过程中判断说过有合并的逻辑失败情况，如果有失败，标识当前合并有冲突，
        try {
            // merge manifest entries and also check if the files we want to delete are still there
            mergedEntries = FileEntry.mergeEntries(allEntries);
            FileEntry.assertNoDelete(mergedEntries);
        } catch (Throwable e) {
            Pair<RuntimeException, RuntimeException> conflictException =
                    createConflictException(
                            "File deletion conflicts detected! Give up committing.",
                            baseCommitUser,
                            baseEntries,
                            changes,
                            e,
                            50);
            LOG.warn("", conflictException.getLeft());
            throw conflictException.getRight();
        }

        // TODO-2 : 判断最新的合并树, 是否符合规范限制
        //          对于 level >= 2 层级的所有文件，必须保证文件的全局有序性
        if (keyComparator == null) return;

        // 收集 level >= 2 的所有有效的 entry 信息
        Map<LevelIdentifier, List<SimpleFileEntry>> levels = new HashMap<>();
        for (SimpleFileEntry entry : mergedEntries) {
            int level = entry.level();
            if (level >= 1) {
                levels.computeIfAbsent(new LevelIdentifier(entry.partition(), entry.bucket(), level), lv -> new ArrayList<>()).add(entry);
            }
        }

        for (List<SimpleFileEntry> entries : levels.values()) {
            // 对每个 level 的所有 entry 进行按照 minKey 进行排序
            entries.sort((a, b) -> keyComparator.compare(a.minKey(), b.minKey()));
            for (int i = 0; i + 1 < entries.size(); i++) {
                SimpleFileEntry a = entries.get(i);
                SimpleFileEntry b = entries.get(i + 1);
                // 判断 相邻之间 entry 是否有 key 重叠
                if (keyComparator.compare(a.maxKey(), b.minKey()) >= 0) {
                    Pair<RuntimeException, RuntimeException> conflictException =
                            createConflictException(
                                    "LSM conflicts detected! Give up committing. Conflict files are:\n"
                                            + a.identifier().toString(pathFactory)
                                            + "\n"
                                            + b.identifier().toString(pathFactory),
                                    baseCommitUser,
                                    baseEntries,
                                    changes,
                                    null,
                                    50);

                    LOG.warn("", conflictException.getLeft());
                    throw conflictException.getRight();
                }
            }
        }
    }

    /**
     * Construct detailed conflict exception. The returned exception is formed of (full exception,
     * simplified exception), The simplified exception is generated when the entry length is larger
     * than the max limit.
     */
    private Pair<RuntimeException, RuntimeException> createConflictException(
            String message,
            String baseCommitUser,
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> changes,
            Throwable cause,
            int maxEntry) {
        String possibleCauses =
                String.join(
                        "\n",
                        "Don't panic!",
                        "Conflicts during commits are normal and this failure is intended to resolve the conflicts.",
                        "Conflicts are mainly caused by the following scenarios:",
                        "1. Your job is suffering from back-pressuring.",
                        "   There are too many snapshots waiting to be committed "
                                + "and an exception occurred during the commit procedure "
                                + "(most probably due to checkpoint timeout).",
                        "   See https://paimon.apache.org/docs/master/maintenance/write-performance/ "
                                + "for how to improve writing performance.",
                        "2. Multiple jobs are writing into the same partition at the same time, "
                                + "or you use STATEMENT SET to execute multiple INSERT statements into the same Paimon table.",
                        "   You'll probably see different base commit user and current commit user below.",
                        "   You can use "
                                + "https://paimon.apache.org/docs/master/maintenance/dedicated-compaction#dedicated-compaction-job"
                                + " to support multiple writing.",
                        "3. You're recovering from an old savepoint, or you're creating multiple jobs from a savepoint.",
                        "   The job will fail continuously in this scenario to protect metadata from corruption.",
                        "   You can either recover from the latest savepoint, "
                                + "or you can revert the table to the snapshot corresponding to the old savepoint.");
        String commitUserString =
                "Base commit user is: "
                        + baseCommitUser
                        + "; Current commit user is: "
                        + commitUser;
        String baseEntriesString =
                "Base entries are:\n"
                        + baseEntries.stream()
                                .map(Object::toString)
                                .collect(Collectors.joining("\n"));
        String changesString =
                "Changes are:\n"
                        + changes.stream().map(Object::toString).collect(Collectors.joining("\n"));

        RuntimeException fullException =
                new RuntimeException(
                        message
                                + "\n\n"
                                + possibleCauses
                                + "\n\n"
                                + commitUserString
                                + "\n\n"
                                + baseEntriesString
                                + "\n\n"
                                + changesString,
                        cause);

        RuntimeException simplifiedException;
        if (baseEntries.size() > maxEntry || changes.size() > maxEntry) {
            baseEntriesString =
                    "Base entries are:\n"
                            + baseEntries.subList(0, Math.min(baseEntries.size(), maxEntry))
                                    .stream()
                                    .map(Object::toString)
                                    .collect(Collectors.joining("\n"));
            changesString =
                    "Changes are:\n"
                            + changes.subList(0, Math.min(changes.size(), maxEntry)).stream()
                                    .map(Object::toString)
                                    .collect(Collectors.joining("\n"));
            simplifiedException =
                    new RuntimeException(
                            message
                                    + "\n\n"
                                    + possibleCauses
                                    + "\n\n"
                                    + commitUserString
                                    + "\n\n"
                                    + baseEntriesString
                                    + "\n\n"
                                    + changesString
                                    + "\n\n"
                                    + "The entry list above are not fully displayed, please refer to taskmanager.log for more information.",
                            cause);
            return Pair.of(fullException, simplifiedException);
        } else {
            return Pair.of(fullException, fullException);
        }
    }

    private void cleanUpTmpManifests(
            String previousChangesListName,
            String newChangesListName,
            String changelogListName,
            String newIndexManifest,
            List<ManifestFileMeta> oldMetas,
            List<ManifestFileMeta> newMetas,
            List<ManifestFileMeta> changelogMetas) {
        // clean up newly created manifest list
        if (previousChangesListName != null) {
            manifestList.delete(previousChangesListName);
        }
        if (newChangesListName != null) {
            manifestList.delete(newChangesListName);
        }
        if (changelogListName != null) {
            manifestList.delete(changelogListName);
        }
        if (newIndexManifest != null) {
            indexManifestFile.delete(newIndexManifest);
        }
        // clean up newly merged manifest files
        Set<ManifestFileMeta> oldMetaSet = new HashSet<>(oldMetas); // for faster searching
        for (ManifestFileMeta suspect : newMetas) {
            if (!oldMetaSet.contains(suspect)) {
                manifestList.delete(suspect.fileName());
            }
        }
        // clean up changelog manifests
        for (ManifestFileMeta meta : changelogMetas) {
            manifestList.delete(meta.fileName());
        }
    }

    private static class LevelIdentifier {

        private final BinaryRow partition;
        private final int bucket;
        private final int level;

        private LevelIdentifier(BinaryRow partition, int bucket, int level) {
            this.partition = partition;
            this.bucket = bucket;
            this.level = level;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof LevelIdentifier)) {
                return false;
            }
            LevelIdentifier that = (LevelIdentifier) o;
            return Objects.equals(partition, that.partition)
                    && bucket == that.bucket
                    && level == that.level;
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, bucket, level);
        }
    }

    /** Should do conflict check. */
    interface ConflictCheck {
        boolean shouldCheck(long latestSnapshot);
    }

    static ConflictCheck hasConflictChecked(@Nullable Long checkedLatestSnapshotId) {
        return latestSnapshot -> !Objects.equals(latestSnapshot, checkedLatestSnapshotId);
    }

    static ConflictCheck noConflictCheck() {
        return latestSnapshot -> false;
    }

    public static ConflictCheck mustConflictCheck() {
        return latestSnapshot -> true;
    }
}
