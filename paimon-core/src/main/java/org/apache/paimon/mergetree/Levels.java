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

package org.apache.paimon.mergetree;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.Preconditions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 存储合并树所有层级文件的类。
 **/
public class Levels {

    private final Comparator<InternalRow> keyComparator;
    private final TreeSet<DataFileMeta> level0;            // 存放 level=0  文件元信息集合
    private final List<SortedRun> levels;                  // 存放 level>0  文件元信息集合

    private final List<DropFileCallback> dropFileCallbacks = new ArrayList<>();

    /***
     * @param keyComparator    比较器
     * @param inputFiles       当前要读取的所有文件
     * @param numLevels        level 树的最高等级
     */
    public Levels(Comparator<InternalRow> keyComparator, List<DataFileMeta> inputFiles, int numLevels) {

        this.keyComparator = keyComparator;

        int restoredNumLevels = Math.max(numLevels,
                        inputFiles.stream().mapToInt(DataFileMeta::level).max().orElse(-1) + 1);

        checkArgument(restoredNumLevels > 1, "Number of levels must be at least 2.");

        this.level0 = new TreeSet<>(
                        (a, b) -> {
                            if (a.maxSequenceNumber() != b.maxSequenceNumber()) {
                                // 序列号较大的文件应排在前
                                return Long.compare(b.maxSequenceNumber(), a.maxSequenceNumber());
                            } else {
                                // 当两个或多个作业写入同一个合并树时，有可能多个文件具有相同的 maxSequenceNumber。
                                // 在这种情况下，我们必须比较它们的文件名，以便具有相同 maxSequenceNumber 的文件不会被树集“去重”。
                                return a.fileName().compareTo(b.fileName());
                            }
                        });

        this.levels = new ArrayList<>();
        for (int i = 1; i < restoredNumLevels; i++) {
            levels.add(SortedRun.empty());
        }

        // 遍历所有要读取的文件集合， level=0 的文件塞入到 level0 变量中
        //                        level>0 的文件塞入 levels<SortedRun> 中
        Map<Integer, List<DataFileMeta>> levelMap = new HashMap<>();
        for (DataFileMeta file : inputFiles) {
            levelMap.computeIfAbsent(file.level(), level -> new ArrayList<>()).add(file);
        }
        levelMap.forEach((level, files) -> updateLevel(level, emptyList(), files));

        Preconditions.checkState(
                level0.size() + levels.stream().mapToInt(r -> r.files().size()).sum()
                        == inputFiles.size(),
                "Number of files stored in Levels does not equal to the size of inputFiles. This is unexpected.");
    }

    public TreeSet<DataFileMeta> level0() {
        return level0;
    }

    public void addDropFileCallback(DropFileCallback callback) {
        dropFileCallbacks.add(callback);
    }

    public void addLevel0File(DataFileMeta file) {
        checkArgument(file.level() == 0);
        level0.add(file);
    }

    // 获取 level>0 层的 SortedRun
    public SortedRun runOfLevel(int level) {
        checkArgument(level > 0, "Level0 does not have one single sorted run.");
        return levels.get(level - 1);
    }

    // 获取最高的 level 数
    public int numberOfLevels() {
        return levels.size() + 1;
    }

    // 获取最高的 level 数
    public int maxLevel() {
        return levels.size();
    }

    //
    public int numberOfSortedRuns() {
        int numberOfSortedRuns = level0.size();
        for (SortedRun run : levels) {
            if (run.nonEmpty()) {
                numberOfSortedRuns++;
            }
        }
        return numberOfSortedRuns;
    }

    // 获取有效（非空）的最高层
    public int nonEmptyHighestLevel() {
        int i;
        for (i = levels.size() - 1; i >= 0; i--) {
            if (levels.get(i).nonEmpty()) {
                return i + 1;
            }
        }
        return level0.isEmpty() ? -1 : 0;
    }

    public List<DataFileMeta> allFiles() {
        List<DataFileMeta> files = new ArrayList<>();
        List<LevelSortedRun> runs = levelSortedRuns();
        for (LevelSortedRun run : runs) {
            files.addAll(run.run().files());
        }
        return files;
    }

    public List<LevelSortedRun> levelSortedRuns() {
        List<LevelSortedRun> runs = new ArrayList<>();
        // level-0 的文件， 每个文件都是一个 sorted-run
        level0.forEach(file -> runs.add(new LevelSortedRun(0, SortedRun.fromSingle(file))));
        // level > 0 的文件， 每层所有文件是一个 sorted-run
        for (int i = 0; i < levels.size(); i++) {
            SortedRun run = levels.get(i);
            if (run.nonEmpty()) {
                runs.add(new LevelSortedRun(i + 1, run));
            }
        }
        return runs;
    }

    public void update(List<DataFileMeta> before, List<DataFileMeta> after) {
        Map<Integer, List<DataFileMeta>> groupedBefore = groupByLevel(before);
        Map<Integer, List<DataFileMeta>> groupedAfter = groupByLevel(after);
        for (int i = 0; i < numberOfLevels(); i++) {
            updateLevel(
                    i,
                    groupedBefore.getOrDefault(i, emptyList()),
                    groupedAfter.getOrDefault(i, emptyList()));
        }

        if (dropFileCallbacks.size() > 0) {
            Set<String> droppedFiles =
                    before.stream().map(DataFileMeta::fileName).collect(Collectors.toSet());
            // exclude upgrade files
            after.stream().map(DataFileMeta::fileName).forEach(droppedFiles::remove);
            for (DropFileCallback callback : dropFileCallbacks) {
                droppedFiles.forEach(callback::notifyDropFile);
            }
        }
    }

    private void updateLevel(int level, List<DataFileMeta> before, List<DataFileMeta> after) {
        if (before.isEmpty() && after.isEmpty())  return;

        if (level == 0) {
            before.forEach(level0::remove);
            level0.addAll(after);
        } else {
            List<DataFileMeta> files = new ArrayList<>(runOfLevel(level).files());
            files.removeAll(before);
            files.addAll(after);
            levels.set(level - 1, SortedRun.fromUnsorted(files, keyComparator));
        }
    }

    private Map<Integer, List<DataFileMeta>> groupByLevel(List<DataFileMeta> files) {
        return files.stream()
                .collect(Collectors.groupingBy(DataFileMeta::level, Collectors.toList()));
    }

    /** A callback to notify dropping file. */
    public interface DropFileCallback {

        void notifyDropFile(String file);
    }
}
