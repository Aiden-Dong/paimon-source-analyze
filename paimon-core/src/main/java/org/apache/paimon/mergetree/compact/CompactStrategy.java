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

import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.mergetree.LevelSortedRun;

import java.util.List;
import java.util.Optional;

/**
 * 决定选择哪些文件进行压缩的压缩策略。
 * */
public interface CompactStrategy {

    /**
     * 从运行中选择压缩单元。
     *
     * <ul>
     *   <li>压缩基于运行，而不是基于文件。</li>
     *   <li>级别 0 是特殊的，每个文件一个运行；所有其他级别每个级别一个运行。</li>
     *   <li>压缩从小级别到大级别是顺序的。</li>
     * </ul>
     */
    Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs);

    /***
     * 将当前的所有文件加入压缩
     * @param numLevels   最高的 level 层
     * @param runs        所有待排序的 SortedRun
     */
    static Optional<CompactUnit> pickFullCompaction(int numLevels, List<LevelSortedRun> runs) {
        int maxLevel = numLevels - 1;
        if (runs.isEmpty() || (runs.size() == 1 && runs.get(0).level() == maxLevel)) {
            // no sorted run or only 1 sorted run on the max level, no need to compact
            return Optional.empty();
        } else {
            return Optional.of(CompactUnit.fromLevelRuns(maxLevel, runs));
        }
    }
}
