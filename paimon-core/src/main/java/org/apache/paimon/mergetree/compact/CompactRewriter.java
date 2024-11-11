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
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;

import java.io.Closeable;
import java.util.List;

/**
 * 重写分区到新级别。
 **/
public interface CompactRewriter extends Closeable {

    /**
     * 重写分区到新级别。
     *
     * @param outputLevel 新级别
     * @param dropDelete 是否删除删除，请参阅 {@link MergeTreeCompactManager#triggerCompaction}
     * @param sections 待压缩的文件集合
     * @return 压缩结果
     * @throws Exception 异常
     */
    CompactResult rewrite(int outputLevel, boolean dropDelete, List<List<SortedRun>> sections)
            throws Exception;

    /**
     * 将文件升级到新级别，通常文件数据不会被重写，只有元数据会被更新。
     * 但在某些特定情况下，我们也必须重写文件，例如 {@link ChangelogMergeTreeRewriter}。
     *
     * @param outputLevel 新级别
     * @param file 要更新的文件
     * @return 压缩结果
     * @throws Exception 异常
     */
    CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception;
}
