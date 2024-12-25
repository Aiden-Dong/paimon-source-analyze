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

package org.apache.paimon.compact;

import org.apache.paimon.io.DataFileMeta;

import java.io.Closeable;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * Manager to submit compaction task.
 * 提交压缩任务的管理者
 * */
public interface CompactManager extends Closeable {

    /** 应该等待 compaction 结束 */
    boolean shouldWaitForLatestCompaction();

    boolean shouldWaitForPreparingCheckpoint();

    /** 将新的文件添加到压缩任务中. */
    void addNewFile(DataFileMeta file);

    Collection<DataFileMeta> allFiles();

    /**
     * 触发一个新的压缩任务执行.
     *
     * @param fullCompaction if caller needs a guaranteed full compaction
     */
    void triggerCompaction(boolean fullCompaction);

    /**
     * 获取压缩结果， 如果 {@code blocking} 设置为 true 则阻塞等待压缩任务结束
     * Get compaction result. Wait finish if {@code blocking} is true.
     * */
    Optional<CompactResult> getCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException;

    /** 取消当前的压缩任务执行 */
    void cancelCompaction();

    /** 检查压缩任务是否还在进行中，如果已经运行完嗯获取结果. */
    boolean isCompacting();
}
