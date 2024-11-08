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

package org.apache.paimon.utils;

import org.apache.paimon.io.DataFileMeta;

import java.util.Collection;
import java.util.List;

/**
 * {@code RecordWriter} 负责写入数据并处理用于写入尚未暂存的数据的进行中的文件。
 *  准备提交的增量文件由 {@link #prepareCommit(boolean)} 返回给系统。
 *
 * @param <T> 要写入的记录类型。
 */
public interface RecordWriter<T> {

    // 向写入器添加一个键值元素。
    void write(T record) throws Exception;

    /**
     * 压缩与写入器相关的文件。请注意，压缩过程仅提交，在方法返回时可能尚未完成。
     *
     * @param fullCompaction 是否触发完全压缩或仅正常压缩
     */
    void compact(boolean fullCompaction) throws Exception;

    /**
     * 将文件添加到内部的 {@link org.apache.paimon.compact.CompactManager}。
     *
     * @param files 要添加的文件
     */
    void addNewFiles(List<DataFileMeta> files);

    // 获取此写入器维护的所有数据文件。
    Collection<DataFileMeta> dataFiles();

    /**
     * 获取此写入器写入的记录的最大序列号。
     **/
    long maxSequenceNumber();

    /**
     * 准备提交。
     *
     * @param waitCompaction 是否需要等待当前压缩完成
     * @return 此快照周期中的增量文件
     */
    CommitIncrement prepareCommit(boolean waitCompaction) throws Exception;

    /**
     * 检查是否正在进行压缩，或者是否仍有压缩结果需要获取。
     **/
    boolean isCompacting();

    /**
     * 同步写入器。与文件读取和写入相关的结构是线程不安全的，写入器内部有异步线程，在读取数据之前应同步这些线程。
     */
    void sync() throws Exception;

    /**
     * 关闭此写入器，调用将删除新生成但未提交的文件。
     **/
    void close() throws Exception;
}
