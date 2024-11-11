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

package org.apache.paimon.io;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.io.SingleFileWriter.AbortExecutor;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * 写文件工具
 * 当当前大小超过目标文件大小时，写入器将滚动到新文件。
 *
 * @param <T> 记录数据类型
 * @param <R> 文件元数据结果
 */
public class RollingFileWriter<T, R> implements FileWriter<T, List<R>> {

    private static final Logger LOG = LoggerFactory.getLogger(RollingFileWriter.class);
    private static final int CHECK_ROLLING_RECORD_CNT = 1000;

    private final Supplier<? extends SingleFileWriter<T, R>> writerFactory;
    private final long targetFileSize;                                          // 单文件的大小上限
    private final List<AbortExecutor> closedWriters;
    private final List<R> results;

    private SingleFileWriter<T, R> currentWriter = null;
    private long recordCount = 0;
    private boolean closed = false;

    public RollingFileWriter(Supplier<? extends SingleFileWriter<T, R>> writerFactory, long targetFileSize) {
        this.writerFactory = writerFactory;
        this.targetFileSize = targetFileSize;
        this.results = new ArrayList<>();
        this.closedWriters = new ArrayList<>();
    }

    @VisibleForTesting
    public long targetFileSize() {
        return targetFileSize;
    }

    // 标识每1000行校验一次 是否达到文件大小上限
    // 如果已经达到文件上限， 则关闭重新开一个写
    @VisibleForTesting
    boolean rollingFile() throws IOException {
        return currentWriter.reachTargetSize(recordCount % CHECK_ROLLING_RECORD_CNT == 0, targetFileSize);
    }

    @Override
    public void write(T row) throws IOException {
        try {

            if (currentWriter == null) openCurrentWriter();   // 检查是否需要打开一个新的写入工具

            currentWriter.write(row);
            recordCount += 1;

            if (rollingFile())  closeCurrentWriter();         // 检查是否需要关闭重新打开一个写入工具

        } catch (Throwable e) {

            abort();
            throw e;
        }
    }

    private void openCurrentWriter() {
        currentWriter = writerFactory.get();
    }

    private void closeCurrentWriter() throws IOException {
        if (currentWriter == null) {
            return;
        }

        currentWriter.close();
        // only store abort executor in memory
        // cannot store whole writer, it includes lots of memory for example column vectors to read
        // and write
        closedWriters.add(currentWriter.abortExecutor());
        results.add(currentWriter.result());
        currentWriter = null;
    }

    @Override
    public long recordCount() {
        return recordCount;
    }

    @Override
    public void abort() {
        if (currentWriter != null) {
            currentWriter.abort();
        }
        for (AbortExecutor abortExecutor : closedWriters) {
            abortExecutor.abort();
        }
    }

    @Override
    public List<R> result() {
        Preconditions.checkState(closed, "Cannot access the results unless close all writers.");
        return results;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        try {
            closeCurrentWriter();
        } catch (IOException e) {
            LOG.warn(
                    "Exception occurs when writing file " + currentWriter.path() + ". Cleaning up.",
                    e);
            abort();
            throw e;
        } finally {
            closed = true;
        }
    }
}
