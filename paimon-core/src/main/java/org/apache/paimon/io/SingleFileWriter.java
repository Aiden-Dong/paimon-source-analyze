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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;

/**
 * 用于生成单个文件的 {@link FileWriter}。
 *
 * @param <T> 要写入的记录类型。
 * @param <R> 写入文件后生成的结果类型。
 */
public abstract class SingleFileWriter<T, R> implements FileWriter<T, R> {

    private static final Logger LOG = LoggerFactory.getLogger(SingleFileWriter.class);

    protected final FileIO fileIO;                        // 写入的文件操作句柄
    protected final Path path;                            // 写文件路径
    private final Function<T, InternalRow> converter;     // ? ??

    private final FormatWriter writer;                   // 用于对接不同的 文件写出类型 ： ORC / PARQUET
    private PositionOutputStream out;                    // write 底层使用的 IO 工具类

    private long recordCount;
    protected boolean closed;

    public SingleFileWriter(
            FileIO fileIO,
            FormatWriterFactory factory,
            Path path,
            Function<T, InternalRow> converter,
            String compression) {
        this.fileIO = fileIO;
        this.path = path;
        this.converter = converter;

        try {
            out = fileIO.newOutputStream(path, false);
            writer = factory.create(out, compression);
        } catch (IOException e) {

            if (out != null) {
                abort();
            }
            throw new UncheckedIOException(e);
        }

        this.recordCount = 0;
        this.closed = false;
    }

    public Path path() {
        return path;
    }

    @Override
    public void write(T record) throws IOException {
        writeImpl(record);
    }

    protected InternalRow writeImpl(T record) throws IOException {
        if (closed) {
            throw new RuntimeException("Writer has already closed!");
        }

        try {
            InternalRow rowData = converter.apply(record);
            writer.addElement(rowData);
            recordCount++;
            return rowData;
        } catch (Throwable e) {

            abort();
            throw e;
        }
    }

    @Override
    public long recordCount() {
        return recordCount;
    }

    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        return writer.reachTargetSize(suggestedCheck, targetSize);
    }

    @Override
    public void abort() {
        IOUtils.closeQuietly(out);
        fileIO.deleteQuietly(path);
    }

    public AbortExecutor abortExecutor() {
        if (!closed) {
            throw new RuntimeException("Writer should be closed!");
        }

        return new AbortExecutor(fileIO, path);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Closing file " + path);
        }

        try {
            writer.flush();
            writer.finish();

            out.flush();
            out.close();
        } catch (IOException e) {
            LOG.warn("Exception occurs when closing file " + path + ". Cleaning up.", e);
            abort();
            throw e;
        } finally {
            closed = true;
        }
    }

    /** Abort executor to just have reference of path instead of whole writer. */
    public static class AbortExecutor {

        private final FileIO fileIO;
        private final Path path;

        private AbortExecutor(FileIO fileIO, Path path) {
            this.fileIO = fileIO;
            this.path = path;
        }

        public void abort() {
            fileIO.deleteQuietly(path);
        }
    }
}
