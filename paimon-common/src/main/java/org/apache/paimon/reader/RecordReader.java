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

package org.apache.paimon.reader;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.ConsumerWithIOException;
import org.apache.paimon.utils.Filter;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 读取记录批次的读取器。
 * @since 0.4.0
 */

@Public
public interface RecordReader<T> extends Closeable {

    /**
     * 读取一批记录。当达到输入的末尾时，该方法应返回 null。
     * <p>返回的迭代器对象及其包含的对象可能会被源持有一段时间，因此不应立即被读取器重新使用。
     */
    @Nullable
    RecordIterator<T> readBatch() throws IOException;

    // 关闭读取器，并应释放所有资源。
    @Override
    void close() throws IOException;

    /**
     * 一个内部迭代器接口，提供比 {@link Iterator} 更严格的 API。
     */
    interface RecordIterator<T> {

        /**
         * 获取迭代器中的下一个记录。如果该迭代器没有更多元素，则返回 null。
         */
        @Nullable
        T next() throws IOException;

        /**
         * 释放该迭代器所迭代的批次。此方法不应关闭读取器及其资源，而只是一个信号，表示该迭代器不再使用。
         * 此方法可用作钩子，回收/重用重量级对象结构。
         */
        void releaseBatch();

        /**
         * 返回一个迭代器，对每个元素应用 {@code function}。
         **/
        default <R> RecordReader.RecordIterator<R> transform(Function<T, R> function) {
            RecordReader.RecordIterator<T> thisIterator = this;
            return new RecordReader.RecordIterator<R>() {
                @Nullable
                @Override
                public R next() throws IOException {
                    T next = thisIterator.next();
                    if (next == null) {
                        return null;
                    }
                    return function.apply(next);
                }

                @Override
                public void releaseBatch() {
                    thisIterator.releaseBatch();
                }
            };
        }

        /**
         * 过滤一个 {@link RecordIterator}。
         **/
        default RecordIterator<T> filter(Filter<T> filter) {
            RecordIterator<T> thisIterator = this;
            return new RecordIterator<T>() {
                @Nullable
                @Override
                public T next() throws IOException {
                    while (true) {
                        T next = thisIterator.next();
                        if (next == null) {
                            return null;
                        }
                        if (filter.test(next)) {
                            return next;
                        }
                    }
                }

                @Override
                public void releaseBatch() {
                    thisIterator.releaseBatch();
                }
            };
        }
    }

    // -------------------------------------------------------------------------
    //                     Util methods
    // -------------------------------------------------------------------------

    /**
     * 对 {@link RecordReader} 中的每个剩余元素执行给定的操作，直到所有元素都被处理完毕或操作抛出异常。
     */
    default void forEachRemaining(Consumer<? super T> action) throws IOException {
        RecordReader.RecordIterator<T> batch;
        T record;

        try {
            while ((batch = readBatch()) != null) {
                while ((record = batch.next()) != null) {
                    action.accept(record);
                }
                batch.releaseBatch();
            }
        } finally {
            close();
        }
    }

    /**
     * 对 {@link RecordReader} 中每个剩余元素及其行位置执行给定的操作，直到所有元素都被处理完毕或操作抛出异常。
     */
    default void forEachRemainingWithPosition(BiConsumer<Long, ? super T> action)
            throws IOException {
        FileRecordIterator<T> batch;
        T record;

        try {
            while ((batch = (FileRecordIterator<T>) readBatch()) != null) {
                while ((record = batch.next()) != null) {
                    action.accept(batch.returnedPosition(), record);
                }
                batch.releaseBatch();
            }
        } finally {
            close();
        }
    }

    /**
     * 对 {@link RecordReader} 中的每个剩余元素执行给定的操作，直到所有元素都被处理完毕或操作抛出异常。
     */
    default void forIOEachRemaining(ConsumerWithIOException<? super T> action) throws IOException {
        RecordReader.RecordIterator<T> batch;
        T record;

        try {
            while ((batch = readBatch()) != null) {
                while ((record = batch.next()) != null) {
                    action.accept(record);
                }
                batch.releaseBatch();
            }
        } finally {
            close();
        }
    }

    /**
     * 返回一个 {@link RecordReader}，该读取器对每个元素应用 {@code function}。
     **/
    default <R> RecordReader<R> transform(Function<T, R> function) {
        RecordReader<T> thisReader = this;
        return new RecordReader<R>() {
            @Nullable
            @Override
            public RecordIterator<R> readBatch() throws IOException {
                RecordIterator<T> iterator = thisReader.readBatch();
                if (iterator == null) {
                    return null;
                }
                return iterator.transform(function);
            }

            @Override
            public void close() throws IOException {
                thisReader.close();
            }
        };
    }

    /** 过滤操作 {@link RecordReader}. */
    default RecordReader<T> filter(Filter<T> filter) {
        RecordReader<T> thisReader = this;
        return new RecordReader<T>() {
            @Nullable
            @Override
            public RecordIterator<T> readBatch() throws IOException {
                RecordIterator<T> iterator = thisReader.readBatch();
                if (iterator == null) {
                    return null;
                }
                return iterator.filter(filter);
            }

            @Override
            public void close() throws IOException {
                thisReader.close();
            }
        };
    }

    /** Convert this reader to a {@link CloseableIterator}. */
    default CloseableIterator<T> toCloseableIterator() {
        return new RecordReaderIterator<>(this);
    }
}
