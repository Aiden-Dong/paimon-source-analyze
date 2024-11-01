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

package org.apache.paimon.data.serializer;

import org.apache.paimon.data.AbstractPagedInputView;
import org.apache.paimon.data.AbstractPagedOutputView;
import org.apache.paimon.data.BinaryRow;

import java.io.IOException;

/** A type serializer which provides paged serialize and deserialize methods. */
public interface PagedTypeSerializer<T> extends Serializer<T> {

    /**
     * 将给定记录序列化到指定的目标分页输出视图。
     * 一些实现可能会跳过某些字节，如果当前页面没有足够的剩余空间，例如 {@link BinaryRow}。
     *
     * @param record 要序列化的记录。
     * @param target 要写入序列化数据的输出视图。
     * @return 返回跳过的字节数。
     * @throws IOException 如果序列化过程中遇到 I/O 相关错误，将抛出此异常。通常由输出视图引发，输出视图可能具有底层 I/O 通道并将其委托。
     */
    int serializeToPages(T record, AbstractPagedOutputView target) throws IOException;

    /**
     * De-serializes a record from the given source paged input view. For consistency with serialize
     * format, some implementations may need to skip some bytes of source before de-serializing,
     * .e.g {@link BinaryRow}. Typically, the content read from source should be copied out when
     * de-serializing, and we are not expecting the underlying data from source is reused. If you
     * have such requirement, see {@link #mapFromPages(T, AbstractPagedInputView)}.
     *
     * @param source The input view from which to read the data.
     * @return The de-serialized element.
     * @throws IOException Thrown, if the de-serialization encountered an I/O related error.
     *     Typically raised by the input view, which may have an underlying I/O channel from which
     *     it reads.
     */
    T deserializeFromPages(AbstractPagedInputView source) throws IOException;

    /** Reuse version of {@link #deserializeFromPages(AbstractPagedInputView)}. */
    T deserializeFromPages(T reuse, AbstractPagedInputView source) throws IOException;

    /**
     * Map a reused record from the given source paged input view. This method provides a
     * possibility to achieve zero copy when de-serializing. You can either choose copy or not copy
     * the content read from source, but we encourage to make it zero copy.
     *
     * <p>If you choose the zero copy way, you have to deal with the lifecycle of the pages
     * properly.
     *
     * @param reuse the reused record to be mapped
     * @param source The input view from which to read the data.
     * @return The mapped record.
     * @throws IOException Thrown, if the de-serialization encountered an I/O related error.
     *     Typically raised by the input view, which may have an underlying I/O channel from which
     *     it reads.
     */
    T mapFromPages(T reuse, AbstractPagedInputView source) throws IOException;

    /** Skip over bytes of one record from the paged input view, discarding the skipped bytes. */
    void skipRecordFromPages(AbstractPagedInputView source) throws IOException;
}
