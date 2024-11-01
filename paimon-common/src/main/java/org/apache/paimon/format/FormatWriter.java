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

package org.apache.paimon.format;

import org.apache.paimon.data.InternalRow;

import java.io.IOException;

/***
 * 写入记录的写入器.
 **/
public interface FormatWriter {

    /**
     * 向编码器添加一个元素。编码器可能会暂时缓冲该元素，或立即将其写入流。
     *
     * <p>添加此元素可能会填满内部缓冲区，并导致一批内部缓冲的元素被编码和刷新。
     *
     * @param element 要添加的元素。
     * @throws IOException 如果无法将元素添加到编码器，或输出流抛出异常，则抛出此异常。
     */
    void addElement(InternalRow element) throws IOException;

    /**
     * 将所有中间缓冲的数据刷新到输出流。频繁刷新可能会降低编码效率。
     *
     * @throws IOException 如果编码器无法刷新或输出流抛出异常，则抛出此异常。
     */
    void flush() throws IOException;

    /**
     * 完成写入操作。此方法必须刷新所有内部缓冲区、完成编码并写入文件尾。
     *
     * <p>调用此方法后，写入器不应再处理任何通过 {@link #addElement(InternalRow)} 添加的记录。
     *
     * <p><b>重要：</b>此方法绝不能关闭写入器写入的流。流的关闭应由调用此方法的程序完成。
     *
     * @throws IOException 如果最终化操作失败，则抛出此异常。
     */
    void finish() throws IOException;

    /**
     * 检查写入器是否已达到 <code>targetSize</code>。
     *
     * @param suggestedCheck 是否需要检查，但子类也可以自行决定是否检查。
     * @param targetSize 目标大小。
     * @return 如果达到目标大小则返回 true，否则返回 false。
     * @throws IOException 如果计算长度失败，则抛出此异常。
     */
    boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException;
}
