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

package org.apache.paimon.sort;

// 可索引的可排序类型，用于提供比较和交换功能。
public interface IndexedSortable {

    /**
     * 比较给定地址处的项目，符合
     * {@link java.util.Comparator#compare(Object, Object)} 的语义。
     */
    int compare(int i, int j);

    /**
     * 比较给定地址处的记录，符合 {@link java.util.Comparator#compare(Object, Object)} 的语义。
     *
     * @param segmentNumberI 包含第一个记录的内存段索引
     * @param segmentOffsetI 包含第一个记录的内存段偏移量
     * @param segmentNumberJ 包含第二个记录的内存段索引
     * @param segmentOffsetJ 包含第二个记录的内存段偏移量
     * @return 一个负整数、零或正整数，表示第一个参数小于、等于或大于第二个参数。
     */
    int compare(int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ);

    // 在给定地址处交换项目。
    void swap(int i, int j);

    /**
     * 交换给定地址处的记录。
     *
     * @param segmentNumberI 包含第一个记录的内存段索引
     * @param segmentOffsetI 包含第一个记录的内存段偏移量
     * @param segmentNumberJ 包含第二个记录的内存段索引
     * @param segmentOffsetJ 包含第二个记录的内存段偏移量
     */
    void swap(int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ);

    // 获取可排序元素的数量。
    int size();

    /**
     * 获取每条记录的大小，即连续记录头之间的字节数。
     * @return The record size
     */
    int recordSize();

    /**
     * 获取每个内存段中的元素数量。
     *
     * @return The number of records per segment
     */
    int recordsPerSegment();
}
