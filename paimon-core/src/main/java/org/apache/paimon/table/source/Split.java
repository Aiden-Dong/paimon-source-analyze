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

package org.apache.paimon.table.source;

import org.apache.paimon.annotation.Public;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * 用于读取的输入拆分。
 */
@Public
public interface Split extends Serializable {

    long rowCount();

    /**
     * 如果该拆分中的所有文件都可以在不进行合并的情况下读取，则返回一个 {@link Optional} 包装的 {@link RawFile} 列表。
     * 如果不能，则返回 {@link Optional#empty()}。
     */
    default Optional<List<RawFile>> convertToRawFiles() {
        return Optional.empty();
    }

    /**
     * 返回数据文件的删除文件，指示数据文件中的哪一行被删除。
     *
     * <p>如果没有对应的删除文件，该元素将为 null。
     */
    default Optional<List<DeletionFile>> deletionFiles() {
        return Optional.empty();
    }

    /**
     * 返回数据文件的索引文件，例如布隆过滤器索引。所有类型的索引和列将存储在一个单独的索引文件中。
     *
     * <p>如果没有对应的索引文件，该元素将为 null。
     */
    default Optional<List<IndexFile>> indexFiles() {
        return Optional.empty();
    }
}
