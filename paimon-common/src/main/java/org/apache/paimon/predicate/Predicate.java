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

package org.apache.paimon.predicate;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;

import java.io.Serializable;
import java.util.Optional;

/**
 * 返回布尔值并通过统计信息提供测试的谓词。
 * 条件判断
 * @see PredicateBuilder
 * @since 0.4.0
 */
@Public
public interface Predicate extends Serializable {

    /**
     * 基于特定输入行进行测试。
     * @return 命中时返回 true，未命中时返回 false。
     **/
    boolean test(InternalRow row);

    /**
     * 基于统计信息测试以确定是否可能命中。
     * @return 返回 true 表示可能命中（也可能有假阳性），返回 false 表示绝对不可能命中。
     */
    boolean test(long rowCount, InternalRow minValues, InternalRow maxValues, InternalArray nullCounts);

    /** @return 如果可能，返回此谓词的否定谓词。 */
    Optional<Predicate> negate();

    <T> T visit(PredicateVisitor<T> visitor);
}
