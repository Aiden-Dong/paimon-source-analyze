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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;

/**
 * 合并函数，用于合并多个 {@link KeyValue}。
 *
 * <p>重要提示，关于 {@link #add} 输入中的 kv 对象复用：
 *
 * <ul>
 *   <li>请不要将 KeyValue 和 InternalRow 的引用保存在 List 中：前两个对象中的 KeyValue 和
 *       其中的 InternalRow 对象是安全的，但第三个对象的引用可能会覆盖第一个对象的引用。
 *   <li>你可以保存字段的引用：字段不会复用它们的对象。
 * </ul>
 *
 * @param <T> 结果类型
 */
public interface MergeFunction<T> {

    /** Reset the merge function to its default state. */
    void reset();

    /** Add the given {@link KeyValue} to the merge function. */
    void add(KeyValue kv);

    /** Get current merged value. */
    T getResult();
}
