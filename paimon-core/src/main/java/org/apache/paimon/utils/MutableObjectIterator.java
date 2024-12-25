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

import java.io.IOException;

/**
 * 一个简单的迭代器接口。与 {@link java.util.Iterator} 的主要区别是：
 *
 * <ul>
 *   <li>它有两个不同的 `next()` 方法，其中一个变体允许传递一个对象，如果类型是可变的，则可以重用该对象。</li>
 *   <li>它将逻辑合并到单个 `next()` 函数中，而不是将其拆分为两个不同的函数，如 `hasNext()` 和 `next()`。</li>
 * </ul>
 *
 * @param <E> 迭代的集合的元素类型。
 */
public interface MutableObjectIterator<E> {

    /**
     * Gets the next element from the collection. The contents of that next element is put into the
     * given reuse object, if the type is mutable.
     *
     * @param reuse The target object into which to place next element if E is mutable.
     * @return The filled object or <code>null</code> if the iterator is exhausted.
     * @throws IOException Thrown, if a problem occurred in the underlying I/O layer or in the
     *     serialization / deserialization logic
     */
    E next(E reuse) throws IOException;

    /**
     * Gets the next element from the collection. The iterator implementation must obtain a new
     * instance.
     *
     * @return The object or <code>null</code> if the iterator is exhausted.
     * @throws IOException Thrown, if a problem occurred in the underlying I/O layer or in the
     *     serialization / deserialization logic
     */
    E next() throws IOException;
}
