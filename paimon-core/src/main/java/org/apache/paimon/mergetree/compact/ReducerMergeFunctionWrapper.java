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
 * 作为 reducer 的 {@link MergeFunction} 的包装器。
 *
 * <p>reducer 是一种函数类型。如果只有一个输入，则结果等于该输入；
 * 否则，结果是通过某种方式合并所有输入来计算的。
 *
 * <p>此包装器优化了包装的 {@link MergeFunction}。如果只有一个输入，则将存储输入，并且不会调用内部合并函数，从而节省一些计算时间。
 */
public class ReducerMergeFunctionWrapper implements MergeFunctionWrapper<KeyValue> {

    private final MergeFunction<KeyValue> mergeFunction;

    private KeyValue initialKv;
    private boolean isInitialized;

    public ReducerMergeFunctionWrapper(MergeFunction<KeyValue> mergeFunction) {
        this.mergeFunction = mergeFunction;
    }

    /** Resets the {@link MergeFunction} helper to its default state. */
    @Override
    public void reset() {
        initialKv = null;
        mergeFunction.reset();
        isInitialized = false;
    }

    /** Adds the given {@link KeyValue} to the {@link MergeFunction} helper. */
    @Override
    public void add(KeyValue kv) {
        if (initialKv == null) {
            initialKv = kv;
        } else {
            if (!isInitialized) {
                merge(initialKv);
                isInitialized = true;
            }
            merge(kv);
        }
    }

    private void merge(KeyValue kv) {
        mergeFunction.add(kv);
    }

    /** Get current value of the {@link MergeFunction} helper. */
    @Override
    public KeyValue getResult() {
        return isInitialized ? mergeFunction.getResult() : initialKv;
    }
}
