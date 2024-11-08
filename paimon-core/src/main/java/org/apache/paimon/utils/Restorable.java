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

import org.apache.paimon.annotation.Public;

/**
 * 实现此接口的操作可以在不同实例之间检查点和恢复其状态。
 *
 * @param <S> 状态类型
 * @since 0.4.0
 */
@Public
public interface Restorable<S> {

    // 提取当前操作实例的状态。
    S checkpoint();

    // 将先前操作实例的状态恢复到当前操作实例。
    void restore(S state);
}
