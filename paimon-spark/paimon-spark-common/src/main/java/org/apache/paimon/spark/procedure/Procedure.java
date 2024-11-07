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

package org.apache.paimon.spark.procedure;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

/**
 * 定义一个可执行存储过程的接口。
 **/
public interface Procedure {

    // 返回存储过程的输入参数。
    ProcedureParameter[] parameters();

    // 返回存储过程生成的行的类型。
    StructType outputType();

    /**
     * 执行给定的存储过程。
     *
     * @param args 输入参数。
     * @return 使用给定参数执行存储过程的结果。
     */
    InternalRow[] call(InternalRow args);

    // 返回存储过程的描述
    default String description() {
        return this.getClass().toString();
    }
}
