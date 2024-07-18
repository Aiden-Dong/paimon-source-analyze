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

package org.apache.paimon.table;

/**
 * 表的桶模式，它影响写入过程，也影响读取时的数据跳过。
 */
public enum BucketMode {

    /**
     * 用户配置的固定桶数量只能通过离线命令修改。数据根据桶键（默认为主键）分配到相应的桶中，
     * 读取端可以根据桶键的过滤条件进行数据跳过。
     */
    FIXED,

    /**
     * 动态桶模式通过索引文件记录键对应的桶。此模式不支持多重并发写入或读取过滤条件的数据跳过。
     * 此模式仅适用于变更日志表。
     */
    DYNAMIC,

    /**
     * 与 DYNAMIC 模式相比，此模式不仅为分区表动态分配桶，还在分区之间更新数据。
     * 主键不包含分区字段。
     */
    GLOBAL_DYNAMIC,

    /**
     * 忽略桶可以理解为所有数据进入全局桶，并随机写入表中。桶中的数据完全没有顺序关系。
     * 此模式仅适用于仅追加表。
     */
    UNAWARE
}
