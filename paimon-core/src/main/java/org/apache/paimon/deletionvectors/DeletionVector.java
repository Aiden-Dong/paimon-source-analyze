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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DeletionFile;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * DeletionVector 可以高效地记录文件中已删除行的位置，然后在处理文件时用于过滤掉已删除的行。
 */
public interface DeletionVector {

    /**
     * 将指定位置标记为删除.
     *
     * @param position 要标记为已删除的行的位置。.
     */
    void delete(long position);

    /**
     * 将指定位置的行标记为已删除。
     *
     * @param position 要标记为已删除的行的位置。
     * @return 如果添加的位置之前没有被删除，则返回 true。否则返回 false。
     */
    default boolean checkedDelete(long position) {
        if (isDeleted(position)) {
            return false;
        } else {
            delete(position);
            return true;
        }
    }

    /**
     * 检查指定位置的行是否标记为已删除。
     *
     * @param position 要检查的行的位置。
     * @return 如果行被标记为已删除，则返回 true，否则返回 false。
     */
    boolean isDeleted(long position);

    /**
     * 确定删除向量是否为空，表示没有删除。
     *
     * @return 如果删除向量为空，则返回 true，否则返回 false。
     */
    boolean isEmpty();

    /**
     * 将删除向量序列化为字节数组以进行存储或传输。
     *
     * @return 表示序列化删除向量的字节数组。
     */
    byte[] serializeToBytes();

    /**
     * 从字节数组中反序列化删除向量。
     *
     * @param bytes 包含序列化删除向量的字节数组。
     * @return 表示反序列化数据的 DeletionVector 实例。
     */
    static DeletionVector deserializeFromBytes(byte[] bytes) {

        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                DataInputStream dis = new DataInputStream(bis)) {

            int magicNum = dis.readInt();

            if (magicNum == BitmapDeletionVector.MAGIC_NUMBER) {
                return BitmapDeletionVector.deserializeFromDataInput(dis);
            } else {
                throw new RuntimeException("Invalid magic number: " + magicNum);
            }

        } catch (IOException e) {
            throw new RuntimeException("Unable to deserialize deletion vector", e);
        }
    }

    // 解析得到一个删除向量
    static DeletionVector read(FileIO fileIO, DeletionFile deletionFile) throws IOException {
        Path path = new Path(deletionFile.path());
        try (SeekableInputStream input = fileIO.newInputStream(path)) {

            input.seek(deletionFile.offset());                 // deletion index 的索引起始位置

            DataInputStream dis = new DataInputStream(input);
            int actualLength = dis.readInt();

            if (actualLength != deletionFile.length()) {
                throw new RuntimeException(
                        "Size not match, actual size: " + actualLength
                                + ", expert size: " + deletionFile.length() + ", file path: " + path);
            }
            int magicNum = dis.readInt();
            if (magicNum == BitmapDeletionVector.MAGIC_NUMBER) {
                return BitmapDeletionVector.deserializeFromDataInput(dis);
            } else {
                throw new RuntimeException("Invalid magic number: " + magicNum);
            }
        }
    }

    static Factory emptyFactory() {
        return fileName -> Optional.empty();
    }

    static Factory factory(@Nullable DeletionVectorsMaintainer dvMaintainer) {
        if (dvMaintainer == null) {
            return emptyFactory();
        }
        return dvMaintainer::deletionVectorOf;
    }

    static Factory factory(FileIO fileIO,
                           List<DataFileMeta> files,
                           @Nullable List<DeletionFile> deletionFiles) {

        if (deletionFiles == null) {
            return emptyFactory();
        }

        Map<String, DeletionFile> fileToDeletion = new HashMap<>();
        for (int i = 0; i < files.size(); i++) {
            DeletionFile deletionFile = deletionFiles.get(i);
            if (deletionFile != null) {
                fileToDeletion.put(files.get(i).fileName(), deletionFile);
            }
        }
        return fileName -> {
            DeletionFile deletionFile = fileToDeletion.get(fileName);
            if (deletionFile == null) {
                return Optional.empty();
            }

            return Optional.of(DeletionVector.read(fileIO, deletionFile));
        };
    }

    /** Interface to create {@link DeletionVector}. */
    interface Factory {
        Optional<DeletionVector> create(String fileName) throws IOException;
    }
}
