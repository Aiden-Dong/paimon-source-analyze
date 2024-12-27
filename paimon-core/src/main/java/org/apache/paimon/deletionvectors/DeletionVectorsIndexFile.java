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
import org.apache.paimon.index.IndexFile;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PathFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * DeletionVectors index file.
 * Deletion-Vectors 的索引文件只包含涉及到删除操作的每个文件的位图信息，
 * 改文件的元信息 { <file, (startPos, limit)> } 保存在 manifest 中
 * */
public class DeletionVectorsIndexFile extends IndexFile {

    public static final String DELETION_VECTORS_INDEX = "DELETION_VECTORS";
    public static final byte VERSION_ID_V1 = 1;

    public DeletionVectorsIndexFile(FileIO fileIO, PathFactory pathFactory) {
        super(fileIO, pathFactory);
    }

    /**
     * 从指定文件中读取所有删除向量。
     *
     * @param fileName 要从中读取删除向量的文件名。
     * @param deletionVectorRanges 一个映射，其中键表示 DeletionVector 所属的文件，值是一个 Pair 对象，指定删除向量数据在文件中的范围（起始位置和大小）。
     * @return 一个映射，其中键表示 DeletionVector 所属的文件，值是相应的 DeletionVector 对象。
     * @throws UncheckedIOException 如果在从文件读取时发生 I/O 错误。
     */
    public Map<String, DeletionVector> readAllDeletionVectors(
            String fileName, LinkedHashMap<String, Pair<Integer, Integer>> deletionVectorRanges) {

        Map<String, DeletionVector> deletionVectors = new HashMap<>();
        Path filePath = pathFactory.toPath(fileName);

        try (SeekableInputStream inputStream = fileIO.newInputStream(filePath)) {
            checkVersion(inputStream);
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            for (Map.Entry<String, Pair<Integer, Integer>> entry :
                    deletionVectorRanges.entrySet()) {
                deletionVectors.put(
                        entry.getKey(),
                        readDeletionVector(dataInputStream, entry.getValue().getRight()));
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to read deletion vectors from file: "
                            + filePath + ", deletionVectorRanges: " + deletionVectorRanges,
                    e);
        }
        return deletionVectors;
    }

    /**
     * Reads a single deletion vector from the specified file.
     *
     * @param fileName The name of the file from which to read the deletion vector.
     * @param deletionVectorRange A Pair specifying the range (start position and size) within the
     *     file where the deletion vector data is located.
     * @return The DeletionVector object read from the specified range in the file.
     * @throws UncheckedIOException If an I/O error occurs while reading from the file.
     */
    public DeletionVector readDeletionVector(
            String fileName, Pair<Integer, Integer> deletionVectorRange) {
        Path filePath = pathFactory.toPath(fileName);
        try (SeekableInputStream inputStream = fileIO.newInputStream(filePath)) {
            checkVersion(inputStream);
            inputStream.seek(deletionVectorRange.getLeft());
            DataInputStream dataInputStream = new DataInputStream(inputStream);
            return readDeletionVector(dataInputStream, deletionVectorRange.getRight());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to read deletion vector from file: "
                            + filePath
                            + ", deletionVectorRange: "
                            + deletionVectorRange,
                    e);
        }
    }

    /**
     * 将删除向量写入新文件，此文件的格式可以在以下链接中找到参考：<a href="https://cwiki.apache.org/confluence/x/Tws4EQ">PIP-16</a>。
     *
     * @param input 一个映射，其中键表示 DeletionVector 所属的文件，值是对应的 DeletionVector 对象。
     * @return 一个 Pair 对象，指定写入的新文件的名称以及一个映射，其中键表示 DeletionVector 所属的文件，值是一个 Pair 对象，
     *     指定文件中的范围（起始位置和大小），删除向量数据位于该范围内。
     * @throws UncheckedIOException 如果写入文件时发生 I/O 错误。
     */
    public Pair<String, LinkedHashMap<String, Pair<Integer, Integer>>> write(Map<String, DeletionVector> input) {

        int size = input.size();

        // <file, (startPos, limit)>
        LinkedHashMap<String, Pair<Integer, Integer>> deletionVectorRanges = new LinkedHashMap<>(size);
        Path path = pathFactory.newPath();

        try (DataOutputStream dataOutputStream = new DataOutputStream(fileIO.newOutputStream(path, true))) {

            dataOutputStream.writeByte(VERSION_ID_V1);  // 写入版本信息

            for (Map.Entry<String, DeletionVector> entry : input.entrySet()) {
                String key = entry.getKey();                             // 拿到删除向量对应的文件
                byte[] valueBytes = entry.getValue().serializeToBytes();  // 拿到删除向量对应的具体位图编码信息
                deletionVectorRanges.put(key, Pair.of(dataOutputStream.size(), valueBytes.length));
                dataOutputStream.writeInt(valueBytes.length);
                dataOutputStream.write(valueBytes);
                dataOutputStream.writeInt(calculateChecksum(valueBytes));
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Unable to write deletion vectors to file: " + path.getName(), e);
        }
        return Pair.of(path.getName(), deletionVectorRanges);
    }

    private void checkVersion(InputStream in) throws IOException {
        int version = in.read();
        if (version != VERSION_ID_V1) {
            throw new RuntimeException(
                    "Version not match, actual version: "
                            + version + ", expert version: " + VERSION_ID_V1);
        }
    }

    private DeletionVector readDeletionVector(DataInputStream inputStream, int size) {
        try {
            // check size
            int actualSize = inputStream.readInt();
            if (actualSize != size) {
                throw new RuntimeException(
                        "Size not match, actual size: " + actualSize + ", expert size: " + size);
            }

            // read DeletionVector bytes
            byte[] bytes = new byte[size];
            inputStream.readFully(bytes);

            // check checksum
            int checkSum = calculateChecksum(bytes);
            int actualCheckSum = inputStream.readInt();
            if (actualCheckSum != checkSum) {
                throw new RuntimeException(
                        "Checksum not match, actual checksum: "
                                + actualCheckSum
                                + ", expected checksum: "
                                + checkSum);
            }
            return DeletionVector.deserializeFromBytes(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read deletion vector", e);
        }
    }

    private int calculateChecksum(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }
}
