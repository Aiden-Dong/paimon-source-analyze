package org.apache.paimon.mytest;

import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : lan                                                                                *
 * @date : 2025/7/11                                                                                *
 * ============================================================================================== */
public class MySnapshotReader {
  public static void main(String[] args) throws IOException {
    FileStoreTable table = (FileStoreTable) TableUtil.getTable();   // PrimaryKeyFileStoreTable

    RowType rowType = table.rowType();

    TableSchema schema = table.schema();

    schema.fields().stream()
            .map(field -> field.id())
            .sorted()
            .forEach(field -> System.out.println(field));


    List<String> strings = table.partitionKeys();

    System.out.println(strings);
//    
//
//
//
//    System.out.println(schema.toString());

//    SnapshotReader snapshotReader = table.newSnapshotReader();
//    SnapshotManager snapshotManager = snapshotReader.snapshotManager();
//
//    Snapshot snapshot = snapshotManager.latestSnapshot();
//
//    System.out.println("===================Snapshot========================");
//
//    System.out.println(snapshot.toJson());
//
//    System.out.println("==================================================");
//
//    FileStore<?> store = table.store();
//    ManifestList manifestList = store.manifestListFactory().create();
//
//
//    System.out.println("===================Base-ManifestFile========================");
//    List<ManifestFileMeta> baseManifestFileMetaList = manifestList.read(snapshot.baseManifestList());
//    for (ManifestFileMeta manifestFileMeta : baseManifestFileMetaList) {
//      System.out.println(manifestFileMeta.toString());
//    }
//    System.out.println("============================================================");
//
//    System.out.println("===================Delta-ManifestFile========================");
//    List<ManifestFileMeta> deltaManifestList = manifestList.read(snapshot.deltaManifestList());
//    for (ManifestFileMeta manifestFileMeta : deltaManifestList) {
//      System.out.println(manifestFileMeta.toString());
//    }
//    System.out.println("============================================================");
//
//
//    List<ManifestFileMeta> manifestFileMetaList = new ArrayList<>();
//    manifestFileMetaList.addAll(baseManifestFileMetaList);
//    manifestFileMetaList.addAll(deltaManifestList);
//
//    System.out.println("===================ManifestEntry========================");
//    ManifestFile manifestFile = store.manifestFileFactory().create();
//
//    for (ManifestFileMeta manifestFileMeta : manifestFileMetaList) {
//      List<ManifestEntry> manifestEntries = manifestFile.read(manifestFileMeta.fileName());
//      for (ManifestEntry manifestEntry : manifestEntries) {
//        System.out.println(manifestEntry);
//      }
//    }
//    System.out.println("============================================================");
//
//


  }
}
