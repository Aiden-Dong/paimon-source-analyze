package org.apache.paimon.mytest;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : lan                                                                                *
 * @date : 2024/11/13                                                                                *
 * ============================================================================================== */
public class CatalogUtil {
  // 创建 paimon catalog
  public static Catalog getFilesystemCatalog() {

    CatalogContext context = CatalogContext.create(new Path("file:////Users/lan/tmp/paimon-catalog"));
    return CatalogFactory.createCatalog(context);
  }

  // 创建 Hive Catalog
  public Catalog createHiveCatalog() {
    // Paimon Hive catalog relies on Hive jars
    // You should add hive classpath or hive bundled jar.
    Options options = new Options();
    options.set("warehouse", "...");
    options.set("metastore", "hive");
    options.set("uri", "...");
    options.set("hive-conf-dir", "...");
    options.set("hadoop-conf-dir", "...");
    CatalogContext context = CatalogContext.create(options);
    return CatalogFactory.createCatalog(context);
  }
}