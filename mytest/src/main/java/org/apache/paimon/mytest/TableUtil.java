package org.apache.paimon.mytest;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;

import java.io.IOException;

/**************************************************************************************************
 * <pre>                                                                                          *
 *  .....                                                                                         *
 * </pre>                                                                                         *
 *                                                                                                *
 * @auth : lan                                                                                *
 * @date : 2024/11/13                                                                                *
 * ============================================================================================== */
public class TableUtil {
  public static void createTable() throws IOException {
    Schema.Builder schemaBuilder = Schema.newBuilder();
    schemaBuilder.column("f0", DataTypes.BIGINT());
    schemaBuilder.column("f1", DataTypes.STRING());
    schemaBuilder.column("f2", DataTypes.STRING());
    schemaBuilder.column("f4", DataTypes.FLOAT());
    schemaBuilder.column("f5", DataTypes.DOUBLE());
    schemaBuilder.column("f6", DataTypes.BOOLEAN());
    schemaBuilder.column("f7", DataTypes.ARRAY(DataTypes.BIGINT()));


    schemaBuilder.primaryKey("f0");
    schemaBuilder.option("bucket", "1");
    schemaBuilder.option("file.format", "parquet");
    Schema schema = schemaBuilder.build();

    String dbName = "my_db";

    Identifier identifier = Identifier.create("my_db", "my_table");

    try {
      Catalog catalog = CatalogUtil.getFilesystemCatalog();

      catalog.createDatabase(dbName, true);

      catalog.createTable(identifier, schema, true);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static Table getTable() throws IOException {
    try {
      Identifier identifier = Identifier.create("my_db", "my_table");
      Catalog catalog = CatalogUtil.getFilesystemCatalog();
      return catalog.getTable(identifier);
    } catch (Catalog.TableNotExistException e) {
      throw new IOException(e);
    }
  }

  public static void main(String[] args) throws IOException {
    createTable();
  }
}
