/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pingcap.tidb.presto;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiTableInfo;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class TiDBClient {
  /**
   * SchemaName -> (TableName -> TableMetadata)
   */
  private final Supplier<Map<String, Map<String, TiTableInfo>>> schemas;

  private TiSession tiSession;

  private TiDBConfig config;

  @Inject
  public TiDBClient(TiDBConfig config) {
    requireNonNull(config, "config is null");

    this.config = config;
    TiConfiguration tiConfiguration = TiConfiguration.createDefault(config.getPdaddresses());
    tiSession = TiSession.getInstance(tiConfiguration);
    schemas = Suppliers.memoize(schemasSupplier(tiSession));
  }

  public TiDBConfig getConfig() {
    return config;
  }

  public TiSession getTiSession() {
    return tiSession;
  }

  public Set<String> getSchemaNames() {
    return schemas.get().keySet();
  }

  public Set<String> getTableNames(String schema) {
    requireNonNull(schema, "schema is null");
    Map<String, TiTableInfo> tables = schemas.get().get(schema);
    if (tables == null) {
      return ImmutableSet.of();
    }
    return tables.keySet();
  }

  public TiTableInfo getTable(String schema, String tableName) {
    requireNonNull(schema, "schema is null");
    requireNonNull(tableName, "tableName is null");
    Map<String, TiTableInfo> tables = schemas.get().get(schema);
    if (tables == null) {
      return null;
    }
    return tables.get(tableName);
  }

  private static Supplier<Map<String, Map<String, TiTableInfo>>> schemasSupplier(final TiSession tiSession) {
    return () -> {
      HashMap<String, Map<String, TiTableInfo>> result = new HashMap<>();
      Catalog catalog = tiSession.getCatalog();

      for (TiDBInfo databases : catalog.listDatabases()) {
        HashMap<String, TiTableInfo> map = new HashMap<>();
        for (TiTableInfo tiTableInfo : catalog.listTables(databases)) {
          map.put(tiTableInfo.getName(), tiTableInfo);
        }

        result.put(databases.getName(), map);
      }

      return result;
    };
  }
}
