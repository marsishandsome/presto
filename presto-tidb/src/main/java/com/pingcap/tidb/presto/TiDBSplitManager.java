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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.key.RowKey;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.util.KeyRangeUtils;
import com.pingcap.tikv.util.RangeSplitter;
import org.tikv.kvproto.Coprocessor;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TiDBSplitManager
    implements ConnectorSplitManager {
  private final String connectorId;
  private final TiDBClient tiDBClient;

  @Inject
  public TiDBSplitManager(TiDBConnectorId connectorId, TiDBClient tiDBClient) {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
    this.tiDBClient = requireNonNull(tiDBClient, "client is null");
  }

  @Override
  public ConnectorSplitSource getSplits(
      ConnectorTransactionHandle handle,
      ConnectorSession session,
      ConnectorTableLayoutHandle layout,
      SplitSchedulingContext splitSchedulingContext) {
    TiDBTableLayoutHandle layoutHandle = (TiDBTableLayoutHandle) layout;
    TiDBTableHandle tableHandle = layoutHandle.getTable();
    TiSession tiSession = tiDBClient.getTiSession();
    TiTableInfo table = tiDBClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());
    // this can happen if table is removed during a query
    checkState(table != null, "Table %s.%s no longer exists",
        tableHandle.getSchemaName(), tableHandle.getTableName());

    List<ConnectorSplit> splits = new ArrayList<>();

    RowKey start = RowKey.createMin(table.getId());
    RowKey end = RowKey.createBeyondMax(table.getId());
    List<Coprocessor.KeyRange> keyRanges =
        ImmutableList.of(KeyRangeUtils.makeCoprocRange(start.toByteString(), end.toByteString()));
    List<RangeSplitter.RegionTask> regionTasks =
        RangeSplitter.newSplitter(tiSession.getRegionManager()).splitRangeByRegion(keyRanges);

    int i = 0;
    for (RangeSplitter.RegionTask task : regionTasks) {
      for(Coprocessor.KeyRange keyRange : task.getRanges()) {
        String taskStart = Base64.getEncoder().encodeToString(keyRange.getStart().toByteArray());
        String taskEnd = Base64.getEncoder().encodeToString(keyRange.getEnd().toByteArray());
        splits.add(new TiDBSplit(i, connectorId, tableHandle.getPdaddresses(),
            tableHandle.getSchemaName(), tableHandle.getTableName(), table.getId(), taskStart, taskEnd, layoutHandle.getTupleDomain()));
        i = i + 1;
      }
    }
    Collections.shuffle(splits);

    return new FixedSplitSource(splits);
  }
}
