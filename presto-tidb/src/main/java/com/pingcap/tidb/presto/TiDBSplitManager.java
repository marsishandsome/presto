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
import com.pingcap.tikv.TiBatchWriteUtils;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.region.TiRegion;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TiDBSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final TiDBClient tiDBClient;

    @Inject
    public TiDBSplitManager(TiDBConnectorId connectorId, TiDBClient tiDBClient)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.tiDBClient = requireNonNull(tiDBClient, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle handle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        TiDBTableLayoutHandle layoutHandle = (TiDBTableLayoutHandle) layout;
        TiDBTableHandle tableHandle = layoutHandle.getTable();
        TiTableInfo table = tiDBClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists", tableHandle.getSchemaName(), tableHandle.getTableName());

        List<ConnectorSplit> splits = new ArrayList<>();
        // TODO: mars
        TiTableInfo tiTableInfo = tiDBClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());
        List<TiRegion> regionList = TiBatchWriteUtils.getRegionsByTable(tiDBClient.getTiSession(), tiTableInfo);

        for(TiRegion region : regionList) {
            String start = Base64.getEncoder().encodeToString(region.getStartKey().toByteArray());
            String end = Base64.getEncoder().encodeToString(region.getEndKey().toByteArray());
            splits.add(new TiDBSplit(connectorId, tableHandle.getPdaddresses(), tableHandle.getSchemaName(), tableHandle.getTableName(), tiTableInfo.getId(), start, end));
        }
        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }
}
