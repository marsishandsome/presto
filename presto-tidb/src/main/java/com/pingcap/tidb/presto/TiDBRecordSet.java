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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.operation.iterator.CoprocessIterator;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.util.RangeSplitter;
import org.tikv.kvproto.Coprocessor;
import shade.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static com.pingcap.tikv.util.KeyRangeUtils.makeCoprocRange;
import static java.util.Objects.requireNonNull;

public class TiDBRecordSet
        implements RecordSet
{
    private final List<TiDBColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final CoprocessIterator<Row> iterator;

    public TiDBRecordSet(TiDBSplit split, List<TiDBColumnHandle> columnHandles)
    {
        requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (TiDBColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();

        ByteString startKey = ByteString.copyFrom(Base64.getDecoder().decode(split.getStartKey()));
        ByteString endKey = ByteString.copyFrom(Base64.getDecoder().decode(split.getEndKey()));

        TiConfiguration tiConfiguration = TiConfiguration.createDefault(split.getPdaddresses());
        TiSession tiSession = TiSession.getInstance(tiConfiguration);
        TiTableInfo tiTableInfo = tiSession.getCatalog().getTable(split.getSchemaName(), split.getTableName());

        TiTimestamp startTs = tiSession.getTimestamp();

        ArrayList<String> requiredCols = new ArrayList<>();
        for(TiColumnInfo col : tiTableInfo.getColumns()) {
            requiredCols.add(col.getName());
        }

        TiDAGRequest req = TiDAGRequest.Builder
            .newBuilder()
            .setFullTableScan(tiTableInfo)
            .addRequiredCols(requiredCols)
            .setStartTs(startTs)
            .build(TiDAGRequest.PushDownType.NORMAL);

        List<Coprocessor.KeyRange> keyRanges = ImmutableList.of(makeCoprocRange(startKey, endKey));
        List<RangeSplitter.RegionTask> regionTasks = RangeSplitter.newSplitter(tiSession.getRegionManager()).splitRangeByRegion(keyRanges);

        iterator = CoprocessIterator.getRowIterator(req,regionTasks, tiSession);
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new TiDBRecordCursor(columnHandles, iterator);
    }
}
