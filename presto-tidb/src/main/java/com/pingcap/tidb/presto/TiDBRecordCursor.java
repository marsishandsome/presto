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
import com.facebook.presto.spi.type.Type;
import com.pingcap.tikv.operation.iterator.CoprocessIterator;
import com.pingcap.tikv.row.Row;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.math.BigDecimal;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class TiDBRecordCursor
        implements RecordCursor
{
    private final List<TiDBColumnHandle> columnHandles;
    private final CoprocessIterator<Row> iterator;
    private Row row = null;

    public TiDBRecordCursor(List<TiDBColumnHandle> columnHandles, CoprocessIterator<Row> iterator)
    {
        this.columnHandles = columnHandles;
        this.iterator = iterator;
    }

    @Override
    public long getCompletedBytes()
    {
        return 100L;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if(iterator.hasNext()) {
            row = iterator.next();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean getBoolean(int field)
    {
        return (boolean) row.get(field, null);
    }

    @Override
    public long getLong(int field)
    {
      Object o = row.get(field, null);
      if(o instanceof BigDecimal) {
        return ((BigDecimal) o).longValue();
      } else {
        return (long) row.get(field, null);
      }
    }

    @Override
    public double getDouble(int field)
    {
        return (double) row.get(field, null);
    }

    @Override
    public Slice getSlice(int field)
    {
        String v = row.getString(field);
        return Slices.utf8Slice(v);
    }

    @Override
    public Object getObject(int field)
    {
        return row.get(field, null);
    }

    @Override
    public boolean isNull(int field)
    {
        return row.isNull(field);
    }

    @Override
    public void close()
    {
    }
}
