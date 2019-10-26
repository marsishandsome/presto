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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.*;
import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.expression.*;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.operation.iterator.CoprocessIterator;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.util.KeyRangeUtils;
import com.pingcap.tikv.util.RangeSplitter;
import io.airlift.slice.Slice;
import org.tikv.kvproto.Coprocessor;
import shade.com.google.protobuf.ByteString;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.Decimals.decodeUnscaledValue;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TiDBRecordSet
        implements RecordSet {
    private final List<TiDBColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final CoprocessIterator<Row> iterator;

    private Expression buildExpressionFromPredicate(RowExpression predicate) {
        Expression tiExpression = null;
        if (predicate instanceof SpecialFormExpression) {
            String form = ((SpecialFormExpression) predicate).getForm().name();
            List<Expression> expressions = ((SpecialFormExpression) predicate)
                    .getArguments().stream()
                    .map(each -> buildExpressionFromPredicate(each))
                    .collect(Collectors.toList());
            switch (form) {
                case "AND": {
                    for (Expression each : expressions) {
                        if (tiExpression == null) {
                            tiExpression = each;
                        } else {
                            tiExpression = LogicalBinaryExpression.and(each, tiExpression);
                        }
                    }
                    break;
                }
                case "OR": {
                    for (Expression each : expressions) {
                        if (tiExpression == null) {
                            tiExpression = each;
                        } else {
                            tiExpression = LogicalBinaryExpression.or(each, tiExpression);
                        }
                    }
                    break;
                }
                default:
                    break;
            }
        } else if (predicate instanceof CallExpression) {
            String displayName = ((CallExpression) predicate).getDisplayName();
            switch (displayName) {
                case "LIKE": {
                    String rightValue = new String(((Slice) ((ConstantExpression) ((CallExpression) ((CallExpression) predicate).getArguments().get(1)).getArguments().get(0)).getValue()).getBytes());
                    String leftValue = ((VariableReferenceExpression) ((CallExpression) predicate).getArguments().get(0)).getName();
                    tiExpression = StringRegExpression.like(ColumnRef.create(leftValue), Constant.create(rightValue));
                    break;
                }
                case "EQUAL": {
                    String leftValue = ((VariableReferenceExpression) ((CallExpression) predicate).getArguments().get(0)).getName();
                    ConstantExpression constant = ((ConstantExpression) ((CallExpression) predicate).getArguments().get(1));
                    if (constant.getType() instanceof BigintType) {
                        Long rightValue = (Long)((ConstantExpression) ((CallExpression) predicate).getArguments().get(1)).getValue();
                        tiExpression = ComparisonBinaryExpression.equal(ColumnRef.create(leftValue), Constant.create(rightValue));
                    }
                    if (constant.getType() instanceof VarcharType) {
                        String rightValue = new String(((Slice) ((ConstantExpression) ((CallExpression) predicate).getArguments().get(1)).getValue()).getBytes());
                        tiExpression = ComparisonBinaryExpression.equal(ColumnRef.create(leftValue), Constant.create(rightValue));
                    }
                }
                default:
                    break;
            }
        }
        return tiExpression;
    }


    public TiDBRecordSet(TiDBSplit split, List<TiDBColumnHandle> columnHandles) {
        requireNonNull(split, "split is null");

        // TODO: filter push down
        TupleDomain<ColumnHandle> tupleDomain = split.getTupleDomain();

        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (TiDBColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();

        TiConfiguration tiConfiguration = TiConfiguration.createDefault(split.getPdaddresses());
        TiSession tiSession = TiSession.getInstance(tiConfiguration);
        TiTableInfo tiTableInfo = tiSession.getCatalog().getTable(split.getSchemaName(), split.getTableName());

        TiTimestamp startTs = tiSession.getTimestamp();

        ArrayList<String> requiredCols = new ArrayList<>();
        for (TiDBColumnHandle col : columnHandles) {
            requiredCols.add(col.getColumnName());
        }

        if (requiredCols.isEmpty()) {
            requiredCols.add(tiTableInfo.getColumns().get(0).getName());
        }

        List<com.pingcap.tikv.expression.Expression> filters = new ArrayList<>();
        if (split.isEnablePPD()) {
            if (tupleDomain.getDomains().isPresent()) {
                Map<ColumnHandle, Domain> domainMap = tupleDomain.getDomains().get();
                // try to pushdown filters
                for (TiDBColumnHandle col : columnHandles) {
                    Domain domain = domainMap.get(col);
                    if (domain != null) {
                        filters.add(getExprFromDomain(col.getColumnName(), col.getColumnType(), domain));
                    }
                }
            }
        }

        TiDAGRequest.Builder builder = TiDAGRequest.Builder
                .newBuilder()
                .setFullTableScan(tiTableInfo)
                .addRequiredCols(requiredCols)
                .setStartTs(startTs);
        if (filters.size() > 0)
            builder.addFilter(combineExpressions(filters, LogicalBinaryExpression.Type.AND));

        try {
            Expression expression = buildExpressionFromPredicate(split.getPredicate());
            if (expression != null) {
                builder.addFilter(expression);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        TiDAGRequest req = builder.build(TiDAGRequest.PushDownType.NORMAL);

        ByteString startKey = ByteString.copyFrom(Base64.getDecoder().decode(split.getStartKey()));
        ByteString endKey = ByteString.copyFrom(Base64.getDecoder().decode(split.getEndKey()));

        List<Coprocessor.KeyRange> keyRanges = ImmutableList.of(KeyRangeUtils.makeCoprocRange(startKey, endKey));
        List<RangeSplitter.RegionTask> regionTasks =
                RangeSplitter.newSplitter(tiSession.getRegionManager()).splitRangeByRegion(keyRanges);

        iterator = CoprocessIterator.getRowIterator(req, regionTasks, tiSession);
    }

    private Object convertToTiDBValue(Object value, Type type) {
        if (type instanceof DateType) {
            Long dateInDay = (Long) value;
            return new java.sql.Date(dateInDay * 24 * 3600 * 1000);
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            Slice s = (Slice) value;
            return s.toStringUtf8();
        }
        if (type instanceof DecimalType) {
            if (((DecimalType) type).isShort()) {
                Long val = (Long) value;
                return new BigDecimal(BigInteger.valueOf(val), ((DecimalType) type).getScale());
            } else {
                Slice val = (Slice) value;
                return new BigDecimal(decodeUnscaledValue(val), ((DecimalType) type).getScale());
            }
        }
        return value;
    }

    private Expression getComparisonBinaryExpr(Expression col_ref, Object value, Type type, ComparisonBinaryExpression.Type funcType) {
        Object tidbValue = convertToTiDBValue(value, type);
        Expression const_value = Constant.create(tidbValue);
        return new ComparisonBinaryExpression(funcType, col_ref, const_value);
    }

    private Expression combineExpressions(List<Expression> expressions, LogicalBinaryExpression.Type type) {
        Expression ret = expressions.get(0);
        for (int i = 1; i < expressions.size(); i++) {
            ret = new LogicalBinaryExpression(type, ret, expressions.get(i));
        }
        return ret;
    }

    private Expression getExprFromDomain(String colname, Type type, Domain domain) {
        Expression col_ref = ColumnRef.create(colname);
        if (domain.getValues().isNone()) {
            return new IsNull(col_ref);
        }
        if (domain.getValues().isAll()) {
            Expression is_null = new IsNull(col_ref);
            return new Not(is_null);
        }

        List<Expression> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
            checkState(!range.isAll());
            if (range.isSingleValue()) {
                singleValues.add(range.getLow().getValue());
            } else {
                List<Expression> rangeConjuncts = new ArrayList<>();
                if (!range.getLow().isLowerUnbounded()) {
                    switch (range.getLow().getBound()) {
                        case ABOVE:
                            rangeConjuncts.add(getComparisonBinaryExpr(col_ref, range.getLow().getValue(), type,
                                    ComparisonBinaryExpression.Type.GREATER_THAN));
                            break;
                        case EXACTLY:
                            rangeConjuncts.add(getComparisonBinaryExpr(col_ref, range.getLow().getValue(), type,
                                    ComparisonBinaryExpression.Type.GREATER_EQUAL));
                            break;
                        case BELOW:
                            throw new IllegalArgumentException("Low marker should never use BELOW bound");
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                    }
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    switch (range.getHigh().getBound()) {
                        case ABOVE:
                            throw new IllegalArgumentException("High marker should never use ABOVE bound");
                        case EXACTLY:
                            rangeConjuncts.add(getComparisonBinaryExpr(col_ref, range.getHigh().getValue(), type,
                                    ComparisonBinaryExpression.Type.LESS_EQUAL));
                            break;
                        case BELOW:
                            rangeConjuncts.add(getComparisonBinaryExpr(col_ref, range.getHigh().getValue(), type,
                                    ComparisonBinaryExpression.Type.LESS_THAN));
                            break;
                        default:
                            throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                    }
                }
                checkState(!rangeConjuncts.isEmpty());
                disjuncts.add(combineExpressions(rangeConjuncts, LogicalBinaryExpression.Type.AND));
            }
        }
        for (Object o : singleValues) {
            // todo support In expression in tikv client
            disjuncts.add(getComparisonBinaryExpr(col_ref, o, type, ComparisonBinaryExpression.Type.EQUAL));
        }

        checkState(!disjuncts.isEmpty());
        if (domain.isNullAllowed())
            disjuncts.add(new IsNull(col_ref));

        return combineExpressions(disjuncts, LogicalBinaryExpression.Type.OR);
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        return new TiDBRecordCursor(columnHandles, iterator);
    }
}
