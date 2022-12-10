/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.source;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.Scheduler;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.data.util.DataFormatConverters.DataFormatConverter;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkReadOptions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkLookupFunction extends TableFunction<RowData> {
  private static final long serialVersionUID = -7248058804931465381L;

  private static final Logger LOG = LoggerFactory.getLogger(FlinkLookupFunction.class);
  // the max number of retries before throwing exception, in case of failure to load the table into
  // cache
  private static final int MAX_RETRIES = 3;

  private final String[] projectedFields;

  private final DataType[] projectedTypes;

  private final String[] lookupKeys;

  private final long cacheMaxSize;

  private final long cacheExpireMs;

  private final int[] lookupCols;

  private transient LoadingCache<RowData, List<RowData>> cache;
  // serializer to copy RowData
  private transient TypeSerializer<RowData> serializer;
  // converters to convert data from internal to external in order to generate keys for the cache
  private final DataFormatConverter[] converters;
  private final DataType[] keyFieldTypes;

  private FlinkInputFormat inputFormat;

  private final ScanContext context;

  private final Schema icebergSchema;

  private final TableLoader tableLoader;

  private static final long MIN_RETRY_SLEEP_TIMEMS = 10000;

  private static final long MAX_RETRY_SLEEP_TIMEMS = 600000;

  private static final long MAX_RETRY_URATIONMS = 600000;

  private static final double SCALE_FACTOR = 2.0;

  private static final ExecutorService EXECUTOR_SERVICE =
      ThreadPools.newScheduledPool("iceberg-thread-pool-" + FlinkLookupFunction.class, 3);

  public FlinkLookupFunction(
      TableSchema schema,
      String[] lookupKeys,
      TableLoader tableLoader,
      Map<String, String> properties,
      Long limit,
      List<Expression> filters,
      ReadableConfig readableConfig) {
    Preconditions.checkNotNull(schema, "Table schema can not be null.");
    this.lookupCols = new int[lookupKeys.length];
    this.converters = new DataFormatConverter[lookupKeys.length];
    this.keyFieldTypes = new DataType[lookupKeys.length];
    this.tableLoader = tableLoader;

    tableLoader.open();
    this.icebergSchema = tableLoader.loadTable().schema();
    this.context =
        ScanContext.builder()
            .resolveConfig(tableLoader.loadTable(), properties, readableConfig)
            .project(icebergSchema)
            .filters(filters)
            .limit(limit == null ? FlinkReadOptions.LIMIT_OPTION.defaultValue() : limit)
            .build();
    this.projectedFields = schema.getFieldNames();
    this.projectedTypes = schema.getFieldDataTypes();
    this.lookupKeys = lookupKeys;
    this.cacheMaxSize = context.cacheMaxSize();
    this.cacheExpireMs = context.cacheExpireMs();

    Map<String, Integer> nameToIndex =
        IntStream.range(0, projectedFields.length)
            .boxed()
            .collect(Collectors.toMap(i -> projectedFields[i], i -> i));
    for (int i = 0; i < lookupKeys.length; i++) {
      Integer index = nameToIndex.get(lookupKeys[i]);
      Preconditions.checkArgument(
          index != null, "Lookup keys %s not selected", Arrays.toString(lookupKeys));
      converters[i] = DataFormatConverters.getConverterForDataType(projectedTypes[index]);
      keyFieldTypes[i] = projectedTypes[index];
      lookupCols[i] = index;
    }
  }

  private List<RowData> loadCache(RowData keys) {
    FlinkInputSplit[] inputSplits = getInputSplits(keys);

    GenericRowData reuse = new GenericRowData(projectedFields.length);

    DataFormatConverters.RowConverter rowConverter =
        new DataFormatConverters.RowConverter(keyFieldTypes);
    Row probeKey = rowConverter.toExternal(keys);

    List<RowData> results = Lists.newArrayList();
    Tasks.foreach(inputSplits)
        .executeWith(EXECUTOR_SERVICE)
        .retry(MAX_RETRIES)
        .exponentialBackoff(
            MIN_RETRY_SLEEP_TIMEMS, MAX_RETRY_SLEEP_TIMEMS, MAX_RETRY_URATIONMS, SCALE_FACTOR)
        .onFailure(
            (is, ex) -> {
              LOG.error("Failed to lookup iceberg table after {} retries", MAX_RETRIES, ex);
              throw ex;
            })
        .run(
            split -> {
              inputFormat.open(split);
              while (!inputFormat.reachedEnd()) {
                RowData row = inputFormat.nextRecord(reuse);
                Row key = FlinkLookupFunction.this.extractKey(row);
                if (probeKey.equals(key)) {
                  RowData newRow = serializer.copy(row);
                  results.add(newRow);
                }
              }

              try {
                inputFormat.close();
              } catch (IOException e) {
                LOG.error("Failed to close inputFormat.", e);
              }
            });
    return results;
  }

  @Override
  public void open(FunctionContext functionContext) throws Exception {
    super.open(functionContext);
    TypeInformation<RowData> rowDataTypeInfo =
        InternalTypeInfo.ofFields(
            Arrays.stream(projectedTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new),
            projectedFields);
    serializer = rowDataTypeInfo.createSerializer(new ExecutionConfig());
    Caffeine<Object, Object> builder = Caffeine.newBuilder();
    if (cacheMaxSize == -1 || cacheExpireMs == -1) {
      builder.maximumSize(0);
    } else {
      builder.expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS).maximumSize(cacheMaxSize);
    }

    cache =
        builder
            .scheduler(Scheduler.systemScheduler())
            .build(
                new CacheLoader<RowData, List<RowData>>() {
                  @Override
                  public @Nullable List<RowData> load(@NonNull RowData keys) throws Exception {
                    List<RowData> results = loadCache(keys);
                    return results;
                  }
                });
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public void eval(Object... values) {
    Preconditions.checkArgument(
        values.length == lookupKeys.length, "Number of values and lookup keys mismatch");
    cache.get(GenericRowData.of(values)).forEach(this::collect);
  }

  private Row extractKey(RowData row) {
    Row key = new Row(lookupCols.length);
    for (int i = 0; i < lookupCols.length; i++) {
      key.setField(i, converters[i].toExternal(row, lookupCols[i]));
    }
    return key;
  }

  public void filtersWithKeys(
      List<Expression> filters, LogicalType fieldType, RowData keys, int index) {
    switch (fieldType.getTypeRoot()) {
      case INTEGER:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
      case INTERVAL_YEAR_MONTH:
        filters.add(Expressions.equal(lookupKeys[index], keys.getInt(index)));
        break;
      case TINYINT:
        filters.add(Expressions.equal(lookupKeys[index], keys.getByte(index)));
        break;
      case SMALLINT:
        filters.add(Expressions.equal(lookupKeys[index], keys.getShort(index)));
        break;
      case VARCHAR:
      case CHAR:
        filters.add(Expressions.equal(lookupKeys[index], keys.getString(index).toString()));
        break;
      case BINARY:
      case VARBINARY:
        filters.add(Expressions.equal(lookupKeys[index], keys.getBinary(index)));
        break;
      case BOOLEAN:
        filters.add(Expressions.equal(lookupKeys[index], keys.getBoolean(index)));
        break;
      case DECIMAL:
        final int decimalPrecision = LogicalTypeChecks.getPrecision(fieldType);
        final int decimalScale = LogicalTypeChecks.getScale(fieldType);
        filters.add(
            Expressions.equal(
                lookupKeys[index], keys.getDecimal(index, decimalPrecision, decimalScale)));
        break;
      case FLOAT:
        filters.add(Expressions.equal(lookupKeys[index], keys.getFloat(index)));
        break;
      case DOUBLE:
        filters.add(Expressions.equal(lookupKeys[index], keys.getDouble(index)));
        break;
      case BIGINT:
      case INTERVAL_DAY_TIME:
        filters.add(Expressions.equal(lookupKeys[index], keys.getLong(index)));
        break;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        final int timestampPrecision = LogicalTypeChecks.getPrecision(fieldType);
        filters.add(
            Expressions.equal(lookupKeys[index], keys.getTimestamp(index, timestampPrecision)));
        break;
      case TIMESTAMP_WITH_TIME_ZONE:
        throw new UnsupportedOperationException();
      case ARRAY:
        filters.add(Expressions.equal(lookupKeys[index], keys.getArray(index)));
        break;
      case MULTISET:
      case MAP:
        filters.add(Expressions.equal(lookupKeys[index], keys.getMap(index)));
        break;
      case ROW:
      case STRUCTURED_TYPE:
        final int rowFieldCount = LogicalTypeChecks.getFieldCount(fieldType);
        filters.add(Expressions.equal(lookupKeys[index], keys.getRow(index, rowFieldCount)));
        break;
      case RAW:
        filters.add(Expressions.equal(lookupKeys[index], keys.getRawValue(index)));
        break;
      case NULL:
      case SYMBOL:
      default:
        throw new IllegalArgumentException();
    }
  }

  private FlinkInputSplit[] getInputSplits(RowData keys) {
    List<Expression> filters = Lists.newArrayList();
    for (int i = 0; i < keys.getArity(); i++) {
      LogicalType fieldType = keyFieldTypes[i].getLogicalType();
      filtersWithKeys(filters, fieldType, keys, i);
    }

    tableLoader.open();
    inputFormat =
        new FlinkInputFormat(
            tableLoader,
            tableLoader.loadTable().schema(),
            tableLoader.loadTable().io(),
            tableLoader.loadTable().encryption(),
            context.copyWithFilters(filters));

    FlinkInputSplit[] inputSplits = null;

    try {
      inputSplits = inputFormat.createInputSplits(0);
    } catch (IOException e) {
      LOG.error("Failed to create inputsplits.", e);
    }
    return inputSplits;
  }
}
