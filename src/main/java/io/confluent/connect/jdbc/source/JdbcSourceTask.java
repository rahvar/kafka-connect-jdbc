/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.jdbc.source;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
//import java.sql.*;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.SQLException;

//import java.util.*;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;
import java.util.PriorityQueue;

import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.JdbcUtils;
import io.confluent.connect.jdbc.util.Version;

/**
 * JdbcSourceTask is a Kafka Connect SourceTask implementation that reads from JDBC databases and
 * generates Kafka Connect records.
 */
public class JdbcSourceTask extends SourceTask {

  private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);

  private Time time;
  private JdbcSourceTaskConfig config;
  private CachedConnectionProvider cachedConnectionProvider;
  private PriorityQueue<TableQuerier> tableQueue = new PriorityQueue<TableQuerier>();
  private AtomicBoolean stop;
  private Map<String, String> anonymizeMap;
  private Map<String, Transformer> transformerMap;
  private String whitelistedColumns;

  public JdbcSourceTask() {
    this.time = new SystemTime();
  }

  public JdbcSourceTask(Time time) {
    this.time = time;
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  public Map<String, Transformer> loadAnonymizerClasses(JdbcSourceConnectorConfig config) {
    Map<String, Transformer> anonymizeMapping = null;
    if (config.originals().containsKey("transformers")) {
      try {
        if (!config.originals().containsKey("anonymization.keystore.pass")) {
          throw new Exception("Keystore password parameter not specified. (anonymization.keystore.pass)");
        }
        if (!config.originals().containsKey("anonymization.keystore.path")) {
          throw new Exception("Keystore path parameter not specified. (anonymization.keystore.path)");
        }
      } catch (Exception e) {
        e.printStackTrace();
        return anonymizeMapping;
      }
      String transformerString = (String) config.originals().get("transformers");
      String[] transformersList = transformerString.split(",");
      anonymizeMapping = new HashMap<String, Transformer>();
      for (String transformer:transformersList) {
        Class<?> c = null;
        try {
          c = Class.forName(transformer.trim());
          Method method = c.getDeclaredMethod("init", String.class, String.class);
          Transformer anonObj = (Transformer) method.invoke(null, (String) config.originals().get("anonymization.keystore.pass"),
                                                            (String) config.originals().get("anonymization.keystore.path"));
          anonymizeMapping.put(c.getSimpleName(), anonObj);
        } catch (ClassNotFoundException e) {
          e.printStackTrace();
        } catch (NoSuchMethodException e) {
          e.printStackTrace();
        } catch (IllegalAccessException e) {
          e.printStackTrace();
        } catch (InvocationTargetException e) {
          e.printStackTrace();
        }
      }
    }
    return anonymizeMapping;
  }

  @Override
  public void start(Map<String, String> properties) {
    try {
      config = new JdbcSourceTaskConfig(properties);
      transformerMap = loadAnonymizerClasses(config);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start JdbcSourceTask due to configuration error", e);
    }

    final String dbUrl = config.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG);
    final String dbUser = config.getString(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG);
    final Password dbPassword = config.getPassword(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG);
    cachedConnectionProvider = new CachedConnectionProvider(dbUrl, dbUser, dbPassword == null ? null : dbPassword.value());

    List<String> tables = config.getList(JdbcSourceTaskConfig.TABLES_CONFIG);
    String query = config.getString(JdbcSourceTaskConfig.QUERY_CONFIG);
    if ((tables.isEmpty() && query.isEmpty()) || (!tables.isEmpty() && !query.isEmpty())) {
      throw new ConnectException("Invalid configuration: each JdbcSourceTask must have at "
                                        + "least one table assigned to it or one query specified");
    }
    TableQuerier.QueryMode queryMode = !query.isEmpty() ? TableQuerier.QueryMode.QUERY :
                                       TableQuerier.QueryMode.TABLE;
    List<String> tablesOrQuery = queryMode == TableQuerier.QueryMode.QUERY ?
                                 Collections.singletonList(query) : tables;

    String mode = config.getString(JdbcSourceTaskConfig.MODE_CONFIG);
    Map<Map<String, String>, Map<String, Object>> offsets = null;
    if (mode.equals(JdbcSourceTaskConfig.MODE_INCREMENTING) ||
        mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP) ||
        mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING)) {
      List<Map<String, String>> partitions = new ArrayList<>(tables.size());
      switch (queryMode) {
        case TABLE:
          for (String table : tables) {
            Map<String, String> partition =
                Collections.singletonMap(JdbcSourceConnectorConstants.TABLE_NAME_KEY, table);
            partitions.add(partition);
          }
          break;
        case QUERY:
          partitions.add(Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                                                  JdbcSourceConnectorConstants.QUERY_NAME_VALUE));
          break;
      }
      offsets = context.offsetStorageReader().offsets(partitions);
    }

    String schemaPattern
        = config.getString(JdbcSourceTaskConfig.SCHEMA_PATTERN_CONFIG);
    String incrementingColumn
        = config.getString(JdbcSourceTaskConfig.INCREMENTING_COLUMN_NAME_CONFIG);
    String timestampColumn
        = config.getString(JdbcSourceTaskConfig.TIMESTAMP_COLUMN_NAME_CONFIG);
    Long timestampDelayInterval
        = config.getLong(JdbcSourceTaskConfig.TIMESTAMP_DELAY_INTERVAL_MS_CONFIG);
    boolean validateNonNulls
        = config.getBoolean(JdbcSourceTaskConfig.VALIDATE_NON_NULL_CONFIG);

//    log.info("Default Incrementing Column: "+incrementingColumn);
//    log.info("Default Timestamp Column: "+timestampColumn);

    String defaultAnonymizer = null;
    if (config.originals().containsKey(JdbcSourceTaskConfig.ANONYMIZE + ".default")) {
      try {
        defaultAnonymizer = config.getString(JdbcSourceTaskConfig.ANONYMIZE + ".default");
        if (transformerMap == null) {
          throw new Exception("Transformers not configured correctly. Please ensure the configuration file specifies transformers " +
                  "e.g. transformers=io.confluent.connect.jdbc.source.HashAnonymizer");
        }
        if (!transformerMap.containsKey(defaultAnonymizer)) {
          throw new Exception("Default anonymization class: " + defaultAnonymizer + " does not exist. Please re-check configuration file." +
                  "e.g. anonymize.default=HashAnonymizer");
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    for (String tableOrQuery : tablesOrQuery) {
      String tableMode = mode;
      try {
        tableMode = config.getString(tableOrQuery + "." + JdbcSourceTaskConfig.MODE_CONFIG);
      } catch (Exception e) {
        log.info("Table specific mode not defined. Reverting to default");
      }

      anonymizeMap = null;
      try {
        String anonymizeKey = tableOrQuery + "." + JdbcSourceTaskConfig.ANONYMIZE + ".column.name";
        if (config.originals().containsKey(anonymizeKey)) {
          if (defaultAnonymizer == null) {
            throw new Exception("Could not find default anonymization class. Please re-check configuration file." +
                    "e.g. anonymize.default=HashAnonymizer");
          }
          String colsToAnonymizeconfig = config.getString(anonymizeKey);
          ArrayList<String> anonymizeList = new  ArrayList<String>(Arrays.asList(colsToAnonymizeconfig.split(",")));
          anonymizeMap = new HashMap<String, String>();
          for (String col:anonymizeList) {
            if (col.contains(":")) {
              String anonClassForCol = col.split(":")[1];
              if (!transformerMap.containsKey(anonClassForCol)) {
                anonymizeMap.put(col, defaultAnonymizer);
                throw new Exception("Anonymization Class:" + anonClassForCol + " for column: " + col + " does not exist. Will revert to default");
              } else {
                anonymizeMap.put(col.split(":")[0], col.split(":")[1]);
              }
            } else {
              anonymizeMap.put(col, defaultAnonymizer);
            }
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }

      /*
      Retrieve whitelisted columns if any.
       */
      whitelistedColumns = null;
      try {
        String whitelistedCol = tableOrQuery + ".whitelist.column.name";
        if (config.originals().containsKey(whitelistedCol)) {
          whitelistedColumns = config.getString(whitelistedCol);
        }
      } catch (Exception e) {
      }

      Connection connection = cachedConnectionProvider.getValidConnection();

      final Set<String> pkColumns = new HashSet<>();
      ResultSet pkResultSet = null;
      try {

        final String catalog = connection.getCatalog();
        final String schema = connection.getSchema();
        pkResultSet = connection.getMetaData().getPrimaryKeys(catalog, schema, tableOrQuery);


        //stmt.close();
        while (pkResultSet.next()) {
          final String colName = pkResultSet.getString("COLUMN_NAME");

          pkColumns.add(colName);
        }

      } catch (SQLException e) {
        e.printStackTrace();
      }


      final Map<String, String> partition;
      switch (queryMode) {
        case TABLE:
          if (validateNonNulls) {
            validateNonNullable(mode, schemaPattern, tableOrQuery, incrementingColumn, timestampColumn);
          }
          partition = Collections.singletonMap(
              JdbcSourceConnectorConstants.TABLE_NAME_KEY, tableOrQuery);
          break;
        case QUERY:
          partition = Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                                               JdbcSourceConnectorConstants.QUERY_NAME_VALUE);
          break;
        default:
          throw new ConnectException("Unexpected query mode: " + queryMode);
      }
      Map<String, Object> offset = offsets == null ? null : offsets.get(partition);

      String topicPrefix = config.getString(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG);
      boolean mapNumerics = config.getBoolean(JdbcSourceTaskConfig.NUMERIC_PRECISION_MAPPING_CONFIG);

      if (tableMode.equals(JdbcSourceTaskConfig.MODE_BULK)) {
        tableQueue.add(new BulkTableQuerier(queryMode, tableOrQuery, schemaPattern,
                topicPrefix, mapNumerics, anonymizeMap, pkColumns, transformerMap, whitelistedColumns));
      } else if (tableMode.equals(JdbcSourceTaskConfig.MODE_INCREMENTING)) {

        String tableIncrementingColumn = incrementingColumn;
        try {
          tableIncrementingColumn = config.getString(tableOrQuery + "." + JdbcSourceTaskConfig.INCREMENTING_COLUMN_NAME_CONFIG);
        } catch (Exception e) {
          log.warn(tableOrQuery + ".incrementing.column.name" + " has not been defined! Attempting auto detection of column using metadata");
        }
        if (validateNonNulls)
          validateNonNullable(mode, schemaPattern, tableOrQuery, tableIncrementingColumn, timestampColumn);
        tableQueue.add(new TimestampIncrementingTableQuerier(
            queryMode, tableOrQuery, topicPrefix, null, tableIncrementingColumn, offset,
                timestampDelayInterval, schemaPattern, mapNumerics, anonymizeMap, pkColumns, transformerMap, whitelistedColumns));
      } else if (tableMode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP)) {
        String tableTimestampColumn = timestampColumn;
        try {
          tableTimestampColumn = config.getString(tableOrQuery + "." + JdbcSourceTaskConfig.TIMESTAMP_COLUMN_NAME_CONFIG);
        } catch (Exception e) {
          log.warn(tableOrQuery + ".timestamp.column.name" + " has not been defined! Attempting auto detection of column using metadata");
          //continue;
        }
        if (validateNonNulls)
          validateNonNullable(mode, schemaPattern, tableOrQuery, incrementingColumn, tableTimestampColumn);
        tableQueue.add(new TimestampIncrementingTableQuerier(
            queryMode, tableOrQuery, topicPrefix, tableTimestampColumn, null, offset,
                timestampDelayInterval, schemaPattern, mapNumerics, anonymizeMap, pkColumns, transformerMap, whitelistedColumns));
      } else if (tableMode.endsWith(JdbcSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING)) {

        String tableIncrementingColumn = incrementingColumn;
        String tableTimestampColumn = timestampColumn;

        try {
          tableIncrementingColumn = config.getString(tableOrQuery + "." + JdbcSourceTaskConfig.INCREMENTING_COLUMN_NAME_CONFIG);
        } catch (Exception e) {
          log.warn(tableOrQuery + ".incrementing.column.name" + " has not been defined! Attempting auto detection of column using metadata");
          //continue;
        }
        try {
          tableTimestampColumn = config.getString(tableOrQuery + "." + JdbcSourceTaskConfig.TIMESTAMP_COLUMN_NAME_CONFIG);
        } catch (Exception e) {
          log.warn(tableOrQuery + ".timestamp.column.name" + " has not been defined! Attempting auto detection of column using metadata");
          //continue;
        }
        if (validateNonNulls)
          validateNonNullable(mode, schemaPattern, tableOrQuery, tableIncrementingColumn, tableTimestampColumn);
        tableQueue.add(new TimestampIncrementingTableQuerier(
            queryMode, tableOrQuery, topicPrefix, tableTimestampColumn, tableIncrementingColumn,
                offset, timestampDelayInterval, schemaPattern, mapNumerics, anonymizeMap, pkColumns, transformerMap, whitelistedColumns));
      }
    }
    stop = new AtomicBoolean(false);
  }

  @Override
  public void stop() throws ConnectException {
    if (stop != null) {
      stop.set(true);
    }
    if (cachedConnectionProvider != null) {
      cachedConnectionProvider.closeQuietly();
    }
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    log.trace("{} Polling for new data");

    while (!stop.get()) {
      final TableQuerier querier = tableQueue.peek();

      if (!querier.querying()) {
        // If not in the middle of an update, wait for next update time
        final long nextUpdate = querier.getLastUpdate() + config.getInt(JdbcSourceTaskConfig.POLL_INTERVAL_MS_CONFIG);
        final long untilNext = nextUpdate - time.milliseconds();
        if (untilNext > 0) {
          log.trace("Waiting {} ms to poll {} next", untilNext, querier.toString());
          time.sleep(untilNext);
          continue; // Re-check stop flag before continuing
        }
      }

      final List<SourceRecord> results = new ArrayList<>();
      try {
        log.debug("Checking for next block of results from {}", querier.toString());
        querier.maybeStartQuery(cachedConnectionProvider.getValidConnection());

        int batchMaxRows = config.getInt(JdbcSourceTaskConfig.BATCH_MAX_ROWS_CONFIG);
        boolean hadNext = true;
        while (results.size() < batchMaxRows && (hadNext = querier.next())) {
          results.add(querier.extractRecord());
        }

        if (!hadNext) {
          // If we finished processing the results from the current query, we can reset and send the querier to the tail of the queue
          resetAndRequeueHead(querier);
        }

        if (results.isEmpty()) {
          log.trace("No updates for {}", querier.toString());
          continue;
        }

        log.debug("Returning {} records for {}", results.size(), querier.toString());
        return results;
      } catch (SQLException e) {
        log.error("Failed to run query for table {}: {}", querier.toString(), e);
        resetAndRequeueHead(querier);
        return null;
      }
    }

    // Only in case of shutdown
    return null;
  }

  private void resetAndRequeueHead(TableQuerier expectedHead) {
    log.debug("Resetting querier {}", expectedHead.toString());
    TableQuerier removedQuerier = tableQueue.poll();
    assert removedQuerier == expectedHead;
    expectedHead.reset(time.milliseconds());
    tableQueue.add(expectedHead);
  }

  private void validateNonNullable(String incrementalMode, String schemaPattern, String table, String incrementingColumn, String timestampColumn) {
    try {
      final Connection connection = cachedConnectionProvider.getValidConnection();
      // Validate that requested columns for offsets are NOT NULL. Currently this is only performed
      // for table-based copying because custom query mode doesn't allow this to be looked up
      // without a query or parsing the query since we don't have a table name.
      if ((incrementalMode.equals(JdbcSourceConnectorConfig.MODE_INCREMENTING) ||
           incrementalMode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING)) &&
          JdbcUtils.isColumnNullable(connection, schemaPattern, table, incrementingColumn)) {
        throw new ConnectException("Cannot make incremental queries using incrementing column " +
                                   incrementingColumn + " on " + table + " because this column is "
                                   + "nullable.");
      }
      if ((incrementalMode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP) ||
           incrementalMode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING)) &&
          JdbcUtils.isColumnNullable(connection, schemaPattern, table, timestampColumn)) {
        throw new ConnectException("Cannot make incremental queries using timestamp column " +
                                   timestampColumn + " on " + table + " because this column is "
                                   + "nullable.");
      }
    } catch (SQLException e) {
      throw new ConnectException("Failed trying to validate that columns used for offsets are NOT"
                                 + " NULL", e);
    }
  }
}
