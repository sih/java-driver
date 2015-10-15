/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;


import java.util.*;

import com.google.common.base.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  An immutable representation of a materialized view.
 *  Materialized views are available starting from Cassandra 3.0.
 */
public class MaterializedViewMetadata extends TableOrView {

    private static final Logger logger = LoggerFactory.getLogger(MaterializedViewMetadata.class);

    private final TableMetadata baseTable;

    private final boolean includeAllColumns;

    private final String whereClause;

    private MaterializedViewMetadata(
        KeyspaceMetadata keyspace,
        TableMetadata baseTable,
        String name,
        UUID id,
        List<ColumnMetadata> partitionKey,
        List<ColumnMetadata> clusteringColumns,
        Map<String, ColumnMetadata> columns,
        boolean includeAllColumns,
        String whereClause,
        TableOptionsMetadata options,
        List<Order> clusteringOrder,
        VersionNumber cassandraVersion) {
        super(keyspace, name, id, partitionKey, clusteringColumns, columns, options, clusteringOrder, cassandraVersion);
        this.baseTable = baseTable;
        this.includeAllColumns = includeAllColumns;
        this.whereClause = whereClause;
    }

    static MaterializedViewMetadata build(KeyspaceMetadata keyspace, Row row, Map<String, ColumnMetadata.Raw> rawCols, VersionNumber cassandraVersion) {

        String name = row.getString("view_name");
        String tableName = row.getString("base_table_name");
        TableMetadata baseTable = keyspace.getTable(tableName);
        if(baseTable == null) {
            logger.trace(String.format("Cannot find base table %s for materialized view %s.%s: "
                    + "Cluster.getMetadata().getKeyspace(\"%s\").getView(\"%s\") will return null",
                tableName, keyspace.getName(), name, keyspace.getName(), name));
            return null;
        }

        UUID id = row.getUUID("id");
        boolean includeAllColumns = row.getBool("include_all_columns");
        String whereClause = row.getString("where_clause");

        int partitionKeySize = findCollectionSize(rawCols.values(), ColumnMetadata.Raw.Kind.PARTITION_KEY);
        int clusteringSize = findCollectionSize(rawCols.values(), ColumnMetadata.Raw.Kind.CLUSTERING_COLUMN);

        List<ColumnMetadata> partitionKey = new ArrayList<ColumnMetadata>(Collections.<ColumnMetadata>nCopies(partitionKeySize, null));
        List<ColumnMetadata> clusteringColumns = new ArrayList<ColumnMetadata>(Collections.<ColumnMetadata>nCopies(clusteringSize, null));
        List<Order> clusteringOrder = new ArrayList<Order>(Collections.<Order>nCopies(clusteringSize, null));

        // We use a linked hashmap because we will keep this in the order of a 'SELECT * FROM ...'.
        LinkedHashMap<String, ColumnMetadata> columns = new LinkedHashMap<String, ColumnMetadata>();

        TableOptionsMetadata options = null;
        try {
            options = new TableOptionsMetadata(row, false, cassandraVersion);
        } catch (RuntimeException e) {
            // See ControlConnection#refreshSchema for why we'd rather not probably this further. Since table options is one thing
            // that tends to change often in Cassandra, it's worth special casing this.
            logger.error(String.format("Error parsing schema options for view %s.%s: "
                    + "Cluster.getMetadata().getKeyspace(\"%s\").getView(\"%s\").getOptions() will return null",
                keyspace.getName(), name, keyspace.getName(), name), e);
        }

        MaterializedViewMetadata view = new MaterializedViewMetadata(
            keyspace, baseTable, name, id, partitionKey, clusteringColumns, columns,
            includeAllColumns, whereClause, options, clusteringOrder, cassandraVersion);

        // We use this temporary set just so non PK columns are added in lexicographical order, which is the one of a
        // 'SELECT * FROM ...'
        Set<ColumnMetadata> otherColumns = new TreeSet<ColumnMetadata>(columnMetadataComparator);
        for (ColumnMetadata.Raw rawCol : rawCols.values()) {
            ColumnMetadata col = ColumnMetadata.fromRaw(view, rawCol);
            switch (rawCol.kind) {
                case PARTITION_KEY:
                    partitionKey.set(rawCol.position, col);
                    break;
                case CLUSTERING_COLUMN:
                    clusteringColumns.set(rawCol.position, col);
                    clusteringOrder.set(rawCol.position, rawCol.isReversed ? Order.DESC : Order.ASC);
                    break;
                default:
                    otherColumns.add(col);
                    break;
            }
        }
        for (ColumnMetadata c : partitionKey)
            columns.put(c.getName(), c);
        for (ColumnMetadata c : clusteringColumns)
            columns.put(c.getName(), c);
        for (ColumnMetadata c : otherColumns)
            columns.put(c.getName(), c);

        baseTable.add(view);

        return view;

    }

    private static int findCollectionSize(Collection<ColumnMetadata.Raw> cols, ColumnMetadata.Raw.Kind kind) {
        int maxId = -1;
        for (ColumnMetadata.Raw col : cols)
            if (col.kind == kind)
                maxId = Math.max(maxId, col.position);
        return maxId + 1;
    }

    /**
     * Return this materialized view's base table.
     * @return this materialized view's base table.
     */
    public TableMetadata getBaseTable() {
        return baseTable;
    }

    @Override
    protected String asCQLQuery(boolean formatted) {

        String keyspaceName = Metadata.escapeId(keyspace.getName());
        String baseTableName = Metadata.escapeId(baseTable.getName());
        String viewName = Metadata.escapeId(name);

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE MATERIALIZED VIEW ")
            .append(keyspaceName).append('.').append(viewName)
            .append(" AS ");
        newLine(sb, formatted);

        // SELECT
        sb.append("SELECT ");
        if(includeAllColumns) {
            sb.append(" * ");
        } else {
            Iterator<ColumnMetadata> it = columns.values().iterator();
            while(it.hasNext()) {
                ColumnMetadata column = it.next();
                sb.append(spaces(4, formatted)).append(Metadata.escapeId(column.getName()));
                if(it.hasNext()) sb.append(",");
                sb.append(" ");
                newLine(sb, formatted);
            }
        }

        // FROM
        newLine(sb.append("FROM ").append(keyspaceName).append('.').append(baseTableName).append(" "), formatted);

        // WHERE
        // the CQL grammar allows missing WHERE clauses, although C* currently disallows it
        if (whereClause != null && !whereClause.isEmpty())
            newLine(sb.append("WHERE ").append(whereClause).append(' '), formatted);

        // PK
        sb.append("PRIMARY KEY (");
        if (partitionKey.size() == 1) {
            sb.append(Metadata.escapeId(partitionKey.get(0).getName()));
        } else {
            sb.append('(');
            boolean first = true;
            for (ColumnMetadata cm : partitionKey) {
                if (first)
                    first = false;
                else
                    sb.append(", ");
                sb.append(Metadata.escapeId(cm.getName()));
            }
            sb.append(')');
        }
        for (ColumnMetadata cm : clusteringColumns)
            sb.append(", ").append(Metadata.escapeId(cm.getName()));
        sb.append(')');

        appendOptions(sb, formatted);
        return sb.toString();

    }

    @Override
    public final boolean equals(Object other) {
        if (other == this)
            return true;
        if (!(other instanceof MaterializedViewMetadata))
            return false;

        MaterializedViewMetadata that = (MaterializedViewMetadata)other;

        boolean tableCmp = this.baseTable == that.baseTable;
        if(!tableCmp && this.baseTable != null && that.baseTable != null) {
            tableCmp = Objects.equal(this.baseTable.getName(), that.baseTable.getName());
        }

        return Objects.equal(this.name, that.name) &&
            Objects.equal(this.id, that.id) &&
            Objects.equal(this.partitionKey, that.partitionKey) &&
            Objects.equal(this.clusteringColumns, that.clusteringColumns) &&
            Objects.equal(this.columns, that.columns) &&
            Objects.equal(this.options, that.options) &&
            Objects.equal(this.clusteringOrder, that.clusteringOrder) &&
            tableCmp &&
            Objects.equal(this.whereClause, that.whereClause) &&
            this.includeAllColumns == that.includeAllColumns;
    }

    @Override
    public final int hashCode() {
        return Objects.hashCode(name, id, partitionKey, clusteringColumns, columns, options, clusteringOrder, whereClause,
            baseTable == null ? null : baseTable.getName(), includeAllColumns);
    }
}