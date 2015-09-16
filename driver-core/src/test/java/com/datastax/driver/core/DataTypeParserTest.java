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

import java.util.Collection;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.DataType.*;
import static com.datastax.driver.core.DataTypeParser.parse;
public class DataTypeParserTest extends CCMBridge.PerClassSingleNodeCluster {

    @Test(groups = "short")
    public void should_parse_native_types() {
        assertThat(parse("ascii", cluster.getMetadata(), keyspace, false)).isEqualTo(ascii());
        assertThat(parse("bigint", cluster.getMetadata(), keyspace, false)).isEqualTo(bigint());
        assertThat(parse("blob", cluster.getMetadata(), keyspace, false)).isEqualTo(blob());
        assertThat(parse("boolean", cluster.getMetadata(), keyspace, false)).isEqualTo(cboolean());
        assertThat(parse("counter", cluster.getMetadata(), keyspace, false)).isEqualTo(counter());
        assertThat(parse("decimal", cluster.getMetadata(), keyspace, false)).isEqualTo(decimal());
        assertThat(parse("double", cluster.getMetadata(), keyspace, false)).isEqualTo(cdouble());
        assertThat(parse("float", cluster.getMetadata(), keyspace, false)).isEqualTo(cfloat());
        assertThat(parse("inet", cluster.getMetadata(), keyspace, false)).isEqualTo(inet());
        assertThat(parse("int", cluster.getMetadata(), keyspace, false)).isEqualTo(cint());
        assertThat(parse("text", cluster.getMetadata(), keyspace, false)).isEqualTo(text());
        assertThat(parse("varchar", cluster.getMetadata(), keyspace, false)).isEqualTo(varchar());
        assertThat(parse("timestamp", cluster.getMetadata(), keyspace, false)).isEqualTo(timestamp());
        assertThat(parse("date", cluster.getMetadata(), keyspace, false)).isEqualTo(date());
        assertThat(parse("time", cluster.getMetadata(), keyspace, false)).isEqualTo(time());
        assertThat(parse("uuid", cluster.getMetadata(), keyspace, false)).isEqualTo(uuid());
        assertThat(parse("varint", cluster.getMetadata(), keyspace, false)).isEqualTo(varint());
        assertThat(parse("timeuuid", cluster.getMetadata(), keyspace, false)).isEqualTo(timeuuid());
        assertThat(parse("tinyint", cluster.getMetadata(), keyspace, false)).isEqualTo(tinyint());
        assertThat(parse("smallint", cluster.getMetadata(), keyspace, false)).isEqualTo(smallint());
    }

    @Test(groups = "short")
    public void should_ignore_whitespace() {
        assertThat(parse("  int  ", cluster.getMetadata(), keyspace, false)).isEqualTo(cint());
        assertThat(parse("  set < bigint > ", cluster.getMetadata(), keyspace, false)).isEqualTo(set(bigint()));
        assertThat(parse("  map  <  date  ,  timeuuid  >  ", cluster.getMetadata(), keyspace, false)).isEqualTo(map(date(), timeuuid()));
    }

    @Test(groups = "short")
    public void should_ignore_case() {
        assertThat(parse("INT", cluster.getMetadata(), keyspace, false)).isEqualTo(cint());
        assertThat(parse("SET<BIGint>", cluster.getMetadata(), keyspace, false)).isEqualTo(set(bigint()));
        assertThat(parse("FROZEN<mAp<Date,Tuple<timeUUID>>>", cluster.getMetadata(), keyspace, false)).isEqualTo(map(date(), cluster.getMetadata().newTupleType(timeuuid()), true));
    }

    @Test(groups = "short")
    public void should_parse_collection_types() {
        assertThat(parse("list<int>", cluster.getMetadata(), keyspace, false)).isEqualTo(list(cint()));
        assertThat(parse("set<bigint>", cluster.getMetadata(), keyspace, false)).isEqualTo(set(bigint()));
        assertThat(parse("map<date,timeuuid>", cluster.getMetadata(), keyspace, false)).isEqualTo(map(date(), timeuuid()));
    }

    @Test(groups = "short")
    public void should_parse_frozen_collection_types() {
        assertThat(parse("frozen<list<int>>", cluster.getMetadata(), keyspace, false)).isEqualTo(list(cint(), true));
        assertThat(parse("frozen<set<bigint>>", cluster.getMetadata(), keyspace, false)).isEqualTo(set(bigint(), true));
        assertThat(parse("frozen<map<date,timeuuid>>", cluster.getMetadata(), keyspace, false)).isEqualTo(map(date(), timeuuid(), true));
    }

    @Test(groups = "short")
    public void should_parse_nested_collection_types() {
        assertThat(parse("list<list<int>>", cluster.getMetadata(), keyspace, false)).isEqualTo(list(list(cint())));
        assertThat(parse("set<list<frozen<map<bigint,varchar>>>>", cluster.getMetadata(), keyspace, false)).isEqualTo(set(list(map(bigint(), varchar(), true))));
    }

    @Test(groups = "short")
    public void should_parse_tuple_types() {
        assertThat(parse("tuple<int,list<text>>", cluster.getMetadata(), keyspace, false)).isEqualTo(cluster.getMetadata().newTupleType(cint(), list(text())));
    }

    @Test(groups = "short")
    public void should_parse_user_defined_types() {
        // note that mixed case identifiers are not quoted in schema tables
        assertThat(parse("MyUdt", cluster.getMetadata(), keyspace, false)).isEqualTo(cluster.getMetadata().getKeyspace(keyspace).getUserType("\"MyUdt\""));
    }

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList(String.format("CREATE TYPE %s.\"MyUdt\" (f1 int, f2 frozen<list<text>>)", keyspace));
    }
    
}
