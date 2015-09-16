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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.DriverInternalError;

import static com.datastax.driver.core.DataType.*;
import static com.datastax.driver.core.Metadata.escapeId;
import static com.datastax.driver.core.ParseUtils.isBlank;
import static com.datastax.driver.core.ParseUtils.isIdentifierChar;
import static com.datastax.driver.core.ParseUtils.skipSpaces;

/*
 * Helps transforming CQL types (as read in the schema tables) to
 * DataTypes, starting from Cassandra 3.0.
 *
 * Note that these methods all throw DriverInternalError when there is a parsing
 * problem because in theory we'll only parse class names coming from Cassandra and
 * so there shouldn't be anything wrong with them.
 */
class DataTypeParser {

    private static final Logger logger = LoggerFactory.getLogger(DataTypeParser.class);

    private static final String FROZEN = "frozen";
    private static final String LIST   = "list";
    private static final String SET    = "set";
    private static final String MAP    = "map";
    private static final String TUPLE  = "tuple";
    private static final String EMPTY  = "empty";

    private static final ImmutableMap<String, DataType> NATIVE_TYPES_MAP =
        new ImmutableMap.Builder<String, DataType>()
            .put("ascii",     ascii())
            .put("bigint",    bigint())
            .put("blob",      blob())
            .put("boolean",   cboolean())
            .put("counter",   counter())
            .put("decimal",   decimal())
            .put("double",    cdouble())
            .put("float",     cfloat())
            .put("inet",      inet())
            .put("int",       cint())
            .put("text",      text())
            .put("varchar",   varchar())
            .put("timestamp", timestamp())
            .put("date",      date())
            .put("time",      time())
            .put("uuid",      uuid())
            .put("varint",    varint())
            .put("timeuuid",  timeuuid())
            .put("tinyint",   tinyint())
            .put("smallint",  smallint())
            .build();

    static DataType parse(String toParse, Metadata metadata, String currentKeyspace, boolean frozen) {

        Parser parser = new Parser(toParse, 0);
        String type = parser.parseTypeName();

        DataType nativeType = NATIVE_TYPES_MAP.get(type.toLowerCase());
        if (nativeType != null)
            return nativeType;

        if (type.equalsIgnoreCase(LIST)) {
            List<String> parameters = parser.parseTypeParameters();
            if (parameters.size() != 1)
                throw new DriverInternalError(String.format("Excepting single parameter for list, got %s", parameters));
            DataType elementType = parse(parameters.get(0), metadata, currentKeyspace, false);
            return list(elementType, frozen);
        }

        if (type.equalsIgnoreCase(SET)) {
            List<String> parameters = parser.parseTypeParameters();
            if (parameters.size() != 1)
                throw new DriverInternalError(String.format("Excepting single parameter for set, got %s", parameters));
            DataType elementType = parse(parameters.get(0), metadata, currentKeyspace, false);
            return set(elementType, frozen);
        }

        if (type.equalsIgnoreCase(MAP)) {
            List<String> parameters = parser.parseTypeParameters();
            if (parameters.size() != 2)
                throw new DriverInternalError(String.format("Excepting two parameters for map, got %s", parameters));
            DataType keyType = parse(parameters.get(0), metadata, currentKeyspace, false);
            DataType valueType = parse(parameters.get(1), metadata, currentKeyspace, false);
            return map(keyType, valueType, frozen);
        }

        if (type.equalsIgnoreCase(FROZEN)) {
            List<String> parameters = parser.parseTypeParameters();
            if (parameters.size() != 1)
                throw new DriverInternalError(String.format("Excepting single parameter for frozen keyword, got %s", parameters));
            return parse(parameters.get(0), metadata, currentKeyspace, true);
        }

        if (frozen)
            logger.warn("Got frozen keyword for something else than a collection, "
                + "this driver version might be too old for your version of Cassandra");

        if (type.equalsIgnoreCase(TUPLE)) {
            List<String> rawTypes = parser.parseTypeParameters();
            List<DataType> types = new ArrayList<DataType>(rawTypes.size());
            for (String rawType : rawTypes) {
                types.add(parse(rawType, metadata, currentKeyspace, false));
            }
            return metadata.newTupleType(types);
        }

        // return a custom type for the special empty type
        // so that it gets detected later on, see TableMetadata
        if(type.equalsIgnoreCase(EMPTY))
            return custom(type);

        // Try UDTs
        KeyspaceMetadata keyspace = metadata.getKeyspace(escapeId(currentKeyspace));
        if (keyspace != null) {
            UserType userType = keyspace.getUserType(escapeId(type));
            if(userType != null)
                return userType;
        }

        // should only happen when we race with the UDT creation
        logger.warn("Could not resolve type: {}", type);
        return custom(type);

    }

    private static class Parser {

        private final String str;

        private int idx;

        Parser(String str, int idx) {
            this.str = str;
            this.idx = idx;
        }

        String parseTypeName() {
            idx = skipSpaces(str, idx);
            return readNextIdentifier();
        }

        List<String> parseTypeParameters() {
            List<String> list = new ArrayList<String>();

            if (isEOS())
                return list;

            skipBlankAndComma();

            if (str.charAt(idx) != '<')
                throw new IllegalStateException();

            ++idx; // skipping '<'

            while (skipBlankAndComma()) {
                if (str.charAt(idx) == '>') {
                    ++idx;
                    return list;
                }

                try {
                    String name = parseTypeName();
                    String args = readRawTypeParameters();
                    list.add(name + args);
                } catch (DriverInternalError e) {
                    DriverInternalError ex = new DriverInternalError(String.format("Exception while parsing '%s' around char %d", str, idx));
                    ex.initCause(e);
                    throw ex;
                }
            }
            throw new DriverInternalError(String.format("Syntax error parsing '%s' at char %d: unexpected end of string", str, idx));
        }

        // left idx positioned on the character stopping the read
        private String readNextIdentifier() {
            int i = idx;
            while (!isEOS() && isIdentifierChar(str.charAt(idx)))
                ++idx;

            return str.substring(i, idx);
        }

        // Assumes we have just read a type name and read it's potential arguments
        // blindly. I.e. it assume that either parsing is done or that we're on a '<'
        // and this reads everything up until the corresponding closing '>'. It
        // returns everything read, including the enclosing brackets.
        private String readRawTypeParameters() {
            idx = skipSpaces(str, idx);

            if (isEOS() || str.charAt(idx) == '>' || str.charAt(idx) == ',')
                return "";

            if (str.charAt(idx) != '<')
                throw new IllegalStateException(String.format("Expecting char %d of %s to be '<' but '%c' found", idx, str, str.charAt(idx)));

            int i = idx;
            int open = 1;
            while (open > 0) {
                ++idx;

                if (isEOS())
                    throw new IllegalStateException("Non closed angle brackets");

                if (str.charAt(idx) == '<') {
                    open++;
                } else if (str.charAt(idx) == '>') {
                    open--;
                }
            }
            // we've stopped at the last closing ')' so move past that
            ++idx;
            return str.substring(i, idx);
        }

        // skip all blank and at best one comma, return true if there not EOS
        private boolean skipBlankAndComma() {
            boolean commaFound = false;
            while (!isEOS()) {
                int c = str.charAt(idx);
                if (c == ',') {
                    if (commaFound)
                        return true;
                    else
                        commaFound = true;
                } else if (!isBlank(c)) {
                    return true;
                }
                ++idx;
            }
            return false;
        }

        private boolean isEOS() {
            return idx >= str.length();
        }

        @Override
        public String toString() {
            return str.substring(0, idx) + "[" + (idx == str.length() ? "" : str.charAt(idx)) + "]" + str.substring(idx+1);
        }
    }
}
