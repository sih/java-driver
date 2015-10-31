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
package com.datastax.driver.extras.codecs.json;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Map;

import javax.json.*;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;

/**
 * A JSON codec that uses the
 * <a href="https://jcp.org/en/jsr/detail?id=353">Java API for JSON processing</a>
 * to perform serialization and deserialization of JSON structures.
 * <p>
 * More specifically, this codec maps an arbitrary {@link JsonStructure} to
 * a CQL {@code varchar} column.
 * <p>
 * Note that this codec handles the Java type {@link JsonStructure}.
 * It is therefore required that values are set and retrieved using that exact Java type;
 * users should manually downcast to either {@link JsonObject} or {@link JsonArray},
 * as in the example below:
 * <pre>{@code
 *
 * // setting values
 * JsonObject myObject = ...
 * PreparedStatement ps = ...
 * // set values using JsonStructure
 * BoundStatement bs = ps.bind().set(1, myObject, JsonStructure.class);
 *
 * // retrieving values
 * Row row = session.execute(bs).one();
 * // use JsonStructure to retrieve values
 * JsonStructure json = row.get(0, JsonStructure.class);
 * if (json instanceof JsonObject) {
 *     myObject = (JsonObject) json;
 *     ...
 * }
 * }</pre>
 *
 */
public class Jsr353JsonCodec extends TypeCodec<JsonStructure> {

    private final JsonReaderFactory readerFactory;

    private final JsonWriterFactory writerFactory;

    public Jsr353JsonCodec() {
        this(null);
    }

    public Jsr353JsonCodec(Map<String, ?> config) {
        super(DataType.varchar(), JsonStructure.class);
        readerFactory = Json.createReaderFactory(config);
        writerFactory = Json.createWriterFactory(config);
    }

    @Override
    public ByteBuffer serialize(JsonStructure value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (value == null)
            return null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            JsonWriter writer = writerFactory.createWriter(baos);
            writer.write(value);
            return ByteBuffer.wrap(baos.toByteArray());
        } catch (JsonException e) {
            throw new InvalidTypeException(e.getMessage(), e);
        } finally {
            try {
                baos.close();
            } catch (IOException e) {
                // cannot happen
                throw new InvalidTypeException(e.getMessage(), e);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public JsonStructure deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (bytes == null)
            return null;
        ByteArrayInputStream bais = new ByteArrayInputStream(Bytes.getArray(bytes));
        try {
            JsonReader reader = readerFactory.createReader(bais);
            return reader.read();
        } catch (JsonException e) {
            throw new InvalidTypeException(e.getMessage(), e);
        } finally {
            try {
                bais.close();
            } catch (IOException e) {
                // cannot happen
                throw new InvalidTypeException(e.getMessage(), e);
            }
        }
    }

    @Override
    public String format(JsonStructure value) throws InvalidTypeException {
        if (value == null)
            return "NULL";
        String json;
        StringWriter sw = new StringWriter();
        try {
            JsonWriter writer = writerFactory.createWriter(sw);
            writer.write(value);
            json = sw.toString();
        } catch (JsonException e) {
            throw new InvalidTypeException(e.getMessage(), e);
        } finally {
            try {
                sw.close();
            } catch (IOException e) {
                // cannot happen
                throw new InvalidTypeException(e.getMessage(), e);
            }
        }
        return '\'' + json.replace("\'", "''") + '\'';
    }

    @Override
    @SuppressWarnings("unchecked")
    public JsonStructure parse(String value) throws InvalidTypeException {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;
        if (value.charAt(0) != '\'' || value.charAt(value.length() - 1) != '\'')
            throw new InvalidTypeException("JSON strings must be enclosed by single quotes");
        String json = value.substring(1, value.length() - 1).replace("''", "'");
        StringReader sr = new StringReader(json);
        try {
            JsonReader reader = readerFactory.createReader(sr);
            return reader.read();
        } catch (JsonException e) {
            throw new InvalidTypeException(e.getMessage(), e);
        } finally {
            sr.close();
        }
    }

}
