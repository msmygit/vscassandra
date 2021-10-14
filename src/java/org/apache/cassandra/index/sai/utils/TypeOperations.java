/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.utils;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;

import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public interface TypeOperations
{
    boolean isValid(ByteBuffer term, AbstractType<?> validator);

    boolean supportsRounding(AbstractType<?> type);

    ByteBuffer min(ByteBuffer a, ByteBuffer b, AbstractType<?> type);

    ByteBuffer max(ByteBuffer a, ByteBuffer b, AbstractType<?> type);

    ByteComparable min(ByteComparable a, ByteComparable b);

    ByteComparable max(ByteComparable a, ByteComparable b);

    int fixedSizeOf(AbstractType<?> type);

    AbstractType<?> cellValueType(Pair<ColumnMetadata, IndexTarget.Type> target);

    String getString(ByteBuffer value, AbstractType<?> type);

    ByteBuffer fromString(String value, AbstractType<?> type);

    ByteSource asComparableBytes(ByteBuffer value, AbstractType<?> type, ByteComparable.Version version);

    void toComparableBytes(ByteBuffer value, AbstractType<?> type, byte[] bytes);

    ByteBuffer encode(ByteBuffer value, AbstractType<?> type);

    int compare(ByteBuffer b1, ByteBuffer b2, AbstractType<?> type);

    int comparePostFilter(Expression.Value requestedValue, Expression.Value columnValue, AbstractType<?> type);

    Iterator<ByteBuffer> collectionIterator(AbstractType<?> validator,
                                            ComplexColumnData cellData,
                                            Pair<ColumnMetadata, IndexTarget.Type> target,
                                            int nowInSecs);

    Comparator<ByteBuffer> comparator(AbstractType<?> type);

    ByteBuffer encodeBigInteger(ByteBuffer value);

    boolean isLiteral(AbstractType<?> type);

    boolean isUTF8OrAscii(AbstractType<?> type);

    boolean isCompositeOrFrozen(AbstractType<?> type);

    boolean isFrozen(AbstractType<?> type);

    boolean isFrozenCollection(AbstractType<?> type);

    boolean isNonFrozenCollection(AbstractType<?> type);

    boolean isIn(AbstractType<?> type, Set<AbstractType<?>> types);

    boolean isComposite(AbstractType<?> type);

    ByteBuffer encodeDecimal(ByteBuffer value);
}
