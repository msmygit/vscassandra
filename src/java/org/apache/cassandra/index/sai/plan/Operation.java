/*
 * All changes to the original code are Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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

package org.apache.cassandra.index.sai.plan;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.disk.IndexSearcherContext;
import org.apache.cassandra.index.sai.disk.MergePostingList;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PostingListRangeIterator;
import org.apache.cassandra.index.sai.disk.PostingListRangeIterator2;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.utils.ConjunctionPostingList;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RangeUnionIterator;
import org.apache.cassandra.index.sai.utils.TermIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.ListSerializer;
import org.apache.cassandra.transport.ProtocolVersion;

public class Operation
{
    public enum OperationType
    {
        AND, OR;

        public boolean apply(boolean a, boolean b)
        {
            switch (this)
            {
                case OR:
                    return a | b;

                case AND:
                    return a & b;

                default:
                    throw new AssertionError();
            }
        }
    }

    @VisibleForTesting
    protected static ListMultimap<ColumnMetadata, Expression> analyzeGroup(QueryController controller,
                                                                           OperationType op,
                                                                           List<RowFilter.Expression> expressions)
    {
        ListMultimap<ColumnMetadata, Expression> analyzed = ArrayListMultimap.create();

        // sort all of the expressions in the operation by name and priority of the logical operator
        // this gives us an efficient way to handle inequality and combining into ranges without extra processing
        // and converting expressions from one type to another.
        expressions.sort((a, b) -> {
            int cmp = a.column().compareTo(b.column());
            return cmp == 0 ? -Integer.compare(getPriority(a.operator()), getPriority(b.operator())) : cmp;
        });

        for (final RowFilter.Expression e : expressions)
        {
            IndexContext indexContext = controller.getContext(e);
            List<Expression> perColumn = analyzed.get(e.column());

            AbstractAnalyzer.AnalyzerFactory analyzerFactory = indexContext.getQueryAnalyzerFactory();
            AbstractAnalyzer analyzer = analyzerFactory.create();
            try
            {
                analyzer.reset(e.getIndexValue().duplicate());

                // EQ/LIKE_*/NOT_EQ can have multiple expressions e.g. text = "Hello World",
                // becomes text = "Hello" OR text = "World" because "space" is always interpreted as a split point (by analyzer),
                // CONTAINS/CONTAINS_KEY are always treated as multiple expressions since they currently only targetting
                // collections, NOT_EQ is made an independent expression only in case of pre-existing multiple EQ expressions, or
                // if there is no EQ operations and NOT_EQ is met or a single NOT_EQ expression present,
                // in such case we know exactly that there would be no more EQ/RANGE expressions for given column
                // since NOT_EQ has the lowest priority.
                boolean isMultiExpression = false;
                switch (e.operator())
                {
                    case EQ:
                        // EQ operator will always be a multiple expression because it is being used by
                        // map entries
                        isMultiExpression = indexContext.isNonFrozenCollection();
                        break;

                    case CONTAINS:
                    case CONTAINS_KEY:
                    case LIKE_PREFIX:
                    case LIKE_MATCHES:
                        isMultiExpression = true;
                        break;

                    case NEQ:
                        isMultiExpression = (perColumn.size() == 0 || perColumn.size() > 1
                                             || (perColumn.size() == 1 && perColumn.get(0).getOp() == Expression.Op.NOT_EQ));
                        break;
                }
                if (isMultiExpression)
                {
                    while (analyzer.hasNext())
                    {
                        final ByteBuffer token = analyzer.next();
                        perColumn.add(new Expression(indexContext, controller.getIndexFeatureSet()).add(e.operator(), token.duplicate()));
                    }
                }
                else
                // "range" or not-equals operator, combines both bounds together into the single expression,
                // if operation of the group is AND, otherwise we are forced to create separate expressions,
                // not-equals is combined with the range iff operator is AND.
                {
                    Expression range;
                    if (perColumn.size() == 0 || op != OperationType.AND)
                    {
                        perColumn.add((range = new Expression(indexContext, controller.getIndexFeatureSet())));
                    }
                    else
                    {
                        range = Iterables.getLast(perColumn);
                    }

                    if (!TypeUtil.instance.isLiteral(indexContext.getValidator()))
                    {
                        range.add(e.operator(), e.getIndexValue().duplicate());
                    }
                    else
                    {
                        while (analyzer.hasNext())
                        {
                            ByteBuffer term = analyzer.next();
                            range.add(e.operator(), term.duplicate());
                        }
                    }
                }
            }
            finally
            {
                analyzer.end();
            }
        }

        return analyzed;
    }

    private static int getPriority(org.apache.cassandra.cql3.Operator op)
    {
        switch (op)
        {
            case EQ:
            case CONTAINS:
            case CONTAINS_KEY:
                return 5;

            case LIKE_PREFIX:
            case LIKE_MATCHES:
                return 4;

            case GTE:
            case GT:
                return 3;

            case LTE:
            case LT:
                return 2;

            case NEQ:
                return 1;

            default:
                return 0;
        }
    }

    static RangeIterator buildIterator(QueryController controller) throws IOException
    {
        Node node = Node.buildTree(controller.filterOperation()).analyzeTree(controller);

        List<Expression> expressions = new ArrayList<>();

        Operation.OperationType op = null;

        if (node instanceof AndNode)
        {
            op = OperationType.AND;

            AndNode andNode = (AndNode)node;

            expressions.addAll(andNode.expressionMap.values());
//
//            System.out.println("AndNode num children="+andNode.children().size());
//            for (Node childNode : andNode.children())
//            {
//                assert childNode.analyzed;
//
//                expressions.add(childNode.expression());
//                //assert childNode.expressionMap != null;
//
//                //expressions.addAll(childNode.expressionMap.values());
//            }
        }
        else if (node instanceof OrNode)
        {

        }

        //return Node.buildTree(controller.filterOperation()).analyzeTree(controller).rangeIterator(controller);

        System.out.println("expressions="+expressions);

        final Multimap<SSTableReader.UniqueIdentifier, QueryController.ExpressionSSTableIndex> map = controller.getIndexes(op, expressions);

        System.out.println("map="+map);

        List<RangeIterator> topLevelRangeIterators = new ArrayList<>();

        QueryContext queryContext = new QueryContext();

        // for each sstable, create a range iterator
        for (Map.Entry<SSTableReader.UniqueIdentifier, Collection<QueryController.ExpressionSSTableIndex>> entry : map.asMap().entrySet())
        {
            List<PostingList> sstablePostings = new ArrayList<>();
            PrimaryKeyMap primaryKeyMap = null;
            Set<SSTableIndex> ssTableIndices = new HashSet<>();
            PrimaryKey minKey = null, maxKey = null;
            SSTableReader ssTableReader = null;

            for (QueryController.ExpressionSSTableIndex e : entry.getValue())
            {
                List<PostingList.PeekablePostingList> expressionPostingsLists = e.ssTableIndex.searchPostings(e.expression, controller.mergeRange(), null);
                PostingList expressionPostings = MergePostingList.merge(expressionPostingsLists);

                if (ssTableReader == null)
                {
                    ssTableReader = e.ssTableIndex.getSSTable();
                }

                if (minKey == null)
                {
                    minKey = e.ssTableIndex.minKey();
                }
                else
                {
                    minKey = e.ssTableIndex.minKey().compareTo(minKey) < 0 ? e.ssTableIndex.minKey() : minKey;
                }
                if (maxKey == null)
                {
                    maxKey = e.ssTableIndex.maxKey();
                }
                else
                {
                    maxKey = e.ssTableIndex.maxKey().compareTo(maxKey) > 0 ? e.ssTableIndex.maxKey() : maxKey;
                }

                sstablePostings.add(expressionPostings);

                if (primaryKeyMap == null)
                {
                    primaryKeyMap = e.ssTableIndex.getSSTableContext().primaryKeyMapFactory.newPerSSTablePrimaryKeyMap(null);
                }
                ssTableIndices.add(e.ssTableIndex);
            }
            ConjunctionPostingList sstableAndPostings = new ConjunctionPostingList(sstablePostings);

            PostingList.PeekablePostingList sstablePeekablePostingList = sstableAndPostings.peekable();

            final long firstRowId = sstablePeekablePostingList.peek();
            if (firstRowId == PostingList.END_OF_STREAM)
            {
                // this sstable has no matches
                // close all related resources
                sstableAndPostings.close();
                ssTableIndices.forEach(i -> i.release());
                continue; // to the next sstable iterator
            }

            SSTableQueryContext ssTableQueryContext = queryContext.getSSTableQueryContext(ssTableReader);

            IndexSearcherContext indexSearcherContext = new IndexSearcherContext(minKey,
                                                                                 maxKey,
                                                                                 ssTableQueryContext,
                                                                                 sstablePeekablePostingList);

            PostingListRangeIterator2 sstableAndRangeIterator = new PostingListRangeIterator2(primaryKeyMap,
                                                                                              indexSearcherContext);

            TermIterator termIterator = new TermIterator(sstableAndRangeIterator, ssTableIndices, new QueryContext());

            topLevelRangeIterators.add(termIterator);
        }

        RangeIterator topLevelUnionIterator = RangeUnionIterator.build(topLevelRangeIterators);

        return topLevelUnionIterator;
    }

    static FilterTree buildFilter(QueryController controller)
    {
        return Node.buildTree(controller.filterOperation()).buildFilter(controller);
    }

    public static abstract class Node
    {
        ListMultimap<ColumnMetadata, Expression> expressionMap;
        boolean analyzed = false;

        boolean canFilter()
        {
            return (expressionMap != null && !expressionMap.isEmpty()) || !children().isEmpty() ;
        }

        List<Node> children()
        {
            return Collections.emptyList();
        }

        void add(Node child)
        {
            throw new UnsupportedOperationException();
        }

        RowFilter.Expression expression()
        {
            throw new UnsupportedOperationException();
        }

        abstract void analyze(List<RowFilter.Expression> expressionList, QueryController controller);

        abstract FilterTree filterTree();

        //abstract RangeIterator rangeIterator(QueryController controller);

        static Node buildTree(RowFilter.FilterElement filterOperation)
        {
            OperatorNode node = filterOperation.isDisjunction() ? new OrNode() : new AndNode();
            for (RowFilter.Expression expression : filterOperation.expressions())
                node.add(buildExpression(expression));
            for (RowFilter.FilterElement child : filterOperation.children())
                node.add(buildTree(child));
            return node;
        }

        static Node buildExpression(RowFilter.Expression expression)
        {
            if (expression.operator() == Operator.IN)
            {
                OperatorNode node = new OrNode();
                int size = ListSerializer.readCollectionSize(expression.getIndexValue(), ByteBufferAccessor.instance, ProtocolVersion.V3);
                int offset = ListSerializer.sizeOfCollectionSize(size, ProtocolVersion.V3);
                for (int index = 0; index < size; index++)
                {
                    node.add(new ExpressionNode(new RowFilter.SimpleExpression(expression.column(),
                                                                               Operator.EQ,
                                                                               ListSerializer.readValue(expression.getIndexValue(),
                                                                                                        ByteBufferAccessor.instance,
                                                                                                        offset,
                                                                                                        ProtocolVersion.V3))));
                    offset += TypeSizes.INT_SIZE + ByteBufferAccessor.instance.getInt(expression.getIndexValue(), offset);
                }
                return node;
            }
            else
                return new ExpressionNode(expression);
        }

        Node analyzeTree(QueryController controller)
        {
            List<RowFilter.Expression> expressionList = new ArrayList<>();
            doTreeAnalysis(this, expressionList, controller);
            if (!expressionList.isEmpty())
                this.analyze(expressionList, controller);
            return this;
        }

        void doTreeAnalysis(Node node, List<RowFilter.Expression> expressions, QueryController controller)
        {
            if (node.children().isEmpty())
                expressions.add(node.expression());
            else
            {
                List<RowFilter.Expression> expressionList = new ArrayList<>();
                for (Node child : node.children())
                    doTreeAnalysis(child, expressionList, controller);
                node.analyze(expressionList, controller);
            }
        }

        FilterTree buildFilter(QueryController controller)
        {
            analyzeTree(controller);
            FilterTree tree = filterTree();
            for (Node child : children())
                if (child.canFilter())
                    tree.addChild(child.buildFilter(controller));
            return tree;
        }
    }

    public static abstract class OperatorNode extends Node
    {
        List<Node> children = new ArrayList<>();

        @Override
        public List<Node> children()
        {
            return children;
        }

        @Override
        public void add(Node child)
        {
            children.add(child);
        }
    }

    public static class AndNode extends OperatorNode
    {
        @Override
        public void analyze(List<RowFilter.Expression> expressionList, QueryController controller)
        {
            expressionMap = analyzeGroup(controller, OperationType.AND, expressionList);
            analyzed = true;
        }

        @Override
        FilterTree filterTree()
        {
            return new FilterTree(OperationType.AND, expressionMap);
        }

//        @Override
//        RangeIterator rangeIterator(QueryController controller)
//        {
//            RangeIterator.Builder builder = controller.getIndexes(OperationType.AND, expressionMap.values());
//            for (Node child : children)
//            {
//                boolean canFilter = child.canFilter();
//                if (canFilter)
//                    builder.add(child.rangeIterator(controller));
//            }
//            return builder.build();
//        }
    }

    public static class OrNode extends OperatorNode
    {
        @Override
        public void analyze(List<RowFilter.Expression> expressionList, QueryController controller)
        {
            expressionMap = analyzeGroup(controller, OperationType.OR, expressionList);
        }

        @Override
        FilterTree filterTree()
        {
            return new FilterTree(OperationType.OR, expressionMap);
        }

//        @Override
//        RangeIterator rangeIterator(QueryController controller)
//        {
//            RangeIterator.Builder builder = controller.getIndexes(OperationType.OR, expressionMap.values());
//            for (Node child : children)
//                if (child.canFilter())
//                    builder.add(child.rangeIterator(controller));
//            return builder.build();
//        }
    }

    public static class ExpressionNode extends Node
    {
        RowFilter.Expression expression;

        @Override
        public void analyze(List<RowFilter.Expression> expressionList, QueryController controller)
        {
            expressionMap = analyzeGroup(controller, OperationType.AND, expressionList);
        }

        @Override
        FilterTree filterTree()
        {
            return new FilterTree(OperationType.AND, expressionMap);
        }

        public ExpressionNode(RowFilter.Expression expression)
        {
            this.expression = expression;
        }

        @Override
        public RowFilter.Expression expression()
        {
            return expression;
        }

//        @Override
//        RangeIterator rangeIterator(QueryController controller)
//        {
//            assert canFilter();
//            return controller.getIndexes(OperationType.AND, expressionMap.values()).build();
//        }
    }
}
