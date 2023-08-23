package org.apache.cassandra.index.sai.v1;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;

import static junit.framework.TestCase.assertEquals;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;
import static org.apache.cassandra.index.sai.LongVectorTest.randomVector;
import static org.apache.cassandra.index.sai.SAITester.waitForIndexQueryable;


public class StorageAttachedIndexTest
{
    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tab";
    private static final int DIMENSION = 3;
    private static final int numSSTables = 2;
    private static final int N = 10;
    private ColumnFamilyStore cfs;
    private SingleColumnRestriction.AnnRestriction testRestriction;
    private StorageAttachedIndex sai;

    @Before
    public void setup() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization(() -> {
            Config config = DatabaseDescriptor.loadConfig();
            config.partitioner = Murmur3Partitioner.class.getName();
            return config;
        });

        SchemaLoader.prepareServer();
        Gossiper.instance.maybeInitializeLocalState(0);
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1));
        QueryProcessor.executeInternal(String.format("CREATE TABLE %s.%s (key int primary key, value vector<float, %s>)", KEYSPACE, TABLE, DIMENSION));
        QueryProcessor.executeInternal(String.format("CREATE CUSTOM INDEX ON %s.%s(value) USING 'StorageAttachedIndex' WITH OPTIONS = { 'similarity_function': 'dot_product' }", KEYSPACE, TABLE));
        waitForIndexQueryable(KEYSPACE, TABLE);
        var keys = IntStream.range(0, N).boxed().collect(Collectors.toList());
        Collections.shuffle(keys);

        cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
        for (int i = 0; i < numSSTables ; i++)
        {
            for (int j = 0; j < N; j++)
            {
                QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (key, value) VALUES (?, ?)", KEYSPACE, TABLE), i, randomVector(DIMENSION));
            }
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        }

        TableMetadata tableMetadata = cfs.metadata();
        String columnName = "value";
        ColumnIdentifier columnIdentifier = new ColumnIdentifier(columnName, true);
        ColumnMetadata columnDef = tableMetadata.getExistingColumn(columnIdentifier);
        if (!(columnDef.type instanceof VectorType))
            throw invalidRequest("ANN is only supported against DENSE FLOAT32 columns");

        List<ByteBuffer> byteBufferList = new ArrayList<>();

        byteBufferList.add(ByteBuffer.wrap(new byte[]{1, 2, 3}));
        byteBufferList.add(ByteBuffer.wrap(new byte[]{4, 5, 6}));
        byteBufferList.add(ByteBuffer.wrap(new byte[]{120, 110, 90}));

        Term terms = new Lists.Value(byteBufferList);

        testRestriction = new SingleColumnRestriction.AnnRestriction(columnDef, terms);

        sai = (StorageAttachedIndex) cfs.getIndexManager().getIndexByName(String.format("%s_value_idx", TABLE));
    }

    @Test
    public void testOrderResults() {
        ResultSet.ResultMetadata resultMetadata = new ResultSet.ResultMetadata(new ArrayList<>(cfs.metadata.get().columns()));
        ResultSet resultSet = new ResultSet(resultMetadata);
        QueryOptions queryOptions = QueryOptions.DEFAULT;

        List<List<ByteBuffer>> rows = new ArrayList<>();
        List<ByteBuffer> row1 = new ArrayList<>();
        row1.add(ByteBuffer.wrap(new byte[]{1}));
        rows.add(row1);
        List<ByteBuffer> row2 = new ArrayList<>();
        row2.add(ByteBuffer.wrap(new byte[]{3}));
        rows.add(row2);
        List<ByteBuffer> row3 = new ArrayList<>();
        row3.add(ByteBuffer.wrap(new byte[]{2}));
        rows.add(row3);

        resultSet.rows = rows;

        SelectStatement selectStatementInstance = (SelectStatement) QueryProcessor.prepareInternal("SELECT key, value FROM " + KEYSPACE + '.' + TABLE).statement;
        selectStatementInstance.orderResults(resultSet, queryOptions);

        List<List<ByteBuffer>> sortedRows = resultSet.rows;

        Comparator<List<ByteBuffer>> descendingComparator = (o1, o2) -> {
            ByteBuffer value1 = o1.get(0);
            ByteBuffer value2 = o2.get(0);
            return value2.compareTo(value1);
        };

        Collections.sort(rows, descendingComparator);

        for (int i = 0; i < sortedRows.size(); i++) {
            List<ByteBuffer> expectedRow = rows.get(i);
            List<ByteBuffer> actualRow = sortedRows.get(i);
            assertEquals(expectedRow, actualRow);
        }
    }
}
