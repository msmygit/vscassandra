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
package org.apache.cassandra.cql3;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.management.JMX;
import javax.management.ObjectName;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import io.reactivex.Single;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.UnauthorizedException;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.auth.*;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.ThreadAwareSecurityManager;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.service.*;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.*;

/**
 * Base class for CQL tests.
 * 
 * TODO: this is now used as a test utility outside of CQL tests, we should really refactor it into a utility class
 * for things like setting up the network, inserting data, etc.
 */
public abstract class CQLTester
{
    protected static final Logger logger = LoggerFactory.getLogger(CQLTester.class);

    public static final String KEYSPACE = "cql_test_keyspace";
    public static final String KEYSPACE_PER_TEST = "cql_test_keyspace_alt";
    protected static final boolean USE_PREPARED_VALUES = Boolean.valueOf(System.getProperty("cassandra.test.use_prepared", "true"));
    protected static final boolean REUSE_PREPARED = Boolean.valueOf(System.getProperty("cassandra.test.reuse_prepared", "true"));
    protected static final long ROW_CACHE_SIZE_IN_MB = Integer.valueOf(System.getProperty("cassandra.test.row_cache_size_in_mb", "0"));
    private static final AtomicInteger seqNumber = new AtomicInteger();
    protected static final ByteBuffer TOO_BIG = ByteBuffer.allocate(FBUtilities.MAX_UNSIGNED_SHORT + 1024);
    public static final String DATA_CENTER = "datacenter1";
    public static final String RACK1 = "rack1";
    private static final User SUPER_USER = new User("cassandra", "cassandra");

    private static NativeTransportService server;

    protected static final int nativePort;
    protected static final InetAddress nativeAddr;
    private static final Map<Pair<User, ProtocolVersion>, Cluster> clusters = new HashMap<>();
    private static final Map<Pair<User, ProtocolVersion>, Session> sessions = new HashMap<>();

    private enum ServerStatus
    {
        NONE,
        PENDING,
        INITIALIZED,
        FAILED
    }

    private static AtomicReference<ServerStatus> serverStatus = new AtomicReference<>(ServerStatus.NONE);
    private static CountDownLatch serverReady = new CountDownLatch(1);

    public static final List<ProtocolVersion> PROTOCOL_VERSIONS = new ArrayList<>();

    private static final String CREATE_INDEX_NAME_REGEX = "(\\s*(\\w*|\"\\w*\")\\s*)";
    private static final String CREATE_INDEX_REGEX = String.format("\\A\\s*CREATE(?:\\s+CUSTOM)?\\s+INDEX" +
                                                                   "(?:\\s+IF\\s+NOT\\s+EXISTS)?\\s*" +
                                                                   "%s?\\s*ON\\s+(%<s\\.)?%<s\\s*" +
                                                                   "(\\((?:\\s*\\w+\\s*\\()?%<s\\))?",
                                                                   CREATE_INDEX_NAME_REGEX);
    private static final Pattern CREATE_INDEX_PATTERN = Pattern.compile(CREATE_INDEX_REGEX, Pattern.CASE_INSENSITIVE);

    /** Return the current server version if supported by the driver, else
     * the latest that is supported.
     *
     * @return - the preferred versions that is also supported by the driver
     */
    public static final ProtocolVersion getDefaultVersion()
    {
        return PROTOCOL_VERSIONS.contains(ProtocolVersion.CURRENT)
               ? ProtocolVersion.CURRENT
               : PROTOCOL_VERSIONS.get(PROTOCOL_VERSIONS.size() - 1);
    }

    static
    {
        // The latest versions might not be supported yet by the java driver
        for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
        {
            try
            {
                com.datastax.driver.core.ProtocolVersion.fromInt(version.asInt());
                PROTOCOL_VERSIONS.add(version);
            }
            catch (IllegalArgumentException e)
            {
                logger.warn("Protocol Version {} not supported by java driver", version);
            }
        }

        // Register an EndpointSnitch which returns fixed values for test.
        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            @Override public String getRack(InetAddress endpoint) { return RACK1; }
            @Override public String getDatacenter(InetAddress endpoint) { return DATA_CENTER; }
            @Override public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2) { return 0; }
        });

        // call after setting the snitch so that DD.applySnitch will not set the snitch to the conf snitch. On slow
        // Jenkins machines, DynamicEndpointSnitch::updateScores may initialize SS with a wrong TMD before we have a
        // chance to change the partitioner, so changing the DD snitch is not enough, we must prevent the config snitch
        // from being initialized.
        DatabaseDescriptor.daemonInitialization();

        nativeAddr = DatabaseDescriptor.getNativeTransportAddress();
        nativePort = DatabaseDescriptor.getNativeTransportPort();
    }

    public static ResultMessage lastSchemaChangeResult;

    private List<String> keyspaces = new ArrayList<>();
    private List<String> tables = new ArrayList<>();
    private List<String> types = new ArrayList<>();
    private List<String> functions = new ArrayList<>();
    private List<String> aggregates = new ArrayList<>();
    private User user;

    // We don't use USE_PREPARED_VALUES in the code below so some test can foce value preparation (if the result
    // is not expected to be the same without preparation)
    private boolean usePrepared = USE_PREPARED_VALUES;
    private static boolean reusePrepared = REUSE_PREPARED;

    public static Runnable preJoinHook = () -> {};

    protected boolean usePrepared()
    {
        return usePrepared;
    }

    public static void prepareServer()
    {
        if (serverStatus.get() == ServerStatus.INITIALIZED)
            return;

        if (!serverStatus.compareAndSet(ServerStatus.NONE, ServerStatus.PENDING))
        {   // Once per-JVM is enough, the first test to execute gets to initialize the server,
            // unless sub-classes call this method from static initialization methods such as
            // requireNetwork(). Note that this method cannot be called by the base class setUp
            // static method because this would make it impossible to change the partitioner in
            // sub-classed. Normally we run tests sequentially but, to be safe, this ensures
            // that if 2 tests execute in parallel, only one test initializes the server and the
            // other test waits.

            // if we couldn't set the server status to pending because another test previously failed
            // then abort without waiting
            if (serverStatus.get() == ServerStatus.FAILED)
                fail("A previous test failed to initialize the server");

            // if we've raced with another test then wait for a sufficient amount of time (Jenkins is quite slow)
            Uninterruptibles.awaitUninterruptibly(serverReady, 3, TimeUnit.MINUTES);

            // now check again if the test that ran in parallel failed
            if (serverStatus.get() == ServerStatus.FAILED)
                fail("A previous test failed to initialize the server: " + serverStatus);

            // otherwise the server must have been initialized
//            assertTrue("Unexpected server status: " + serverStatus, serverStatus.get() == ServerStatus.INITIALIZED);
            return;
        }

//        assertTrue("Unexpected server status: " + serverStatus, serverStatus.get() == ServerStatus.PENDING);

        try
        {
            // Not really required, but doesn't hurt either
            TPC.ensureInitialized();

            // Cleanup first
            try
            {
                cleanupAndLeaveDirs();
            }
            catch (IOException e)
            {
                logger.error("Failed to cleanup and recreate directories.");
                throw new RuntimeException(e);
            }

            // Some tests, e.g. OutOfSpaceTest, require the default handler installed by CassandraDaemon.
            if (Thread.getDefaultUncaughtExceptionHandler() == null)
                Thread.setDefaultUncaughtExceptionHandler((t, e) -> logger.error("Fatal exception in thread " + t, e));

            ThreadAwareSecurityManager.install();

            Keyspace.setInitialized();
            TPCUtils.blockingAwait(SystemKeyspace.persistLocalMetadata());

            SystemKeyspace.finishStartupBlocking();
            TPCUtils.blockingAwait(SystemKeyspace.persistLocalMetadata());
            StorageService.instance.populateTokenMetadata();

            preJoinHook.run();

            //TPC requires local vnodes to be generated so we need to
            //put the SS through join.
            StorageService.instance.initServer();

            // clear the SS shutdown hook as it makes the test take longer on Jenkins
            JVMStabilityInspector.removeShutdownHooks();

            Gossiper.instance.registerUpgradeBarrierListener();

            logger.info("Server initialized");
            boolean ret = serverStatus.compareAndSet(ServerStatus.PENDING, ServerStatus.INITIALIZED);
//            assertTrue("Unexpected server status: " + serverStatus, ret);
        }
        catch (Throwable t)
        {
            logger.error("Failed to initialize the server: {}", t.getMessage(), t);
            boolean ret = serverStatus.compareAndSet(ServerStatus.PENDING, ServerStatus.FAILED);
//            assertTrue("Unexpected server status: " + serverStatus, ret);
        }
        finally
        {
            // signal to any other waiting test that the server is ready
            serverReady.countDown();
        }
    }

    public static void cleanupAndLeaveDirs() throws IOException
    {
        // We need to stop and unmap all CLS instances prior to cleanup() or we'll get failures on Windows.
        CommitLog.instance.stopUnsafe(true);
        mkdirs();
        cleanup();
        mkdirs();
        CommitLog.instance.restartUnsafe();
    }

    public static void cleanup()
    {
        // clean up commitlog
        String[] directoryNames = { DatabaseDescriptor.getCommitLogLocation(), };
        for (String dirName : directoryNames)
        {
            File dir = new File(dirName);
            if (!dir.exists())
                throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
            FileUtils.deleteRecursive(dir);
        }

        File cdcDir = new File(DatabaseDescriptor.getCDCLogLocation());
        if (cdcDir.exists())
            FileUtils.deleteRecursive(cdcDir);

        cleanupSavedCaches();

        // clean up data directory which are stored as data directory/keyspace/data files
        for (String dirName : DatabaseDescriptor.getAllDataFileLocations())
        {
            File dir = new File(dirName);
            if (!dir.exists())
                throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
            FileUtils.deleteRecursive(dir);
        }
    }

    public static void mkdirs()
    {
        DatabaseDescriptor.createAllDirectories();
    }

    public static void cleanupSavedCaches()
    {
        File cachesDir = new File(DatabaseDescriptor.getSavedCachesLocation());

        if (!cachesDir.exists() || !cachesDir.isDirectory())
            return;

        FileUtils.delete(cachesDir.listFiles());
    }

    @BeforeClass
    public static void setUpClass()
    {
        if (ROW_CACHE_SIZE_IN_MB > 0)
            DatabaseDescriptor.setRowCacheSizeInMB(ROW_CACHE_SIZE_IN_MB);

        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @AfterClass
    public static void tearDownClass()
    {
        for (Session sess : sessions.values())
                sess.close();
        for (Cluster cl : clusters.values())
                cl.close();

        if (server != null)
            server.stop();

        // We use queryInternal for CQLTester so prepared statement will populate our internal cache (if reusePrepared is used; otherwise prepared
        // statements are not cached but re-prepared every time). So we clear the cache between test files to avoid accumulating too much.
        if (reusePrepared)
            QueryProcessor.clearInternalStatementsCache();
    }

    @Before
    public void beforeTest() throws Throwable
    {
        // this call is idempotent and will only prepare the server on the first call
        // we cannot prepare the server in setUpClass() because otherwise it would
        // be impossible to change the partitioner in sub-classes, e.g. SelectOrderedPartitionerTest
        prepareServer();

        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE_PER_TEST));
    }

    @After
    public void afterTest() throws Throwable
    {
        dropPerTestKeyspace();

        // Restore standard behavior in case it was changed
        usePrepared = USE_PREPARED_VALUES;
        reusePrepared = REUSE_PREPARED;

        final List<String> keyspacesToDrop = copy(keyspaces);
        final List<String> tablesToDrop = copy(tables);
        final List<String> typesToDrop = copy(types);
        final List<String> functionsToDrop = copy(functions);
        final List<String> aggregatesToDrop = copy(aggregates);
        keyspaces = null;
        tables = null;
        types = null;
        functions = null;
        aggregates = null;
        user = null;

        // We want to clean up after the test, but dropping a table is rather long so just do that asynchronously
        Executors.newSingleThreadExecutor().execute(new Runnable()
        {
            public void run()
            {
                try
                {
                    logger.debug("Dropping {} tables created in previous test", tablesToDrop.size());
                    for (int i = tablesToDrop.size() - 1; i >= 0; i--)
                        schemaChange(String.format("DROP TABLE IF EXISTS %s.%s", KEYSPACE, tablesToDrop.get(i)));

                    logger.debug("Dropping {} aggregate functions created in previous test", aggregatesToDrop.size());
                    for (int i = aggregatesToDrop.size() - 1; i >= 0; i--)
                        schemaChange(String.format("DROP AGGREGATE IF EXISTS %s", aggregatesToDrop.get(i)));

                    logger.debug("Dropping {} scalar functions created in previous test", functionsToDrop.size());
                    for (int i = functionsToDrop.size() - 1; i >= 0; i--)
                        schemaChange(String.format("DROP FUNCTION IF EXISTS %s", functionsToDrop.get(i)));

                    logger.debug("Dropping {} types created in previous test", typesToDrop.size());
                    for (int i = typesToDrop.size() - 1; i >= 0; i--)
                        schemaChange(String.format("DROP TYPE IF EXISTS %s.%s", KEYSPACE, typesToDrop.get(i)));

                    logger.debug("Dropping {} keyspaces created in previous test", keyspacesToDrop.size());
                    for (int i = keyspacesToDrop.size() - 1; i >= 0; i--)
                        schemaChange(String.format("DROP KEYSPACE IF EXISTS %s", keyspacesToDrop.get(i)));

                    // Dropping doesn't delete the sstables. It's not a huge deal but it's cleaner to cleanup after us
                    // Thas said, we shouldn't delete blindly before the TransactionLogs.SSTableTidier for the table we drop
                    // have run or they will be unhappy. Since those taks are scheduled on StorageService.tasks and that's
                    // mono-threaded, just push a task on the queue to find when it's empty. No perfect but good enough.

                    logger.debug("Awaiting sstable cleanup");
                    final CountDownLatch latch = new CountDownLatch(1);
                    ScheduledExecutors.nonPeriodicTasks.execute(new Runnable()
                    {
                        public void run()
                        {
                            latch.countDown();
                        }
                    });
                    if (!latch.await(2, TimeUnit.SECONDS))
                        logger.warn("TImes out waiting for shutdown");

                    logger.debug("Removing sstables");
                    removeAllSSTables(KEYSPACE, tablesToDrop);
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    protected static void requireAuthentication()
    {
        System.setProperty("cassandra.superuser_setup_delay_ms", "-1");

        DatabaseDescriptor.setAuthenticator(new PasswordAuthenticator());
        DatabaseDescriptor.setAuthManager(new AuthManager(new CassandraRoleManager(), new CassandraAuthorizer()));
    }

    public static void requireNetwork() throws ConfigurationException
    {
        if (server != null)
            return;

        prepareServer();

        SystemKeyspace.finishStartupBlocking();
        TPCUtils.blockingAwait(SystemKeyspace.persistLocalMetadata());
        StorageService.instance.populateTokenMetadata();
        StorageService.instance.initServer();
        Gossiper.instance.register(StorageService.instance);
        SchemaLoader.startGossiper();
        Gossiper.instance.maybeInitializeLocalState(1);

        Gossiper.instance.registerUpgradeBarrierListener();
        Gossiper.instance.clusterVersionBarrier.onLocalNodeReady();

        server = new NativeTransportService(nativeAddr, nativePort);
        server.start();
    }

    private static Cluster initClientCluster(User user, ProtocolVersion version)
    {
        Pair<User, ProtocolVersion> key = Pair.create(user, version);
        Cluster cluster = clusters.get(key);
        if (cluster != null)
            return cluster;

        Cluster.Builder builder = clusterBuilder(version);
        if (user != null)
            builder.withCredentials(user.username, user.password);
        cluster = builder.build();

        logger.info("Started Java Driver instance for protocol version {}", version);

        return cluster;
    }

    public static Cluster.Builder clusterBuilder()
    {
        return Cluster.builder()
                      .addContactPoints(nativeAddr)
                      .withPort(nativePort)
                      .withClusterName("Test Cluster")
                      .withNettyOptions(NettyOptions.DEFAULT_INSTANCE)
                      .withoutMetrics();
    }

    public static Cluster.Builder clusterBuilder(ProtocolVersion version)
    {
        Cluster.Builder builder = clusterBuilder();
        if (version.isBeta())
            builder = builder.allowBetaProtocolVersion();
        else
            builder = builder.withProtocolVersion(com.datastax.driver.core.ProtocolVersion.fromInt(version.asInt()));
        return builder;
    }

    public static Cluster createClientCluster(ProtocolVersion version, String clusterName, NettyOptions nettyOptions,
                                              String username, String password)
    {
        Cluster.Builder builder = Cluster.builder()
                                         .addContactPoints(nativeAddr)
                                         .withClusterName(clusterName)
                                         .withPort(nativePort)
                                         .withProtocolVersion(com.datastax.driver.core.ProtocolVersion.fromInt(version.asInt()))
                                         .withNettyOptions(nettyOptions)
                                         .withoutMetrics();
        if (username != null)
            builder.withAuthProvider(new PlainTextAuthProvider(username, password));
        return builder.build();
    }

    public static void closeClientCluster(Cluster cluster)
    {
        cluster.closeAsync().force();
        logger.info("Closed Java Driver instance for cluster {}", cluster.getClusterName());
    }

    protected void dropPerTestKeyspace() throws Throwable
    {
        execute(String.format("DROP KEYSPACE IF EXISTS %s", KEYSPACE_PER_TEST));
    }

    /**
     * Returns a copy of the specified list.
     * @return a copy of the specified list.
     */
    private static List<String> copy(List<String> list)
    {
        return list.isEmpty() ? Collections.<String>emptyList() : new ArrayList<>(list);
    }

    public ColumnFamilyStore getCurrentColumnFamilyStore()
    {
        return getCurrentColumnFamilyStore(KEYSPACE);
    }

    public ColumnFamilyStore getCurrentColumnFamilyStore(String keyspace)
    {
        String currentTable = currentTable();
        return currentTable == null
             ? null
             : Keyspace.open(keyspace).getColumnFamilyStore(currentTable);
    }

    public void flush(boolean forceFlush)
    {
        if (forceFlush)
            flush();
    }

    public void flush()
    {
        flush(KEYSPACE);
    }

    public void flush(String keyspace)
    {
        ColumnFamilyStore store = getCurrentColumnFamilyStore(keyspace);
        if (store != null)
            store.forceBlockingFlush();
    }

    public void disableCompaction(String keyspace)
    {
        ColumnFamilyStore store = getCurrentColumnFamilyStore(keyspace);
        if (store != null)
            store.disableAutoCompaction();
    }

    public void compact()
    {
        compact(KEYSPACE);
    }

    public void compact(String keyspace)
    {
        try
        {
            ColumnFamilyStore store = getCurrentColumnFamilyStore(keyspace);
            if (store != null)
                store.forceMajorCompaction();
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void disableCompaction()
    {
        disableCompaction(KEYSPACE);
    }

    public void enableCompaction(String keyspace)
    {
        ColumnFamilyStore store = getCurrentColumnFamilyStore(keyspace);
        if (store != null)
            store.enableAutoCompaction();
    }

    public void enableCompaction()
    {
        enableCompaction(KEYSPACE);
    }

    public void cleanupCache()
    {
        ColumnFamilyStore store = getCurrentColumnFamilyStore();
        if (store != null)
            store.cleanupCache();
    }

    public static FunctionName parseFunctionName(String qualifiedName)
    {
        int i = qualifiedName.indexOf('.');
        return i == -1
               ? FunctionName.nativeFunction(qualifiedName)
               : new FunctionName(qualifiedName.substring(0, i).trim(), qualifiedName.substring(i+1).trim());
    }

    public static String shortFunctionName(String f)
    {
        return parseFunctionName(f).name;
    }

    private static void removeAllSSTables(String ks, List<String> tables)
    {
        // clean up data directory which are stored as data directory/keyspace/data files
        for (File d : Directories.getKSChildDirectories(ks))
        {
            if (d.exists() && containsAny(d.getName(), tables))
                FileUtils.deleteRecursive(d);
        }
    }

    private static boolean containsAny(String filename, List<String> tables)
    {
        for (int i = 0, m = tables.size(); i < m; i++)
            // don't accidentally delete in-use directories with the
            // same prefix as a table to delete, i.e. table_1 & table_11
            if (filename.contains(tables.get(i) + "-"))
                return true;
        return false;
    }

    protected String keyspace()
    {
        return KEYSPACE;
    }

    protected String currentTable()
    {
        if (tables.isEmpty())
            return null;
        return tables.get(tables.size() - 1);
    }

    protected ByteBuffer unset()
    {
        return ByteBufferUtil.UNSET_BYTE_BUFFER;
    }

    protected void forcePreparedValues()
    {
        this.usePrepared = true;
    }

    protected void stopForcingPreparedValues()
    {
        this.usePrepared = USE_PREPARED_VALUES;
    }

    protected void disablePreparedReuseForTest()
    {
        this.reusePrepared = false;
    }

    protected String createType(String query)
    {
        String typeName = "type_" + seqNumber.getAndIncrement();
        String fullQuery = String.format(query, KEYSPACE + "." + typeName);
        types.add(typeName);
        logger.info(fullQuery);
        schemaChange(fullQuery);
        return typeName;
    }

    protected String createFunction(String keyspace, String argTypes, String query) throws Throwable
    {
        String functionName = keyspace + ".function_" + seqNumber.getAndIncrement();
        createFunctionOverload(functionName, argTypes, query);
        return functionName;
    }

    protected void createFunctionOverload(String functionName, String argTypes, String query) throws Throwable
    {
        String fullQuery = String.format(query, functionName);
        functions.add(functionName + '(' + argTypes + ')');
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }

    protected String createAggregate(String keyspace, String argTypes, String query) throws Throwable
    {
        String aggregateName = keyspace + "." + "aggregate_" + seqNumber.getAndIncrement();
        createAggregateOverload(aggregateName, argTypes, query);
        return aggregateName;
    }

    protected void createAggregateOverload(String aggregateName, String argTypes, String query) throws Throwable
    {
        String fullQuery = String.format(query, aggregateName);
        aggregates.add(aggregateName + '(' + argTypes + ')');
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }

    protected String createKeyspace(String query)
    {
        String currentKeyspace = createKeyspaceName();
        String fullQuery = String.format(query, currentKeyspace);
        logger.info(fullQuery);
        schemaChange(fullQuery);
        return currentKeyspace;
    }

    protected String createKeyspaceName()
    {
        String currentKeyspace = "keyspace_" + seqNumber.getAndIncrement();
        keyspaces.add(currentKeyspace);
        return currentKeyspace;
    }

    public String createTable(String query)
    {
        return createTable(KEYSPACE, query);
    }

    public String createTable(String keyspace, String query)
    {
        String currentTable = createTableName();
        String fullQuery = formatQuery(keyspace, query);
        logger.info(fullQuery);
        schemaChange(fullQuery);
        return currentTable;
    }

    protected String createTableName()
    {
        String currentTable = "table_" + seqNumber.getAndIncrement();
        tables.add(currentTable);
        return currentTable;
    }

    protected void createTableMayThrow(String query) throws Throwable
    {
        String currentTable = createTableName();
        String fullQuery = formatQuery(query);
        logger.info(fullQuery);
        QueryProcessor.executeOnceInternal(fullQuery).blockingGet();
    }

    protected void alterTable(String query)
    {
        String fullQuery = formatQuery(query);
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }

    protected void alterTableMayThrow(String query) throws Throwable
    {
        String fullQuery = formatQuery(query);
        logger.info(fullQuery);
        QueryProcessor.executeOnceInternal(fullQuery).blockingGet();
    }

    public void dropTable(String query)
    {
       dropTable(KEYSPACE, query);
    }

    public void dropTable(String keyspace, String query)
    {
        dropFormattedTable(String.format(query, keyspace + "." + currentTable()));
    }

    protected void dropFormattedTable(String formattedQuery)
    {
        logger.info(formattedQuery);
        schemaChange(formattedQuery);
    }

    protected String createIndex(String query)
    {
        String formattedQuery = formatQuery(query);
        return createFormattedIndex(formattedQuery);
    }

    protected String createFormattedIndex(String formattedQuery)
    {
        logger.info(formattedQuery);
        String indexName = getCreateIndexName(formattedQuery);
        schemaChange(formattedQuery);
        return indexName;
    }

    protected static String getCreateIndexName(String formattedQuery)
    {
        Matcher matcher = CREATE_INDEX_PATTERN.matcher(formattedQuery);
        if (!matcher.find())
            throw new IllegalArgumentException("Expected valid create index query but found: " + formattedQuery);

        String index = matcher.group(2);
        if (!Strings.isNullOrEmpty(index))
            return index;

        String keyspace = matcher.group(5);
        if (Strings.isNullOrEmpty(keyspace))
            throw new IllegalArgumentException("Keyspace name should be specified: " + formattedQuery);

        String table = matcher.group(7);
        if (Strings.isNullOrEmpty(table))
            throw new IllegalArgumentException("Table name should be specified: " + formattedQuery);

        String column = matcher.group(9);
        return Indexes.getAvailableIndexName(keyspace, table, Strings.isNullOrEmpty(column) ? null : column);
    }

    /**
     * Index creation is asynchronous, this method searches in the system table IndexInfo
     * for the specified index and returns true if it finds it, which indicates the
     * index was built. If we haven't found it after 5 seconds we give-up.
     */
    protected boolean waitForIndex(String keyspace, String table, String index) throws Throwable
    {
        long start = System.currentTimeMillis();
        boolean indexCreated = false;
        while (!indexCreated)
        {
            Object[][] results = getRows(execute("select index_name from system.\"IndexInfo\" where table_name = ?", keyspace));
            for(int i = 0; i < results.length; i++)
            {
                if (index.equals(results[i][0]))
                {
                    indexCreated = true;
                    break;
                }
            }

            if (System.currentTimeMillis() - start > 5000)
                break;

            Thread.sleep(10);
        }

        return indexCreated;
    }

    /**
     * Index creation is asynchronous, this method waits until the specified index hasn't any building task running.
     * <p>
     * This method differs from {@link #waitForIndex(String, String, String)} in that it doesn't require the index to be
     * fully nor successfully built, so it can be used to wait for failing index builds.
     *
     * @param keyspace the index keyspace name
     * @param indexName the index name
     * @return {@code true} if the index build tasks have finished in 5 seconds, {@code false} otherwise
     */
    protected boolean waitForIndexBuilds(String keyspace, String indexName) throws InterruptedException
    {
        long start = System.currentTimeMillis();
        SecondaryIndexManager indexManager = getCurrentColumnFamilyStore(keyspace).indexManager;

        while (true)
        {
            if (!indexManager.isIndexBuilding(indexName))
            {
                return true;
            }
            else if (System.currentTimeMillis() - start > 5000)
            {
                return false;
            }
            else
            {
                Thread.sleep(10);
            }
        }
    }

    protected void createIndexMayThrow(String query) throws Throwable
    {
        String fullQuery = formatQuery(query);
        logger.info(fullQuery);
        QueryProcessor.executeOnceInternal(fullQuery).blockingGet();
    }

    protected void dropIndex(String query) throws Throwable
    {
        String fullQuery = String.format(query, KEYSPACE);
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }

    protected static boolean isViewBuiltBlocking(String keyspaceName, String viewName)
    {
        return TPCUtils.blockingGet(SystemKeyspace.isViewBuilt(keyspaceName, viewName));
    }

    /**
     *  Because the tracing executor is single threaded, submitting an empty event should ensure
     *  that all tracing events mutations have been applied.
     */
    protected void waitForTracingEvents()
    {
        try
        {
            StageManager.tracingExecutor.submit(() -> {}).get();
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Failed to wait for tracing events: {}", t);
        }
    }

    protected void assertLastSchemaChange(Event.SchemaChange.Change change, Event.SchemaChange.Target target,
                                          String keyspace, String name,
                                          String... argTypes)
    {
        assertTrue(lastSchemaChangeResult instanceof ResultMessage.SchemaChange);
        ResultMessage.SchemaChange schemaChange = (ResultMessage.SchemaChange) lastSchemaChangeResult;
        Assert.assertSame(change, schemaChange.change.change);
        Assert.assertSame(target, schemaChange.change.target);
        Assert.assertEquals(keyspace, schemaChange.change.keyspace);
        Assert.assertEquals(name, schemaChange.change.name);
        Assert.assertEquals(argTypes != null ? Arrays.asList(argTypes) : null, schemaChange.change.argTypes);
    }

    protected static void schemaChange(String query)
    {
        try
        {
            logger.info("SCHEMA CHANGE: " + query);

            ClientState state = ClientState.forInternalCalls();
            state.setKeyspace(SchemaConstants.SYSTEM_KEYSPACE_NAME);
            QueryState queryState = new QueryState(state, UserRolesAndPermissions.SYSTEM);

            ParsedStatement.Prepared prepared = QueryProcessor.getStatement(query, queryState);
            prepared.statement.validate(queryState);

            QueryOptions options = QueryOptions.forInternalCalls(Collections.<ByteBuffer>emptyList());

            lastSchemaChangeResult = prepared.statement.executeInternal(queryState, options).blockingGet();
        }
        catch (Exception e)
        {
            logger.info("Error performing schema change", e);
            throw new RuntimeException("Error setting schema for test (query was: " + query + ")", e);
        }
    }

    protected TableMetadata currentTableMetadata()
    {
        return currentTableMetadata(KEYSPACE);
    }

    protected TableMetadata currentTableMetadata(String keyspace)
    {
        return tableMetadata(keyspace, currentTable());
    }

    protected TableMetadata tableMetadata(String ks, String name)
    {
        assert tables.contains(name);
        return Schema.instance.getTableMetadata(ks, name);
    }

    protected com.datastax.driver.core.ResultSet executeNet(ProtocolVersion protocolVersion, String query, Object... values) throws Throwable
    {
        return sessionNet(protocolVersion).execute(formatQuery(query), values);
    }

    protected com.datastax.driver.core.ResultSet executeNet(String query, Object... values) throws Throwable
    {
        return executeNet(getDefaultVersion(), query, values);
    }

    protected com.datastax.driver.core.PreparedStatement prepareNet(ProtocolVersion protocolVersion, String query)
    {
        return sessionNet(protocolVersion).prepare(query);
    }

    protected com.datastax.driver.core.PreparedStatement prepareNet(String query)
    {
        return prepareNet(getDefaultVersion(), query);
    }

    protected com.datastax.driver.core.ResultSet executeNet(ProtocolVersion protocolVersion, Statement statement)
    {
        return sessionNet(protocolVersion).execute(statement);
    }

    protected com.datastax.driver.core.ResultSet executeNet(Statement statement)
    {
        return executeNet(getDefaultVersion(), statement);
    }

    protected com.datastax.driver.core.ResultSetFuture executeNetAsync(ProtocolVersion protocolVersion, Statement statement)
    {
        return sessionNet(protocolVersion).executeAsync(statement);
    }

    protected com.datastax.driver.core.ResultSet executeNetWithPaging(String query, int pageSize) throws Throwable
    {
        return sessionNet().execute(new SimpleStatement(formatQuery(query)).setFetchSize(pageSize));
    }

    /**
     * Use the specified user for executing the queries over the network.
     * @param username the user name
     * @param password the user password
     */
    public void useUser(String username, String password)
    {
        this.user = new User(username, password);
    }

    /**
     * Use the super user for executing the queries over the network.
     */
    public void useSuperUser()
    {
        this.user = SUPER_USER;
    }

    public boolean isSuperUser()
    {
        return SUPER_USER.equals(user);
    }

    public Session sessionNet()
    {
        return sessionNet(getDefaultVersion());
    }

    public Session sessionNet(ProtocolVersion protocolVersion)
    {
        requireNetwork();

        return getSession(protocolVersion);
    }

    protected SimpleClient newSimpleClient(ProtocolVersion version, boolean compression) throws IOException
    {
        return new SimpleClient(nativeAddr.getHostAddress(), nativePort, version, version.isBeta(), new EncryptionOptions.ClientEncryptionOptions()).connect(compression);
    }

    private Session getSession(ProtocolVersion protocolVersion)
    {
        Cluster cluster = getCluster(protocolVersion);
        return sessions.computeIfAbsent(Pair.create(user, protocolVersion), userProto -> cluster.connect());
    }

    private Cluster getCluster(ProtocolVersion protocolVersion)
    {
        return clusters.computeIfAbsent(Pair.create(user, protocolVersion),
                                                         userProto -> initClientCluster(user, protocolVersion));
    }

    public static void invalidateAuthCaches()
    {
        invalidate("PermissionsCache");
        invalidate("RolesCache");
    }

    private static void invalidate(String authCacheName)
    {
        try
        {
            final ObjectName objectName = new ObjectName("org.apache.cassandra.auth:type=" + authCacheName);
            JMX.newMBeanProxy(ManagementFactory.getPlatformMBeanServer(), objectName, AuthCacheMBean.class)
               .invalidate();
        }
        catch (Exception e)
        {
            throw new IllegalStateException("Cannot invalidate " + authCacheName, e);
        }
    }

    public String formatQuery(String query)
    {
        return formatQuery(KEYSPACE, query);
    }

    public final String formatQuery(String keyspace, String query)
    {
        String currentTable = currentTable();
        return currentTable == null ? query : String.format(query, keyspace + "." + currentTable);
    }

    protected ResultMessage.Prepared prepare(String query) throws Throwable
    {
        return QueryProcessor.prepare(formatQuery(query), QueryState.forInternalCalls()).blockingGet();
    }

    public UntypedResultSet execute(String query, Object... values) throws Throwable
    {
        return executeFormattedQuery(formatQuery(query), values);
    }

    public Single<UntypedResultSet> executeAsync(String query, Object... values) throws Throwable
    {
        return executeFormattedQueryAsync(formatQuery(query), values);
    }

    public Single<UntypedResultSet> executeFormattedQueryAsync(String query, Object... values) throws Throwable
    {
        Single<UntypedResultSet> rs;
        if (usePrepared)
        {
            if (logger.isTraceEnabled())
                logger.trace("Executing: {} with values {}", query, formatAllValues(values));
            if (reusePrepared)
            {
                rs = QueryProcessor.executeInternalAsync(query, transformValues(values));

                // If a test uses a "USE ...", then presumably its statements use relative table. In that case, a USE
                // change the meaning of the current keyspace, so we don't want a following statement to reuse a previously
                // prepared statement at this wouldn't use the right keyspace. To avoid that, we drop the previously
                // prepared statement.
                if (query.startsWith("USE"))
                    QueryProcessor.clearInternalStatementsCache();
            }
            else
            {
                rs = QueryProcessor.executeOnceInternal(query, transformValues(values));
            }
        }
        else
        {
            query = replaceValues(query, values);
            if (logger.isTraceEnabled())
                logger.trace("Executing: {}", query);
            rs = QueryProcessor.executeOnceInternal(query);
        }
        return rs;
    }

    public UntypedResultSet executeFormattedQuery(String query, Object... values) throws Throwable
    {
        UntypedResultSet rs = executeFormattedQueryAsync(query, values).blockingGet();
        if (rs != null)
        {
            if (logger.isTraceEnabled())
                logger.trace("Got {} rows", rs.size());
        }
        return rs;
    }

    protected void assertRowsNet(ResultSet result, Object[]... rows)
    {
        assertRowsNet(getDefaultVersion(), result, rows);
    }

    public static Comparator<List<ByteBuffer>> RowComparator = (Comparator<List<ByteBuffer>>) (row1, row2) -> {
        int ret = Integer.compare(row1.size(), row2.size());
        if (ret != 0)
            return ret;

        for (int i = 0; i < row1.size(); i++)
        {
            ret = row1.get(i).compareTo(row2.get(i));
            if (ret != 0)
                return ret;
        }

        return 0;
    };

    protected void assertRowsNet(ProtocolVersion protocolVersion, ResultSet result, Object[]... rows)
    {
        assertRowsNet(protocolVersion, false, result, rows);
    }

    protected void assertRowsNet(ProtocolVersion protocolVersion, boolean ignoreOrder, ResultSet result, Object[] ... rows)
    {
        // necessary as we need cluster objects to supply CodecRegistry.
        // It's reasonably certain that the network setup has already been done
        // by the time we arrive at this point, but adding this check doesn't hurt
        requireNetwork();

        if (result == null)
        {
            if (rows.length > 0)
                fail(String.format("No rows returned by query but %d expected", rows.length));
            return;
        }

        ColumnDefinitions meta = result.getColumnDefinitions();

        List<List<ByteBuffer>> expectedRows = new ArrayList<>(rows.length);
        List<List<ByteBuffer>> actualRows = new ArrayList<>(rows.length);

        Cluster cluster = getCluster(protocolVersion);

        com.datastax.driver.core.ProtocolVersion driverVersion = cluster.getConfiguration()
                                                                        .getProtocolOptions()
                                                                        .getProtocolVersion();

        CodecRegistry codecRegistry = cluster.getConfiguration()
                                             .getCodecRegistry();

        Iterator<Row> iter = result.iterator();
        int i;
        for (i = 0; i < rows.length; i++)
        {
            Assert.assertEquals(String.format("Invalid number of (expected) values provided for row %d (using protocol version %s)",
                                              i, protocolVersion),
                                meta.size(), rows[i].length);

            assertTrue(String.format("Got fewer rows than expected. Expected %d but got %d", rows.length, i), iter.hasNext());
            Row actual = iter.next();

            List<ByteBuffer> expectedRow = new ArrayList<>(meta.size());
            List<ByteBuffer> actualRow = new ArrayList<>(meta.size());
            for (int j = 0; j < meta.size(); j++)
            {
                DataType type = meta.getType(j);
                com.datastax.driver.core.TypeCodec<Object> codec = codecRegistry.codecFor(type);
                expectedRow.add(codec.serialize(rows[i][j], driverVersion));
                actualRow.add(actual.getBytesUnsafe(meta.getName(j)));
            }

            expectedRows.add(expectedRow);
            actualRows.add(actualRow);
        }

        if (iter.hasNext())
        {
            List<Row> unexpectedRows = new ArrayList<>(2);
            while (iter.hasNext())
            {
                unexpectedRows.add(iter.next());
                i++;
            }

            String[][] formattedRows = new String[actualRows.size() + unexpectedRows.size()][meta.size()];
            for (int k = 0; k < actualRows.size(); k++)
            {
                List<ByteBuffer> row = actualRows.get(k);
                for (int j = 0; j < meta.size(); j++)
                {
                    DataType type = meta.getType(j);
                    com.datastax.driver.core.TypeCodec<Object> codec = codecRegistry.codecFor(type);

                    formattedRows[k][j] = codec.format(codec.deserialize(row.get(j), driverVersion));
                }
            }

            for (int k = actualRows.size(); k < actualRows.size() + unexpectedRows.size(); k++)
            {
                Row row = unexpectedRows.get(k - actualRows.size());
                for (int j = 0; j < meta.size(); j++)
                {
                    DataType type = meta.getType(j);
                    com.datastax.driver.core.TypeCodec<Object> codec = codecRegistry.codecFor(type);

                    formattedRows[k][j] = codec.format(codec.deserialize(row.getBytesUnsafe(meta.getName(j)), driverVersion));
                }
            }

            fail(String.format("Got more rows than expected. Expected %d but got %d (using protocol version %s)." +
                                      "\nReceived rows:\n%s",
                                      rows.length, i, protocolVersion,
                                      Arrays.stream(formattedRows).map(Arrays::toString).collect(Collectors.toList())));
        }

        if (ignoreOrder)
        {
            Collections.sort(expectedRows, RowComparator);
            Collections.sort(actualRows, RowComparator);
        }

        for(i = 0; i < expectedRows.size(); i++)
        {
            List<ByteBuffer> expected = expectedRows.get(i);
            List<ByteBuffer> actual = actualRows.get(i);

            for (int j = 0; j < meta.size(); j++)
            {
                DataType type = meta.getType(j);
                com.datastax.driver.core.TypeCodec<Object> codec = codecRegistry.codecFor(type);

                if (!Objects.equal(expected.get(j), actual.get(j)))
                    fail(String.format("Invalid value for row %d column %d (%s of type %s), " +
                                              "expected <%s> (%d bytes) but got <%s> (%d bytes) " +
                                              "(using protocol version %s)",
                                              i, j, meta.getName(j), type,
                                              codec.format(codec.deserialize(expected.get(j),driverVersion)),
                                              expected.get(j) == null ? 0 : expected.get(j).capacity(),
                                              codec.format(codec.deserialize(actual.get(j), driverVersion)),
                                              actual.get(j) == null ? 0 : actual.get(j).capacity(),
                                              protocolVersion));
            }
        }
    }

    public Object[][] getRowsNet(Cluster cluster, ColumnDefinitions meta, List<Row> rows)
    {
        if (rows == null || rows.isEmpty())
            return new Object[0][0];

        com.datastax.driver.core.TypeCodec<?>[] codecs = new com.datastax.driver.core.TypeCodec<?>[meta.size()];
        for (int j = 0; j < meta.size(); j++)
            codecs[j] = cluster.getConfiguration().getCodecRegistry().codecFor(meta.getType(j));

        Object[][] ret = new Object[rows.size()][];
        for (int i = 0; i < ret.length; i++)
        {
            Row row = rows.get(i);
            Assert.assertNotNull(row);

            ret[i] = new Object[meta.size()];
            for (int j = 0; j < meta.size(); j++)
                ret[i][j] = row.get(j, codecs[j]);
        }

        return ret;
    }

    public static void assertRows(UntypedResultSet result, Object[]... rows)
    {
        if (result == null)
        {
            if (rows.length > 0)
                fail(String.format("No rows returned by query but %d expected", rows.length));
            return;
        }

        if (logger.isTraceEnabled())
            logger.trace("ROWS:\n{}", String.join("\n", Arrays.stream(getRows(result)).map(row -> Arrays.toString(row)).collect(Collectors.toList())));

        List<ColumnSpecification> meta = result.metadata();
        Iterator<UntypedResultSet.Row> iter = result.iterator();
        int i = 0;
        while (iter.hasNext() && i < rows.length)
        {
            if (rows[i] == null)
                throw new IllegalArgumentException(String.format("Invalid expected value for row: %d. A row cannot be null.", i));

            Object[] expected = rows[i];
            UntypedResultSet.Row actual = iter.next();

            Assert.assertEquals(String.format("Invalid number of (expected) values provided for row %d", i), expected.length, meta.size());

            for (int j = 0; j < meta.size(); j++)
            {
                ColumnSpecification column = meta.get(j);

                ByteBuffer actualValue = actual.getBytes(column.name.toString());
                Object actualValueDecoded = decodeValue(column, actualValue);
                if (!Objects.equal(expected[j], actualValueDecoded))
                {
                    ByteBuffer expectedByteValue = makeByteBuffer(expected[j], column.type);
                    if (!Objects.equal(expectedByteValue, actualValue))
                    {
                        fail(String.format("Invalid value for row %d column %d (%s of type %s), expected <%s> but got <%s>",
                                                  i,
                                                  j,
                                                  column.name,
                                                  column.type.asCQL3Type(),
                                                  formatValue(expectedByteValue, column.type),
                                                  formatValue(actualValue, column.type)));
                    }
                }
            }
            i++;
        }

        if (iter.hasNext())
        {
            while (iter.hasNext())
            {
                UntypedResultSet.Row actual = iter.next();
                i++;

                StringBuilder str = new StringBuilder();
                for (int j = 0; j < meta.size(); j++)
                {
                    ColumnSpecification column = meta.get(j);
                    ByteBuffer actualValue = actual.getBytes(column.name.toString());
                    str.append(String.format("%s=%s ", column.name, formatValue(actualValue, column.type)));
                }
                logger.info("Extra row num {}: {}", i, str.toString());
            }
            fail(String.format("Got more rows than expected. Expected %d but got %d.", rows.length, i));
        }

        assertTrue(String.format("Got %s rows than expected. Expected %d but got %d", rows.length>i ? "less" : "more", rows.length, i), i == rows.length);
    }

    private static Object decodeValue(ColumnSpecification column, ByteBuffer actualValue)
    {
        try
        {
            Object actualValueDecoded = actualValue == null ? null : column.type.getSerializer().deserialize(actualValue);
            return actualValueDecoded;
        }
        catch (MarshalException e)
        {
            throw new AssertionError("Cannot deserialize the value for column " + column.name, e);
        }
    }

    /**
     * Like assertRows(), but ignores the ordering of rows.
     */
    public static void assertRowsIgnoringOrder(UntypedResultSet result, Object[]... rows)
    {
        assertRowsIgnoringOrderInternal(result, false, rows);
    }

    public static void assertRowsIgnoringOrderAndExtra(UntypedResultSet result, Object[]... rows)
    {
        assertRowsIgnoringOrderInternal(result, true, rows);
    }

    private static void assertRowsIgnoringOrderInternal(UntypedResultSet result, boolean ignoreExtra, Object[]... rows)
    {
        if (result == null)
        {
            if (rows.length > 0)
                fail(String.format("No rows returned by query but %d expected", rows.length));
            return;
        }

        List<ColumnSpecification> meta = result.metadata();

        Set<List<ByteBuffer>> expectedRows = new HashSet<>(rows.length);
        for (Object[] expected : rows)
        {
            Assert.assertEquals("Invalid number of (expected) values provided for row", expected.length, meta.size());
            List<ByteBuffer> expectedRow = new ArrayList<>(meta.size());
            for (int j = 0; j < meta.size(); j++)
                expectedRow.add(makeByteBuffer(expected[j], meta.get(j).type));
            expectedRows.add(expectedRow);
        }

        Set<List<ByteBuffer>> actualRows = new HashSet<>(result.size());
        for (UntypedResultSet.Row actual : result)
        {
            List<ByteBuffer> actualRow = new ArrayList<>(meta.size());
            for (int j = 0; j < meta.size(); j++)
                actualRow.add(actual.getBytes(meta.get(j).name.toString()));
            actualRows.add(actualRow);
        }

        com.google.common.collect.Sets.SetView<List<ByteBuffer>> extra = com.google.common.collect.Sets.difference(actualRows, expectedRows);
        com.google.common.collect.Sets.SetView<List<ByteBuffer>> missing = com.google.common.collect.Sets.difference(expectedRows, actualRows);
        if ((!ignoreExtra && !extra.isEmpty()) || !missing.isEmpty())
        {
            List<String> extraRows = makeRowStrings(extra, meta);
            List<String> missingRows = makeRowStrings(missing, meta);
            StringBuilder sb = new StringBuilder();
            if (!extra.isEmpty())
            {
                sb.append("Got ").append(extra.size()).append(" extra row(s) ");
                if (!missing.isEmpty())
                    sb.append("and ").append(missing.size()).append(" missing row(s) ");
                sb.append("in result.  Extra rows:\n    ");
                sb.append(extraRows.stream().collect(Collectors.joining("\n    ")));
                if (!missing.isEmpty())
                    sb.append("\nMissing Rows:\n    ").append(missingRows.stream().collect(Collectors.joining("\n    ")));
                fail(sb.toString());
            }

            if (!missing.isEmpty())
                fail("Missing " + missing.size() + " row(s) in result: \n    " + missingRows.stream().collect(Collectors.joining("\n    ")));
        }

        assert ignoreExtra || expectedRows.size() == actualRows.size();
    }

    protected static List<String> makeRowStrings(UntypedResultSet resultSet)
    {
        List<List<ByteBuffer>> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : resultSet)
        {
            List<ByteBuffer> values = new ArrayList<>();
            for (ColumnSpecification columnSpecification : resultSet.metadata())
            {
                values.add(row.getBytes(columnSpecification.name.toString()));
            }
            rows.add(values);
        }

        return makeRowStrings(rows, resultSet.metadata());
    }

    private static List<String> makeRowStrings(Iterable<List<ByteBuffer>> rows, List<ColumnSpecification> meta)
    {
        List<String> strings = new ArrayList<>();
        for (List<ByteBuffer> row : rows)
        {
            StringBuilder sb = new StringBuilder("row(");
            for (int j = 0; j < row.size(); j++)
            {
                ColumnSpecification column = meta.get(j);
                sb.append(column.name.toString()).append("=").append(formatValue(row.get(j), column.type));
                if (j < (row.size() - 1))
                    sb.append(", ");
            }
            strings.add(sb.append(")").toString());
        }
        return strings;
    }

    protected void assertRowCount(UntypedResultSet result, int numExpectedRows)
    {
        if (result == null)
        {
            if (numExpectedRows > 0)
                fail(String.format("No rows returned by query but %d expected", numExpectedRows));
            return;
        }

        List<ColumnSpecification> meta = result.metadata();
        Iterator<UntypedResultSet.Row> iter = result.iterator();
        int i = 0;
        while (iter.hasNext() && i < numExpectedRows)
        {
            UntypedResultSet.Row actual = iter.next();
            assertNotNull(actual);
            i++;
        }

        if (iter.hasNext())
        {
            while (iter.hasNext())
            {
                iter.next();
                i++;
            }
            fail(String.format("Got less rows than expected. Expected %d but got %d.", numExpectedRows, i));
        }

        assertTrue(String.format("Got %s rows than expected. Expected %d but got %d", numExpectedRows>i ? "less" : "more", numExpectedRows, i), i == numExpectedRows);
    }

    protected static Object[][] getRows(UntypedResultSet result)
    {
        if (result == null)
            return new Object[0][];

        List<Object[]> ret = new ArrayList<>();
        List<ColumnSpecification> meta = result.metadata();

        Iterator<UntypedResultSet.Row> iter = result.iterator();
        while (iter.hasNext())
        {
            UntypedResultSet.Row rowVal = iter.next();
            Object[] row = new Object[meta.size()];
            for (int j = 0; j < meta.size(); j++)
            {
                ColumnSpecification column = meta.get(j);
                ByteBuffer val = rowVal.getBytes(column.name.toString());
                row[j] = val == null ? null : column.type.getSerializer().deserialize(val);
            }

            ret.add(row);
        }

        Object[][] a = new Object[ret.size()][];
        return ret.toArray(a);
    }

    protected void assertColumnNames(UntypedResultSet result, String... expectedColumnNames)
    {
        if (result == null)
        {
            fail("No rows returned by query.");
            return;
        }

        List<ColumnSpecification> metadata = result.metadata();
        Assert.assertEquals("Got less columns than expected.", expectedColumnNames.length, metadata.size());

        for (int i = 0, m = metadata.size(); i < m; i++)
        {
            ColumnSpecification columnSpec = metadata.get(i);
            Assert.assertEquals(expectedColumnNames[i], columnSpec.name.toString());
        }
    }

    protected void assertAllRows(Object[]... rows) throws Throwable
    {
        assertRows(execute("SELECT * FROM %s"), rows);
    }

    public static Object[] row(Object... expected)
    {
        return expected;
    }

    protected void assertEmpty(UntypedResultSet result) throws Throwable
    {
        if (result != null && !result.isEmpty())
            throw new AssertionError(String.format("Expected empty result but got %d rows: %s \n", result.size(), makeRowStrings(result)));
    }

    protected void assertInvalid(String query, Object... values) throws Throwable
    {
        assertInvalidMessage(null, query, values);
    }

    /**
     * Checks that the specified query is not autorized for the current user.
     * @param errorMessage The expected error message
     * @param query the query
     * @param values the query parameters
     */
    protected void assertUnauthorizedQuery(String errorMessage, String query, Object... values) throws Throwable
    {
        assertInvalidThrowMessage(Optional.of(ProtocolVersion.CURRENT),
                                  errorMessage,
                                  UnauthorizedException.class,
                                  query,
                                  values);
    }

    protected void assertInvalidMessage(String errorMessage, String query, Object... values) throws Throwable
    {
        assertInvalidThrowMessage(errorMessage, null, query, values);
    }

    protected void assertInvalidThrow(Class<? extends Throwable> exception, String query, Object... values) throws Throwable
    {
        assertInvalidThrowMessage(null, exception, query, values);
    }

    protected void assertInvalidThrowMessage(String errorMessage, Class<? extends Throwable> exception, String query, Object... values) throws Throwable
    {
        assertInvalidThrowMessage(Optional.empty(), errorMessage, exception, query, values);
    }

    // if a protocol version > Integer.MIN_VALUE is supplied, executes
    // the query via the java driver, mimicking a real client.
    protected void assertInvalidThrowMessage(Optional<ProtocolVersion> protocolVersion,
                                             String errorMessage,
                                             Class<? extends Throwable> exception,
                                             String query,
                                             Object... values) throws Throwable
    {
        try
        {
            if (!protocolVersion.isPresent())
                execute(query, values);
            else
                executeNet(protocolVersion.get(), query, values);

            fail("Query should be invalid but no error was thrown. Query is: " + queryInfo(query, values));
        }
        catch (Exception e)
        {
            if (exception != null && !exception.isAssignableFrom(e.getClass()))
            {
                fail("Query should be invalid but wrong error was thrown. " +
                     "Expected: " + exception.getName() + ", got: " + e.getClass().getName() + ". " +
                     "Query is: " + queryInfo(query, values) + ". Stack trace of unexpected exception is:\n" +
                     String.join("\n", Arrays.stream(e.getStackTrace())
                                             .map(t -> t.toString())
                                             .collect(Collectors.toList())));
            }
            if (errorMessage != null)
            {
                assertMessageContains(errorMessage, e);
            }
        }
    }

    protected void assertClientWarning(String warningMessage,
                                       String query,
                                       Object... values) throws Throwable
    {
        assertClientWarning(getDefaultVersion(), warningMessage, query, values);
    }

    protected void assertClientWarning(ProtocolVersion protocolVersion,
                                       String warningMessage,
                                       String query,
                                       Object... values) throws Throwable
    {
        ResultSet rs = executeNet(protocolVersion, query, values);
        List<String> warnings = rs.getExecutionInfo().getWarnings();
        assertNotNull("Expecting one warning but get none", warnings);
        assertTrue("Expecting one warning but get " + warnings.size(), warnings.size() == 1);
        assertTrue("Expecting warning message to contains " + warningMessage + " but was: " + warnings.get(0),
                   warnings.get(0).contains(warningMessage));
    }

    protected void assertNoClientWarning(String query,
                                         Object... values) throws Throwable
    {
        assertNoClientWarning(getDefaultVersion(), query, values);
    }

    protected void assertNoClientWarning(ProtocolVersion protocolVersion,
                                         String query,
                                         Object... values) throws Throwable
    {
        ResultSet rs = executeNet(protocolVersion, query, values);
        List<String> warnings = rs.getExecutionInfo().getWarnings();
        assertTrue("Expecting no warning but get some: " + warnings, warnings == null || warnings.isEmpty());
    }

    private static String queryInfo(String query, Object[] values)
    {
        return USE_PREPARED_VALUES
               ? query + " (values: " + formatAllValues(values) + ")"
               : replaceValues(query, values);
    }

    protected ParsedStatement assertValidSyntax(String query) throws Throwable
    {
        try
        {
            return QueryProcessor.parseStatement(query);
        }
        catch(SyntaxException e)
        {
            fail(String.format("Expected query syntax to be valid but was invalid. Query is: %s; Error is %s",
                                      query, e.getMessage()));
            return null;
        }
    }

    protected void assertInvalidSyntax(String query, Object... values) throws Throwable
    {
        assertInvalidSyntaxMessage(null, query, values);
    }

    protected void assertInvalidSyntaxMessage(String errorMessage, String query, Object... values) throws Throwable
    {
        try
        {
            execute(query, values);
            fail("Query should have invalid syntax but no error was thrown. Query is: " + queryInfo(query, values));
        }
        catch (SyntaxException e)
        {
            if (errorMessage != null)
            {
                assertMessageContains(errorMessage, e);
            }
        }
    }

    /**
     * Asserts that the message of the specified exception contains the specified text.
     *
     * @param text the text that the exception message must contains
     * @param e the exception to check
     */
    private static void assertMessageContains(String text, Exception e)
    {
        assertTrue("Expected error message to contain '" + text + "', but got '" + e.getMessage() + "'",
                e.getMessage().contains(text));
    }

    /**
     * Sorts a list of int32 keys by their Murmur3Partitioner token order.
     */
    public static List<Integer> partitionerSortedKeys(List<Integer> unsortedKeys)
    {
        List<DecoratedKey> decoratedKeys = unsortedKeys.stream().map(i -> Murmur3Partitioner.instance.decorateKey(Int32Type.instance.getSerializer().serialize(i))).collect(Collectors.toList());
        Collections.sort(decoratedKeys, DecoratedKey.comparator);
        return decoratedKeys.stream().map(dk -> Int32Type.instance.getSerializer().deserialize(dk.getKey())).collect(Collectors.toList());
    }

    @FunctionalInterface
    public interface CheckedFunction {
        void apply() throws Throwable;
    }

    /**
     * Runs the given function before and after a flush of sstables.  This is useful for checking that behavior is
     * the same whether data is in memtables or sstables.
     * @param runnable
     * @throws Throwable
     */
    public void beforeAndAfterFlush(CheckedFunction runnable) throws Throwable
    {
        runnable.apply();
        flush();
        runnable.apply();
    }

    private static String replaceValues(String query, Object[] values)
    {
        StringBuilder sb = new StringBuilder();
        int last = 0;
        int i = 0;
        int idx;
        while ((idx = query.indexOf('?', last)) > 0)
        {
            if (i >= values.length)
                throw new IllegalArgumentException(String.format("Not enough values provided. The query has at least %d variables but only %d values provided", i, values.length));

            sb.append(query.substring(last, idx));

            Object value = values[i++];

            // When we have a .. IN ? .., we use a list for the value because that's what's expected when the value is serialized.
            // When we format as string however, we need to special case to use parenthesis. Hackish but convenient.
            if (idx >= 3 && value instanceof List && query.substring(idx - 3, idx).equalsIgnoreCase("IN "))
            {
                List l = (List)value;
                sb.append("(");
                for (int j = 0; j < l.size(); j++)
                {
                    if (j > 0)
                        sb.append(", ");
                    sb.append(formatForCQL(l.get(j)));
                }
                sb.append(")");
            }
            else
            {
                sb.append(formatForCQL(value));
            }
            last = idx + 1;
        }
        sb.append(query.substring(last));
        return sb.toString();
    }

    // We're rellly only returning ByteBuffers but this make the type system happy
    private static Object[] transformValues(Object[] values)
    {
        // We could partly rely on QueryProcessor.executeOnceInternal doing type conversion for us, but
        // it would complain with ClassCastException if we pass say a string where an int is excepted (since
        // it bases conversion on what the value should be, not what it is). For testing, we sometimes
        // want to pass value of the wrong type and assert that this properly raise an InvalidRequestException
        // and executeOnceInternal goes into way. So instead, we pre-convert everything to bytes here based
        // on the value.
        // Besides, we need to handle things like TupleValue that executeOnceInternal don't know about.

        Object[] buffers = new ByteBuffer[values.length];
        for (int i = 0; i < values.length; i++)
        {
            Object value = values[i];
            if (value == null)
            {
                buffers[i] = null;
                continue;
            }
            else if (value == ByteBufferUtil.UNSET_BYTE_BUFFER)
            {
                buffers[i] = ByteBufferUtil.UNSET_BYTE_BUFFER;
                continue;
            }

            try
            {
                buffers[i] = typeFor(value).decompose(serializeTuples(value));
            }
            catch (Exception ex)
            {
                logger.info("Error serializing query parameter {}:", value, ex);
                throw ex;
            }
        }
        return buffers;
    }

    private static Object serializeTuples(Object value)
    {
        if (value instanceof TupleValue)
        {
            return ((TupleValue)value).toByteBuffer();
        }

        // We need to reach inside collections for TupleValue and transform them to ByteBuffer
        // since otherwise the decompose method of the collection AbstractType won't know what
        // to do with them
        if (value instanceof List)
        {
            List l = (List)value;
            List n = new ArrayList(l.size());
            for (Object o : l)
                n.add(serializeTuples(o));
            return n;
        }

        if (value instanceof Set)
        {
            Set s = (Set)value;
            Set n = new LinkedHashSet(s.size());
            for (Object o : s)
                n.add(serializeTuples(o));
            return n;
        }

        if (value instanceof Map)
        {
            Map m = (Map)value;
            Map n = new LinkedHashMap(m.size());
            for (Object entry : m.entrySet())
                n.put(serializeTuples(((Map.Entry)entry).getKey()), serializeTuples(((Map.Entry)entry).getValue()));
            return n;
        }
        return value;
    }

    private static String formatAllValues(Object[] values)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < values.length; i++)
        {
            if (i > 0)
                sb.append(", ");
            sb.append(formatForCQL(values[i]));
        }
        sb.append("]");
        return sb.toString();
    }

    private static String formatForCQL(Object value)
    {
        if (value == null)
            return "null";

        if (value instanceof TupleValue)
            return ((TupleValue)value).toCQLString();

        // We need to reach inside collections for TupleValue. Besides, for some reason the format
        // of collection that CollectionType.getString gives us is not at all 'CQL compatible'
        if (value instanceof Collection || value instanceof Map)
        {
            StringBuilder sb = new StringBuilder();
            if (value instanceof List)
            {
                List l = (List)value;
                sb.append("[");
                for (int i = 0; i < l.size(); i++)
                {
                    if (i > 0)
                        sb.append(", ");
                    sb.append(formatForCQL(l.get(i)));
                }
                sb.append("]");
            }
            else if (value instanceof Set)
            {
                Set s = (Set)value;
                sb.append("{");
                Iterator iter = s.iterator();
                while (iter.hasNext())
                {
                    sb.append(formatForCQL(iter.next()));
                    if (iter.hasNext())
                        sb.append(", ");
                }
                sb.append("}");
            }
            else
            {
                Map m = (Map)value;
                sb.append("{");
                Iterator iter = m.entrySet().iterator();
                while (iter.hasNext())
                {
                    Map.Entry entry = (Map.Entry)iter.next();
                    sb.append(formatForCQL(entry.getKey())).append(": ").append(formatForCQL(entry.getValue()));
                    if (iter.hasNext())
                        sb.append(", ");
                }
                sb.append("}");
            }
            return sb.toString();
        }

        AbstractType type = typeFor(value);
        String s = type.getString(type.decompose(value));

        if (type instanceof InetAddressType || type instanceof TimestampType)
            return String.format("'%s'", s);
        else if (type instanceof UTF8Type)
            return String.format("'%s'", s.replaceAll("'", "''"));
        else if (type instanceof BytesType)
            return "0x" + s;

        return s;
    }

    protected static ByteBuffer makeByteBuffer(Object value, AbstractType type)
    {
        if (value == null)
            return null;

        if (value instanceof TupleValue)
            return ((TupleValue)value).toByteBuffer();

        if (value instanceof ByteBuffer)
            return (ByteBuffer)value;

        return type.decompose(serializeTuples(value));
    }

    protected static String formatValue(ByteBuffer bb, AbstractType<?> type)
    {
        if (bb == null)
            return "null";

        if (type instanceof CollectionType)
        {
            // CollectionType override getString() to use hexToBytes. We can't change that
            // without breaking SSTable2json, but the serializer for collection have the
            // right getString so using it directly instead.
            TypeSerializer ser = type.getSerializer();
            return ser.toString(ser.deserialize(bb));
        }

        return type.getString(bb);
    }

    protected TupleValue tuple(Object...values)
    {
        return new TupleValue(values);
    }

    protected Object userType(Object... values)
    {
        if (values.length % 2 != 0)
            throw new IllegalArgumentException("userType() requires an even number of arguments");

        String[] fieldNames = new String[values.length / 2];
        Object[] fieldValues = new Object[values.length / 2];
        int fieldNum = 0;
        for (int i = 0; i < values.length; i += 2)
        {
            fieldNames[fieldNum] = (String) values[i];
            fieldValues[fieldNum] = values[i + 1];
            fieldNum++;
        }
        return new UserTypeValue(fieldNames, fieldValues);
    }

    protected Object list(Object...values)
    {
        return Arrays.asList(values);
    }

    protected Set set(Object...values)
    {
        return ImmutableSet.copyOf(values);
    }

    protected Map map(Object...values)
    {
        if (values.length % 2 != 0)
            throw new IllegalArgumentException("Invalid number of arguments, got " + values.length);

        int size = values.length / 2;
        Map m = new LinkedHashMap(size);
        for (int i = 0; i < size; i++)
            m.put(values[2 * i], values[(2 * i) + 1]);
        return m;
    }

    protected com.datastax.driver.core.TupleType tupleTypeOf(ProtocolVersion protocolVersion, DataType...types)
    {
        requireNetwork();
        return getCluster(protocolVersion).getMetadata().newTupleType(types);
    }

    // Attempt to find an AbstracType from a value (for serialization/printing sake).
    // Will work as long as we use types we know of, which is good enough for testing
    private static AbstractType typeFor(Object value)
    {
        if (value instanceof ByteBuffer || value instanceof TupleValue || value == null)
            return BytesType.instance;

        if (value instanceof Byte)
            return ByteType.instance;

        if (value instanceof Short)
            return ShortType.instance;

        if (value instanceof Integer)
            return Int32Type.instance;

        if (value instanceof Long)
            return LongType.instance;

        if (value instanceof Float)
            return FloatType.instance;

        if (value instanceof Duration)
            return DurationType.instance;

        if (value instanceof Double)
            return DoubleType.instance;

        if (value instanceof BigInteger)
            return IntegerType.instance;

        if (value instanceof BigDecimal)
            return DecimalType.instance;

        if (value instanceof String)
            return UTF8Type.instance;

        if (value instanceof Boolean)
            return BooleanType.instance;

        if (value instanceof InetAddress)
            return InetAddressType.instance;

        if (value instanceof Date)
            return TimestampType.instance;

        if (value instanceof UUID)
            return UUIDType.instance;

        if (value instanceof List)
        {
            List l = (List)value;
            AbstractType elt = l.isEmpty() ? BytesType.instance : typeFor(l.get(0));
            return ListType.getInstance(elt, true);
        }

        if (value instanceof Set)
        {
            Set s = (Set)value;
            AbstractType elt = s.isEmpty() ? BytesType.instance : typeFor(s.iterator().next());
            return SetType.getInstance(elt, true);
        }

        if (value instanceof Map)
        {
            Map m = (Map)value;
            AbstractType keys, values;
            if (m.isEmpty())
            {
                keys = BytesType.instance;
                values = BytesType.instance;
            }
            else
            {
                Map.Entry entry = (Map.Entry)m.entrySet().iterator().next();
                keys = typeFor(entry.getKey());
                values = typeFor(entry.getValue());
            }
            return MapType.getInstance(keys, values, true);
        }

        throw new IllegalArgumentException("Unsupported value type (value is " + value + ")");
    }

    private static class TupleValue
    {
        protected final Object[] values;

        TupleValue(Object[] values)
        {
            this.values = values;
        }

        public ByteBuffer toByteBuffer()
        {
            ByteBuffer[] bbs = new ByteBuffer[values.length];
            for (int i = 0; i < values.length; i++)
                bbs[i] = makeByteBuffer(values[i], typeFor(values[i]));
            return TupleType.buildValue(bbs);
        }

        public String toCQLString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            for (int i = 0; i < values.length; i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(formatForCQL(values[i]));
            }
            sb.append(")");
            return sb.toString();
        }

        public String toString()
        {
            return "TupleValue" + toCQLString();
        }
    }

    private static class UserTypeValue extends TupleValue
    {
        private final String[] fieldNames;

        UserTypeValue(String[] fieldNames, Object[] fieldValues)
        {
            super(fieldValues);
            this.fieldNames = fieldNames;
        }

        @Override
        public String toCQLString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            boolean haveEntry = false;
            for (int i = 0; i < values.length; i++)
            {
                if (values[i] != null)
                {
                    if (haveEntry)
                        sb.append(", ");
                    sb.append(ColumnIdentifier.maybeQuote(fieldNames[i]));
                    sb.append(": ");
                    sb.append(formatForCQL(values[i]));
                    haveEntry = true;
                }
            }
            assert haveEntry;
            sb.append("}");
            return sb.toString();
        }

        public String toString()
        {
            return "UserTypeValue" + toCQLString();
        }
    }

    private static class User
    {
        /**
         * The user name
         */
        public final String username;

        /**
         * The user password
         */
        public final String password;

        public User(String username, String password)
        {
            this.username = username;
            this.password = password;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(username, password);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof User))
                return false;

            User u = (User) o;

            return Objects.equal(username, u.username)
                && Objects.equal(password, u.password);
        }
    }
}
