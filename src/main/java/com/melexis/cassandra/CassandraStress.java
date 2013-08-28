package com.melexis.cassandra;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftCluster;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;
import org.apache.commons.cli.*;

import java.util.*;
import java.util.concurrent.*;

public class CassandraStress {

    private final Keyspace keyspace;

    public CassandraStress(final Cluster cluster) {
        // Create a customized Consistency Level
        ConfigurableConsistencyLevel configurableConsistencyLevel = new ConfigurableConsistencyLevel();
        Map<String, HConsistencyLevel> clmap = new HashMap<String, HConsistencyLevel>();

        clmap.put("INDEXCF", HConsistencyLevel.ONE);
        clmap.put("DATACF", HConsistencyLevel.ONE);

        configurableConsistencyLevel.setReadCfConsistencyLevels(clmap);
        configurableConsistencyLevel.setWriteCfConsistencyLevels(clmap);

        keyspace = HFactory.createKeyspace("test", cluster, configurableConsistencyLevel);
    }

    public final void populate(final int n, final int numberOfColumns) {
        final ColumnFamilyTemplate<String, String> indexCf
                = new ThriftColumnFamilyTemplate<String, String>(keyspace, "INDEXCF", StringSerializer.get(), StringSerializer.get());
        final ColumnFamilyTemplate<String, String> dataCf
                = new ThriftColumnFamilyTemplate<String, String>(keyspace, "DATACF", StringSerializer.get(), StringSerializer.get());
        for (int i = 0; i < n; i++) {
            final String uuid = UUID.randomUUID().toString();

            final ColumnFamilyUpdater<String, String> indexUpdater = indexCf.createUpdater("index");
            final ColumnFamilyUpdater<String, String> dataUpdater = dataCf.createUpdater(uuid);

            for (int j = 0; j < numberOfColumns; j++) {
                dataUpdater.setString("hello" + j, "world");
            }

            indexUpdater.setString("col" + i, uuid);
            dataCf.update(dataUpdater);
            indexCf.update(indexUpdater);
        }
    }

    public final void load(final int n, final int numberOfColumns, final int threads, final int getThreads) throws ExecutionException, InterruptedException {
        final ExecutorService executorService = Executors.newFixedThreadPool(threads);
        final List<Future<Object>> futures = new ArrayList<Future<Object>>(threads);

        for (int i = 0; i < threads; i++) {
            final int threadNumber = i;
            futures.add(i, executorService.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    final ExecutorService getPool = Executors.newFixedThreadPool(getThreads);
                    final List<Future<List<HColumn<String, String>>>> getFutures = new ArrayList<Future<List<HColumn<String, String>>>>(n);

                    final long now = System.currentTimeMillis();
                    System.out.println("Starting thread #" + threadNumber);
                    final SliceQuery<String, String, String> indexQuery =
                            HFactory.createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
                    final SliceQuery<String, String, String> dataQuery =
                            HFactory.createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());

                    indexQuery.setColumnFamily("INDEXCF");
                    indexQuery.setKey("index");
                    indexQuery.setRange("a", "z", false, n);

                    dataQuery.setColumnFamily("DATACF");
                    dataQuery.setRange("a", "z", false, numberOfColumns);

                    System.out.println(String.format("[%d] Got index in %d msecs", threadNumber, System.currentTimeMillis() - now));

                    final List<HColumn<String, String>> cols = indexQuery.execute().get().getColumns();
                    for (final HColumn<String, String> col : cols) {
                        getFutures.add(getPool.submit(new Callable<List<HColumn<String, String>>>() {
                            @Override
                            public List<HColumn<String, String>> call() throws Exception {

                                dataQuery.setKey(col.getValue());
                                return dataQuery.execute().get().getColumns();
                            }
                        }));
                    }

                    final List<List<HColumn<String, String>>> results = new ArrayList<List<HColumn<String, String>>>(Lists.transform(getFutures, new Function<Future<List<HColumn<String, String>>>, List<HColumn<String, String>>>() {
                        @Override
                        public List<HColumn<String, String>> apply(final java.util.concurrent.Future<List<HColumn<String, String>>> objectFuture) {
                            try {
                                return objectFuture.get();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }));

                    System.out.println(String.format("[%d] Got %d results in %d msecs", threadNumber, results.size(), System.currentTimeMillis() - now));
                    return null;
                }
            }));
        }

        for (Future<Object> future : futures) {
            future.get();
        }
    }

    private static Options createOptions() {
        final Options options = new Options();
        options.addOption("h", "help", false, "print this message");
        options.addOption("c", "command", true, "the command to run ( one of [populate, load])");
        options.addOption("n", "numberOfRows", true, "the number of rows to populate / load");
        options.addOption("x", "numberOfColumns", true, "the columns of rows to populate / load");
        options.addOption("t", "numberOfConcurrentSessions", false, "the number of concurrent sessions to load");
        options.addOption("g", "numberOfGetThreads", false, "the number of get threads");

        return options;
    }

    private static void showHelp(Options options) {
        HelpFormatter h = new HelpFormatter();
        h.printHelp("help", options);
        System.exit(-1);
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, ParseException {
        final Options options = createOptions();
        final CommandLineParser parser = new PosixParser();
        final CommandLine cmd = parser.parse(options, args);

        final String command = cmd.getOptionValue("c");

        final CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator("esb-a-test.sensors.elex.be,esb-b-test.sensors.elex.be");
        final CassandraStress stress = new CassandraStress(new ThriftCluster("test", cassandraHostConfigurator));

        try {
        if (command.equals("populate")) {
            final int n = Integer.parseInt(cmd.getOptionValue("n"));
            final int numberOfColumns = Integer.parseInt(cmd.getOptionValue("x"));
            stress.populate(n, numberOfColumns);
        } else if (command.equals("load")) {
            final int n = Integer.parseInt(cmd.getOptionValue("n"));
            final int numberOfColumns = Integer.parseInt(cmd.getOptionValue("x"));
            final int numberOfSessions = cmd.getOptionValue("t") == null? 10: Integer.parseInt(cmd.getOptionValue("t"));
            final int getThreads = cmd.getOptionValue("g") == null? 10: Integer.parseInt(cmd.getOptionValue("g"));
            stress.load(n, numberOfColumns, numberOfSessions, getThreads);
        }
        } catch (Exception e) {
            e.printStackTrace();
            showHelp(options);
        }
    }
}
