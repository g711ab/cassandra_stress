package com.melexis.cassandra;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftCluster;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

public class CassandraStress {

    private final Keyspace keyspace;

    public CassandraStress(final Cluster cluster) {
        keyspace = HFactory.createKeyspace("test", cluster);
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

    public final void load(final int n, final int numberOfColumns, final int threads) throws ExecutionException, InterruptedException {
        final ExecutorService executorService = Executors.newFixedThreadPool(threads);
        final List<Future<Object>> futures = new ArrayList<Future<Object>>(threads);

        for (int i = 0; i < threads; i++) {
            final int threadNumber = i;
            futures.add(i, executorService.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
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

                    final List<HColumn<String, String>> cols = indexQuery.execute().get().getColumns();
                    for (HColumn<String, String> col : cols) {
                        dataQuery.setKey(col.getValue());

                        dataQuery.execute().get().getColumns();
                    }

                    System.out.println(String.format("Finished thread #%d in %d msecs", threadNumber, System.currentTimeMillis() - now));
                    return null;
                }
            }));
        }

        for (Future<Object> future: futures) {
            future.get();
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final String command = args[0];
        final CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator("esb-a-test.sensors.elex.be,esb-b-test.sensors.elex.be");
        final CassandraStress stress = new CassandraStress(new ThriftCluster("test", cassandraHostConfigurator));

        if (command.equals("populate")) {
            final int n = Integer.parseInt(args[1]);
            final int numberOfColumns = Integer.parseInt(args[2]);
            stress.populate(n, numberOfColumns);
        } else if (command.equals("load")) {
            final int n = Integer.parseInt(args[1]);
            final int numberOfColumns = Integer.parseInt(args[2]);
            stress.load(n, numberOfColumns, 10);
        }
    }
}
