/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.jet.AggregateOperation;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.TimestampKind;
import com.hazelcast.jet.TimestampedEntry;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.WindowDefinition;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.processor.Processors;
import com.hazelcast.jet.server.JetBootstrap;
import com.hazelcast.nio.IOUtil;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.AggregateOperations.averagingLong;
import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.WatermarkPolicies.withFixedLag;
import static com.hazelcast.jet.WindowDefinition.slidingWindowDef;
import static com.hazelcast.jet.processor.Processors.accumulateByFrame;
import static com.hazelcast.jet.processor.Processors.combineToSlidingWindow;
import static com.hazelcast.jet.processor.Processors.insertWatermarks;
import static com.hazelcast.jet.processor.Sinks.writeFile;
import static java.util.concurrent.TimeUnit.SECONDS;

public class FiveKJobs {

    private static final long BENCHMARK_TIMEOUT = SECONDS.toMillis(1800);

    public static void main(String[] args) throws Exception {
//        System.setProperty("hazelcast.logging.type", "log4j");

        if (args.length != 4) {
            System.err.println("Usage:");
            System.err.println("  " + FiveKJobs.class.getSimpleName()
                    + " <numJobs> <itemsPerSecondPerNode> <isCooperative> <numHeavyJobs(_=all)>");
            System.exit(1);
        }

        /* Number of jobs spawn */
        final int numJobs = Integer.parseInt(args[0].replace("_", ""));
        /* Items emitted per job per second, cluster-wide */
        final int itemsPerSecondPerNode = Integer.parseInt(args[1].replace("_", ""));
        final boolean cooperative = Boolean.parseBoolean(args[2]);
        final int heavyJobs = args[3].equals("_") ? Integer.MAX_VALUE : Integer.parseInt(args[3].replace("_", ""));

        if (itemsPerSecondPerNode % numJobs != 0) {
            System.err.println("Items per second must be an integer multiple of numJobs");
            System.exit(1);
        }

        Path directory = Files.createTempDirectory(FiveKJobs.class.getSimpleName());
        String sDirectory = directory + File.separator;

//        JetInstance instance = Jet.newJetInstance();
//        JetInstance instance2 = Jet.newJetInstance();

//        ClientConfig config = new ClientConfig();
//        config.getNetworkConfig().setAddresses(singletonList("10.212.1.101"));
//        config.getGroupConfig().setName("jet");
//        config.getGroupConfig().setPassword("jet-pass");
//        JetInstance instance = Jet.newJetClient(config);

        JetInstance instance = JetBootstrap.getInstance();


        Runtime.getRuntime().addShutdownHook(new Thread(() -> IOUtil.delete(directory.toFile())));

        try {
            int clusterSize = instance.getCluster().getMembers().size();
            // submit the jobs in parallel
            ExecutorService executor = Executors.newFixedThreadPool(20);
            System.out.println("Submitting " + numJobs + " jobs...");
            for (int i = 0; i < numJobs; i++) {
                int finalI = i;
                executor.submit(() -> {
                    JobConfig jobConfig = new JobConfig();
//                    jobConfig.addClass(FiveKJobs.class);
//                    jobConfig.addClass(RandomDataP.class);
                    try {
                        instance.newJob(buildDag(sDirectory + finalI, clusterSize,
                                finalI == 0 ? "lag" + finalI + "-" : null,
                                finalI < heavyJobs ? 0 : 10000,
                                finalI < heavyJobs ? itemsPerSecondPerNode / numJobs : 100, cooperative), jobConfig).execute();

                        System.out.println("job " + finalI + " submitted");
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                });
            }
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

            IAtomicLong lagTracker = instance.getHazelcastInstance().getAtomicLong("lag0-0");
            for (int i = 0; i < BENCHMARK_TIMEOUT / 1000; i++) {
                System.out.println("1st job emission lag: " + lagTracker.get());
                Thread.sleep(1000);
            }
        } finally {
            Jet.shutdownAll();
        }
    }

    private static DAG buildDag(String directory, int clusterSize, String lagTrackerPrefix, int minSleepTime,
                                int itemsPerSecond, boolean cooperative) {
        DAG dag = new DAG();

        DistributedFunction<Entry<Long, Integer>, Integer> keyExtractor =
                e -> Math.floorMod(e.getValue(), clusterSize * 10);
        WindowDefinition wDef = slidingWindowDef(10_000, 1_000);
        AggregateOperation<Entry<Long, Integer>, ?, Double> aggrOper = averagingLong(Entry::getValue);

        // All processors have local parallelism of 1: with so many expected
        // jobs, the parallelization will be achieved between jobs. This also
        // reduces per-job overhead as the number of queues between processors
        // is lower.
        Vertex source = dag.newVertex("source",
                () -> new RandomDataP(itemsPerSecond, cooperative, lagTrackerPrefix, minSleepTime))
                           .localParallelism(1);
        Vertex insertWms = dag.newVertex("insertWms",
                insertWatermarks(Entry<Long, Integer>::getKey, withFixedLag(0), emitByFrame(wDef)))
                .localParallelism(1);
        Vertex slidingWindowStage1 = dag.newVertex("slidingWindowStage1",
                accumulateByFrame(keyExtractor, Entry::getKey, TimestampKind.EVENT, wDef, aggrOper))
                .localParallelism(1);
        Vertex slidingWindowStage2 = dag.newVertex("slidingWindowStage2",
                combineToSlidingWindow(wDef, aggrOper))
                .localParallelism(1);
        Vertex mapToLatency = dag.newVertex("mapToLatency",
                Processors.map((TimestampedEntry e) -> entry(e.getKey(), System.currentTimeMillis() - e.getTimestamp())))
                .localParallelism(1);
        Vertex sink = dag.newVertex("sink", !cooperative ? writeFile(directory) : ProcessorSupplier.of(Processors.noop()))
                .localParallelism(1);

        dag.edge(between(source, insertWms).isolated())
           .edge(between(insertWms, slidingWindowStage1)
                   .partitioned(keyExtractor))
           .edge(between(slidingWindowStage1, slidingWindowStage2)
                   .partitioned(TimestampedEntry<Integer, Object>::getKey)
                   .distributed())
           .edge(between(slidingWindowStage2, mapToLatency).isolated())
           .edge(between(mapToLatency, sink).isolated());

        return dag;
    }
}
