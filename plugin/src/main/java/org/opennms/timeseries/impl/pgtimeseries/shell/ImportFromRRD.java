package org.opennms.timeseries.impl.pgtimeseries.shell;

import com.google.common.collect.Lists;
import org.apache.commons.io.FilenameUtils;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;

import org.opennms.integration.api.v1.timeseries.IntrinsicTagNames;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.StorageException;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.timeseries.impl.pgtimeseries.PGTimeseriesStorage;
import org.opennms.timeseries.impl.pgtimeseries.util.DBUtils;
import org.opennms.timeseries.impl.pgtimeseries.util.PGTimeseriesDatabaseInitializer;
import org.opennms.timeseries.impl.pgtimeseries.util.rrd.ResourcePath;
import org.opennms.timeseries.impl.pgtimeseries.util.rrd.AbstractDS;
import org.opennms.timeseries.impl.pgtimeseries.util.rrd.AbstractRRA;
import org.opennms.timeseries.impl.pgtimeseries.util.rrd.AbstractRRD;
import org.opennms.timeseries.impl.pgtimeseries.util.rrd.Row;
import org.opennms.timeseries.impl.pgtimeseries.util.rrd.RrdConvertUtils;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Command(scope = "opennms-pgtimeseries", name = "backfill-from-rrd", description = "Backfill time series metrics from local RRD (or jrb) files.")
@Service
public class ImportFromRRD implements Action {

    @Option(name = "-t", aliases = "--threads", description = "Number of import threads to spawn.  Defaults to the number of available processors.", required = false, multiValued = false)
    int threads = Runtime.getRuntime().availableProcessors();

    @Option(name = "-v", aliases = "--verbose", description = "Be verbose; show lots of output", required = false, multiValued = false)
    boolean verbose = false;

    @Option(name = "-b", aliases = "--batch-size --batchsize --batch", description = "Number of samples to batch out the the TSS ringbuffer at one time.", required = false, multiValued = false)
    int batchSize = 100;

    @Option(name = "--yes-really", description = "Yes, I really, really want to read and import all local rrd/jrb timeseries data.", required = false, multiValued = false)
    boolean yesreally = false;

    @Reference
    private DataSource dataSource;

    private final Map<String, Integer> MetricCache = new HashMap<String, Integer>();
    //private final Path onmsHome = Paths.get(System.getProperty("opennms.home"));
    private final Path rrdDir = Paths.get(System.getProperty("rrd.base.dir"));
    private final Boolean StoreByGroup = Boolean.parseBoolean(System.getProperty("org.opennms.rrd.storeByGroup"));
    private StorageStrategy storageStrategy;
    private enum StorageStrategy {
        STORE_BY_METRIC,
        STORE_BY_GROUP,
    }
    private StorageTool storageTool;
    private enum StorageTool {
        RRDTOOL,
        JROBIN,
    }
    private ExecutorService executor;
    private final Map<Integer, ForeignId> foreignIds = Maps.newHashMap();

    public Object execute() {

        if (!yesreally) {
            System.out.println();
            System.out.println("This utility performs significant work and can severely bloat and/or pollute your timeseries store. Providing '--yes-really' shows you understand the risks and is required.");
            System.out.println();
            return null;
        }

        // Initialize node ID to foreign ID mapping
        try (
            final Connection conn = dataSource.getConnection();
            final Statement st = conn.createStatement();
            final ResultSet rs = st.executeQuery("SELECT nodeid, foreignsource, foreignid from node n")) {
            while (rs.next()) {
                foreignIds.put(rs.getInt("nodeid"),
                        new ForeignId(rs.getString("foreignsource"),
                                rs.getString("foreignid")));
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        System.out.println("Found " + foreignIds.size() + " nodes on the database.");

        if (StoreByGroup) {
            storageStrategy = StorageStrategy.STORE_BY_GROUP;
            System.out.println("Using storeByGroup");
        }
        else {
            storageStrategy = StorageStrategy.STORE_BY_METRIC;
            System.out.println("Using store by metric");
        }

        if (System.getProperty("org.opennms.rrd.strategyClass") == null || System.getProperty("org.opennms.rrd.strategyClass").equals("org.opennms.netmgt.rrd.jrobin.JRobinRrdStrategy")) {
            storageTool = StorageTool.JROBIN;
            System.out.println("Using JRobin strategy");
        }
        else if (System.getProperty("org.opennms.rrd.strategyClass") != null && (System.getProperty("org.opennms.rrd.strategyClass").equals("org.opennms.netmgt.rrd.rrdtool.MultithreadedJniRrdStrategy") ||
                System.getProperty("org.opennms.rrd.strategyClass").equals("org.opennms.netmgt.rrd.rrdtool.JniRrdStrategy"))) {
            storageTool = StorageTool.RRDTOOL;
            System.out.println("Using RRDtool strategy");
        }

        try {
            System.out.println("Using " + threads + " threads for import");
            this.executor = new ForkJoinPool(threads,
                    ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                    null,
                    true);

        } catch (Exception e) {
            System.out.println(e.getMessage());
            return null;
        }

        try {
            System.out.print("Starting import.");
            this.processStoreByGroupResources(this.rrdDir.resolve("response"));
            switch (storageStrategy) {
                case STORE_BY_GROUP:
                    this.processStoreByGroupResources(this.rrdDir.resolve("snmp"));
                    break;

                case STORE_BY_METRIC:
                    this.processStoreByMetricResources(this.rrdDir.resolve("snmp"));
                    break;
            }
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }
        finally {
            this.executor.shutdown();
            boolean narf = false;
            while (!narf) {
                try {
                    narf = this.executor.awaitTermination(3, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    // pass
                }
                System.out.print(".");
            }
            System.out.println();
            System.out.println("Party on dudes!");
            System.out.println();
        }
        return null;
    }

    private void processStoreByGroupResources(final Path path) {
        try {
            // Find and process all resource folders containing a 'ds.properties' file
            Files.walk(path)
                    .filter(p -> p.endsWith("ds.properties"))
                    .forEach(p -> processStoreByGroupResource(p.getParent()));

        } catch (Exception e) {
            System.out.println("Error while reading RRD files: " + e);
        }
    }

    private void processStoreByGroupResource(final Path path) {
        // Load the 'ds.properties' for the current path
        final Properties ds = new Properties();
        try (final BufferedReader r = Files.newBufferedReader(path.resolve("ds.properties"))) {
            ds.load(r);

        } catch (final IOException e) {
            System.out.println("No group information found");
            return;
        }

        // Get all groups declared in the ds.properties and process the RRD files
        Sets.newHashSet(Iterables.transform(ds.values(), Object::toString))
                .forEach(group -> this.executor.execute(() -> processResource(path,
                        group,
                        group)));
    }

    private void processStoreByMetricResources(final Path path) {
        try {
            // Find an process all '.meta' files and the according RRD files
            Files.walk(path)
                    .filter(p -> p.getFileName().toString().endsWith(".meta"))
                    .forEach(this::processStoreByMetricResource);

        } catch (Exception e) {
            System.out.println("Error while reading RRD files: " + e);
        }
    }

    private void processStoreByMetricResource(final Path metaPath) {
        // Use the containing directory as resource path
        final Path path = metaPath.getParent();

        // Extract the metric name from the file name
        final String metric = FilenameUtils.removeExtension(metaPath.getFileName().toString());

        // Load the '.meta' file to get the group name
        final Properties meta = new Properties();
        try (final BufferedReader r = Files.newBufferedReader(metaPath)) {
            meta.load(r);

        } catch (final IOException e) {
            System.out.println("Failed to read .meta file '" + metaPath + "': " + e);
            return;
        }

        final String group = meta.getProperty("GROUP");
        if (group == null) {
            System.out.println("No group information found");
            return;
        }

        // Process the resource
        this.executor.execute(() -> this.processResource(path,
                metric,
                group));
    }

    /**
     * Process metric.
     *
     * @param resourceDir the path where the resource file lives in
     * @param fileName the RRD file name without extension
     * @param group the group name
     */
    private void processResource(final Path resourceDir,
                                 final String fileName,
                                 final String group) {
        if (verbose) {
            System.out.println("Processing resource: dir='" + resourceDir + "', file='" + fileName + "', group='" + group + "'");
        }
        final ResourcePath resourcePath = buildResourcePath(resourceDir);
        if (resourcePath == null) {
            return;
        }
        // Get the strings.properties
        final Properties properties = new Properties();
        try (final BufferedReader r = Files.newBufferedReader(resourceDir.resolve("strings.properties"))) {
            properties.load(r);
        } catch (final IOException e) {
            //pass
        }

        // Load the RRD file
        final Path file;
        switch (this.storageTool) {
            case RRDTOOL:
                file = resourceDir.resolve(fileName + ".rrd");
                break;

            case JROBIN:
                file = resourceDir.resolve(fileName + ".jrb");
                break;

            default:
                file = null;
        }

        if (!Files.exists(file)) {
            System.out.println("File not found: " + file);
            return;
        }

        final AbstractRRD rrd;
        try {
            switch (this.storageTool) {
                case RRDTOOL:
                    rrd = RrdConvertUtils.dumpRrd(file.toFile());
                    break;

                case JROBIN:
                    rrd = RrdConvertUtils.dumpJrb(file.toFile());
                    break;

                default:
                    rrd = null;
            }

        } catch (final Exception e) {
            System.out.println("Can't parse JRB/RRD file '"+ file +"':" + e);
            return;
        }

        try {
            this.injectSamples(resourcePath, group, rrd.getDataSources(), generateSamples(rrd), properties);
        } catch (final Exception e) {
            System.out.println("Failed to convert file '"+ file + "':" + e);
        }
    }

    private Sample toSample(AbstractDS ds, ResourcePath resourcePath, String group, Instant timestamp, double value, Properties props) {

        String type;
        if (ds.isCounter()) {
            type = "count";
        } else {
            type = "gauge";
        }

        ImmutableMetric.MetricBuilder metric = ImmutableMetric.builder()
                    .intrinsicTag(IntrinsicTagNames.resourceId, resourcePath.toString() + "/" + group)
                    .intrinsicTag(IntrinsicTagNames.name, ds.getName())
                    .metaTag("mtype", type);
        // Probably overkill.
        if (props != null && !props.isEmpty()) {
            Set<String> keys = props.stringPropertyNames();
            for (String k : keys) {
                metric.externalTag(k, props.getProperty(k));
            }
        }
        return ImmutableSample.builder()
                .time(timestamp)
                .value(value)
                .metric(metric.build())
                .build();
    }

    private void injectSamples(final ResourcePath resourcePath,
                                      final String group,
                                      final List<? extends AbstractDS> dataSources,
                                      final SortedMap<Long, List<Double>> samples,
                                      final Properties props) throws SQLException, StorageException {

        // Transform the RRD samples into OPA Samples
        List<Sample> batch = new ArrayList<>(this.batchSize);
        for (final Map.Entry<Long, List<Double>> s : samples.entrySet()) {
            for (int i = 0; i < dataSources.size(); i++) {
                final double value = s.getValue().get(i);
                if (Double.isNaN(value)) {
                    continue;
                }
                final AbstractDS ds = dataSources.get(i);
                final Instant timestamp = Instant.ofEpochSecond(s.getKey());

                try {
                    batch.add(toSample(ds, resourcePath, group, timestamp, value, props));
                } catch (IllegalArgumentException e) {
                    // This can happen when the value is outside of the range for the expected
                    // type i.e. negative for a counter, so we silently skip these
                    continue;
                }

                if (batch.size() >= this.batchSize) {
                    store(batch);
                    batch = new ArrayList<>(this.batchSize);
                }
            }
        }

        if (!batch.isEmpty()) {
            store(batch);
        }
    }

    private ResourcePath buildResourcePath(final Path resourceDir) {
        final ResourcePath resourcePath;
        final Path relativeResourceDir = this.rrdDir.relativize(resourceDir);

        // Transform store-by-id path into store-by-foreign-source path
        if (relativeResourceDir.startsWith(Paths.get("snmp")) &&
                !relativeResourceDir.startsWith(Paths.get("snmp", "fs"))) {

            // The part after snmp/ is considered the node ID
            final int nodeId = Integer.parseInt(relativeResourceDir.getName(1).toString());

            // Get the foreign source for the node
            final ForeignId foreignId = foreignIds.get(nodeId);
            if (foreignId == null) {
                return null;
            }

            // Make a store-by-foreign-source compatible path by using the found foreign ID and append the remaining path as-is
            resourcePath = ResourcePath.get(ResourcePath.get(ResourcePath.get("snmp", "fs"),
                            foreignId.foreignSource,
                            foreignId.foreignId),
                    Iterables.transform(Iterables.skip(relativeResourceDir, 2),
                            Path::toString));

        } else {
            resourcePath = ResourcePath.get(Iterables.transform(relativeResourceDir,
                    Path::toString));
        }
        return resourcePath;
    }

    public static SortedMap<Long, List<Double>> generateSamples(final AbstractRRD rrd) {
        // When having pdpPerRow = 1, the correlation function does not change the generated samples as the correlation
        // functions are reflexive wen applied to a single sample - `sample == cf([sample])` holds for all possible cf.
        // Therefore we use RRA with pdpPerRow = 1 which covers the longest time range regardless of its correlation
        // function. To fill up remaining samples, we only use RRA with AVERAGE correlation function.
        final NavigableSet<AbstractRRA> rras = Stream.concat(rrd.getRras()
                                .stream()
                                .filter(rra -> rra.getPdpPerRow() == 1L)
                                .max(Comparator.comparingInt(rra -> rra.getRows().size()))
                                .map(Stream::of)
                                .orElseGet(Stream::empty),
                        rrd.getRras()
                                .stream()
                                .filter(AbstractRRA::hasAverageAsCF))
                .collect(Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(AbstractRRA::getPdpPerRow))));

        final SortedMap<Long, List<Double>> collected = new TreeMap<>();

        for (final AbstractRRA rra : rras) {
            NavigableMap<Long, List<Double>> samples = generateSamples(rrd, rra);

            final AbstractRRA lowerRra = rras.lower(rra);
            if (lowerRra != null) {
                final long lowerRraStart = rrd.getLastUpdate() - lowerRra.getPdpPerRow() * rrd.getStep() * lowerRra.getRows().size();
                final long rraStep = rra.getPdpPerRow() * rrd.getStep();
                samples = samples.headMap((int) Math.ceil(((double) lowerRraStart) / ((double) rraStep)) * rraStep, false);
            }

            if (samples.isEmpty()) {
                // The whole timespan of this RRA is covered by an RRA with an higher resolution - so there is nothing
                // to add from this RRA
                continue;
            }

            final AbstractRRA higherRra = rras.higher(rra);
            if (higherRra != null) {
                final long higherRraStep = higherRra.getPdpPerRow() * rrd.getStep();
                samples = samples.tailMap((int) Math.ceil(((double) samples.firstKey()) / ((double) higherRraStep)) * higherRraStep, true);
            }

            collected.putAll(samples);
        }

        return collected;
    }

    public static NavigableMap<Long, List<Double>> generateSamples(final AbstractRRD rrd, final AbstractRRA rra) {
        final long step = rra.getPdpPerRow() * rrd.getStep();
        final long start = rrd.getStartTimestamp(rra);
        final long end = rrd.getEndTimestamp(rra);

        // Initialize Values Map
        final NavigableMap<Long, List<Double>> valuesMap = new TreeMap<>();
        for (long ts = start; ts <= end; ts += step) {
            final List<Double> values = new ArrayList<>();
            for (int i = 0; i < rrd.getDataSources().size(); i++) {
                values.add(Double.NaN);
            }
            valuesMap.put(ts, values);
        }

        // Initialize Last Values
        final List<Double> lastValues = new ArrayList<>();
        for (final AbstractDS ds : rrd.getDataSources()) {
            final double v = ds.getLastDs() == null ? 0.0 : ds.getLastDs();
            lastValues.add(v - v % step);
        }

        // Set Last-Value for Counters
        for (int i = 0; i < rrd.getDataSources().size(); i++) {
            if (rrd.getDataSource(i).isCounter()) {
                valuesMap.get(end).set(i, lastValues.get(i));
            }
        }

        // Process
        // Counters must be processed in reverse order (from latest to oldest) in order to recreate the counter raw values
        // The first sample is processed separated because the lastValues must be updated after adding each sample.
        long ts = end - step;
        for (int j = rra.getRows().size() - 1; j >= 0; j--) {
            final Row row = rra.getRows().get(j);

            for (int i = 0; i < rrd.getDataSources().size(); i++) {
                if (rrd.getDataSource(i).isCounter()) {
                    if (j > 0) {
                        final Double last = lastValues.get(i);
                        final Double current = row.getValue(i).isNaN() ? 0 : row.getValue(i);

                        Double value = last - (current * step);
                        if (value < 0) { // Counter-Wrap emulation
                            value += Math.pow(2, 64);
                        }
                        lastValues.set(i, value);
                        if (!row.getValue(i).isNaN()) {
                            valuesMap.get(ts).set(i, value);
                        }
                    }
                } else {
                    if (!row.getValue(i).isNaN()) {
                        valuesMap.get(ts + step).set(i, row.getValue(i));
                    }
                }
            }

            ts -= step;
        }

        return valuesMap;
    }
    private static class ForeignId {
        private final String foreignSource;
        private final String foreignId;

        public ForeignId(final String foreignSource,
                         final String foreignId) {
            this.foreignSource = foreignSource;
            this.foreignId = foreignId;
        }
    }
    public void store(List<Sample> entries) throws StorageException, SQLException {
        String sample_sql = "INSERT INTO pgtimeseries_time_series(time, keyid, value)  values (?, ?, ?)";
        String metric_sql = "INSERT INTO pgtimeseries_metric(keyid, key) VALUES(nextid('pgtimeseries_metric_seq'), ?) ON CONFLICT (key) DO NOTHING RETURNING keyid";

        final DBUtils db = new DBUtils(this.getClass());
        int batchSize = 0;
        Integer keyid = null;
        Connection connection = null;
        try {
            if (PGTimeseriesDatabaseInitializer.isExternalDatasourceURLAvailable()) {
                connection = PGTimeseriesDatabaseInitializer.getWhichDataSourceConnection();
                System.out.println("Store got connection: " + connection.toString());
            } else {
                connection = this.dataSource.getConnection();
            }
            db.watch(connection);
            PreparedStatement mps = connection.prepareStatement(metric_sql);
            PreparedStatement sps = connection.prepareStatement(sample_sql);
            db.watch(sps);
            db.watch(mps);
            // Partition the samples into collections smaller than max_batch_size
            for (List<Sample> batch : Lists.partition(entries, this.batchSize)) {
                System.out.println("Inserting "+ batch.size()+" samples");
                for (Sample sample : batch) {
                    if (MetricCache.containsKey(sample.getMetric().getKey())) {
                        keyid = MetricCache.get(sample.getMetric().getKey());
                    } else {
                        keyid = getMetricID(sample.getMetric().getKey());
                        if (keyid == null) {
                            mps.setString(1, sample.getMetric().getKey());
                            ResultSet mrs = mps.executeQuery();
                            db.watch(mrs);
                            while (mrs.next()) {
                                keyid = mrs.getInt("keyid");
                            }
                            mrs.close();
                        }
                        // update the cache
                        MetricCache.put(sample.getMetric().getKey(), keyid);
                    }
                    sps.setTimestamp(1, new Timestamp(sample.getTime().toEpochMilli()));
                    sps.setInt(2, keyid);
                    sps.setDouble(3, sample.getValue());
                    sps.addBatch();
                    storeTags(connection, keyid, ImmutableMetric.TagType.intrinsic, sample.getMetric().getIntrinsicTags());
                    storeTags(connection, keyid, ImmutableMetric.TagType.meta, sample.getMetric().getMetaTags());
                    storeTags(connection, keyid, ImmutableMetric.TagType.external, sample.getMetric().getExternalTags());
                }
                batchSize = batch.size();
                sps.executeBatch();
            }
        } catch (SQLException e) {
            System.out.println("An error occurred while inserting samples. Some sample may be lost." + e);
            throw new StorageException(e);
        } finally {
            db.cleanUp();
        }
        }

        private void storeTags(final Connection connection, final Integer keyid, final ImmutableMetric.TagType tagType, final Collection<Tag> tags) throws SQLException {
        final String sql = "INSERT INTO pgtimeseries_tag(keyid, key, value, type)  values (?, ?, ?, ?) ON CONFLICT (keyid, key, value, type) DO NOTHING;";
        final DBUtils db = new DBUtils(this.getClass());
        try {
            PreparedStatement ps = connection.prepareStatement(sql);
            db.watch(ps);
            for (Tag tag : tags) {
                ps.setInt(1, keyid);
                ps.setString(2, tag.getKey());
                ps.setString(3, tag.getValue());
                ps.setString(4, tagType.name());
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
        } finally {
            db.cleanUp();
        }
    }
    private Integer getMetricID (String key) {
        final DBUtils db = new DBUtils(this.getClass());
        Connection connection = null;
        Integer keyid = null;
        try {
            String sql = "select keyid from pgtimeseries_metric where key=?";
            if (PGTimeseriesDatabaseInitializer.isExternalDatasourceURLAvailable()) {
                connection = PGTimeseriesDatabaseInitializer.getWhichDataSourceConnection();
                System.out.println("getMetricID got connection: " + connection.toString());
            } else {
                connection = this.dataSource.getConnection();
            }
            db.watch(connection);

            PreparedStatement ps = connection.prepareStatement(sql);
            db.watch(ps);
            ResultSet rs = ps.executeQuery();
            db.watch(rs);
            // should only return one result
            while (rs.next()) {
                keyid = rs.getInt("keyid");
            }
            rs.close();
        } catch (SQLException e) {
            System.out.println("Error querying keyid for metric '"+key+"': " + e);
        }
        return keyid;
    }
}
