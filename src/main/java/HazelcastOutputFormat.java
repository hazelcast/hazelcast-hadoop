import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

/**
 * OutputFormat for writing key/value pairs to Hazelcast Map.
 */
public class HazelcastOutputFormat extends OutputFormat {

    private final String HAZELCAST_TARGET_MAP = "hazelcast.target.map";
    private ClientConfig config;
    private static Logger logger = Logger.getLogger(HazelcastOutputFormat.class);


    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        logger.info("Starting Hazelcast Client!");
        HazelcastInstance client = createHazelcastClient();
        return new HazelcastRecordWriter(client);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new HazelcastOutputCommitter();
    }


    private HazelcastInstance createHazelcastClient() throws IOException {
        File f = new File("hazelcast-client.xml");
        XmlClientConfigBuilder builder = new XmlClientConfigBuilder(f);
        config = builder.build();
        return HazelcastClient.newHazelcastClient(config);
    }


    class HazelcastRecordWriter<K, V> extends RecordWriter<K, V> {

        private HazelcastInstance client;

        public HazelcastRecordWriter(HazelcastInstance client) {
            this.client = client;
        }

        @Override
        public void write(K key, V value) throws IOException, InterruptedException {
            String targetMap = config.getProperty(HAZELCAST_TARGET_MAP);
            IMap<Object, Object> map = client.getMap(targetMap);
            map.put(key, value);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            logger.info("Shutting down Hazelcast Client!");
            client.getLifecycleService().shutdown();
        }
    }

    class HazelcastOutputCommitter extends OutputCommitter {

        @Override
        public void setupJob(JobContext jobContext) throws IOException {
        }

        @Override
        public void setupTask(TaskAttemptContext taskContext) throws IOException {
        }

        @Override
        public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
            return false;
        }

        @Override
        public void commitTask(TaskAttemptContext taskContext) throws IOException {
        }

        @Override
        public void abortTask(TaskAttemptContext taskContext) throws IOException {
        }
    }
}
