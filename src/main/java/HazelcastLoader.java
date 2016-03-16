import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

/**
 * Data loader for Hazelcast from HDFS.
 */
public class HazelcastLoader {

    private static Logger logger = Logger.getLogger(HazelcastLoader.class);

    public static class HazelcastCSVMapper extends Mapper<Object, Text, Object, Object> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString();
            int keyIndex = Integer.valueOf(context.getConfiguration().get("key-index"));
            StringTokenizer tokenizer = new StringTokenizer(val, ",");

            int count = 0;
            while (tokenizer.hasMoreTokens()) {
                String nextToken = tokenizer.nextToken();
                if (count++ == keyIndex) {
                    context.write(nextToken, val);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length < 3) {
            logger.info("Usage: HazelcastLoader <input-file> <type[csv|parquet]> <key-indemvnx>");
            System.exit(2);
        }
        String filePath = remainingArgs[0];
        String type = remainingArgs[1];
        String keyIndex = remainingArgs[2];
        conf.set("key-index", keyIndex);

        Job job = Job.getInstance(conf, "Hazelcast Data Loader");
        job.setJarByClass(HazelcastLoader.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(HazelcastOutputFormat.class);
        Path path = new Path(filePath);
        setMapperAndInputFormatClass(job, path, type);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void setMapperAndInputFormatClass(Job job, Path path, String type) throws IOException {
        if ("csv".equals(type)) {
            job.setMapperClass(HazelcastCSVMapper.class);
            FileInputFormat.addInputPath(job, path);
        } else {
            throw new IllegalArgumentException("Unsupported file type : " + type);
        }
    }
}
