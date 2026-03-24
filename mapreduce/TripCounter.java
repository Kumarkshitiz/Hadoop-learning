import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TripCounter {

    // MAPPER
    public static class TripMapper 
        extends Mapper<Object, Text, Text, IntWritable> {
        
        private final IntWritable one = new IntWritable(1);
        private Text location = new Text();
        private boolean isHeader = true;

        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
            
            String line = value.toString();
            
            // Skip header
            if (isHeader && line.contains("trip_id")) {
                isHeader = false;
                return;
            }
            
            String[] fields = line.split(",");
            if (fields.length > 5) {
                location.set(fields[10]); // pickup_location
                context.write(location, one);
            }
        }
    }

    // REDUCER
    public static class TripReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
            
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // DRIVER
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Trip Counter");
        
        job.setJarByClass(TripCounter.class);
        job.setMapperClass(TripMapper.class);
        job.setReducerClass(TripReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}