package kmeans;

import com.sun.javafx.scene.control.skin.ContextMenuContent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.LogManager;

/**
 * @author Sushmita Chaudhari
 */
public class Kmeans extends Configured implements Tool {
    public enum MAX{
        MAX_FOLLOWER,
        SSE
    }
    private static final Logger logger = Logger.getLogger(Kmeans.class);

    /**
     * Mapper class to find the follower count of each user
     */
    public static class KmeansFollowerMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private static final IntWritable one = new IntWritable(1);
        private Text user = new Text();
        @Override
        public void map(Object key, Text value, Context context) throws InterruptedException, IOException
        {
            String edge = value.toString();
            String[] users = edge.split(",");
            user.set(users[1]);
            context.write(user, one);
        }
    }

    /**
     * Reducer class for follower count of each user
     */
    public static class KmeansFollowerReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private int maxfollower = 0;
        private IntWritable result = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws InterruptedException, IOException
        {
            int numFollowers = 0;
            for(IntWritable val : values)
            {
                numFollowers += val.get();
            }
            result.set(numFollowers);
            if(maxfollower < result.get())
            {
                maxfollower = result.get();
            }
            context.getCounter(MAX.MAX_FOLLOWER).setValue(maxfollower);
            context.write(key, result);
        }
    }

    /**
     * Mapper for KMeans clustering
     */
    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {
        List<Long> centroids = new ArrayList<>();

        /**
         * Setup method for kMeans
         * @param context
         * @throws IOException
         */
        @Override
        public void setup(Context context) throws IOException {
            int iteration;

            Configuration configuration = context.getConfiguration();
            iteration = configuration.getInt("iteration", 0);
            String filePath = "centroid";

            URI[] cacheFiles = context.getCacheFiles();
            if(cacheFiles!= null && cacheFiles.length > 0)
            {
                for(int i = 0; i < cacheFiles.length ;i++)
                {
                    try {
                        //change to "s3://Input/Path/" later
                        FileSystem fs = FileSystem.get(new Path(filePath+"_"+iteration).toUri(), configuration);
                        Path path = new Path(cacheFiles[i].toString());
                        System.out.println("Path = " + path.toUri().toString());
                        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                            String line;
                            while ((line = reader.readLine()) != null) {
                                centroids.add(Long.parseLong(line));
                            }
                        }

                    } catch (Exception E) {
                        logger.error("", E);
                    }
                }
            }

        }

        /**
         * Map for Kmeans: Calculates the distance between each data point and the centroid
         * @param key
         * @param value
         * @param context
         * @throws InterruptedException
         * @throws IOException
         */
        @Override
        public void map(Object key, Text value, Context context) throws InterruptedException, IOException
        {
            Text outkey = new Text();
            Text outvalue = new Text();
            String line = value.toString();

            String[] users = line.split(",");
            String followers = users[1];
            long numOfFollowers = Integer.parseInt(followers);
            long minDistance = Integer.MAX_VALUE;
            long updatedCentroid = centroids.get(0);
            long distance = 0;

            for(long centroid: centroids)
            {
                distance = Math.abs(centroid - numOfFollowers);
                if(distance < minDistance)
                {
                    updatedCentroid = centroid;
                    minDistance = distance;
                }
            }
            outkey.set(new Text(String.valueOf(updatedCentroid)));
            outvalue.set(new Text(String.valueOf(numOfFollowers)));

            context.write(outkey, outvalue);
        }
    }

    /**
     * Reducer for Kmeans
     */
    public static class KMeansReducer extends Reducer<Text, Text, NullWritable,Text>
    {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws InterruptedException, IOException {

            int sum = 0;
            int n = 0;
            int updatedCentroid = 0;
            double sse = 0;
            Text outvalue = new Text();
            while(values.iterator().hasNext())
            {
                int followers = Integer.parseInt(values.iterator().next().toString());
                sum = sum + followers;
                n += 1;

                double squaredError = Math.pow(Math.abs(Integer.parseInt(key.toString()) - followers), 2);
                sse = sse + squaredError;
            }
            if(n != 0)
            {
                updatedCentroid = sum/n;
            }
            else
                throw new IllegalStateException("n cannot be 0");

            context.getCounter(MAX.SSE).increment((long)sse);

            System.out.println("updated centroid: "+updatedCentroid);
            logger.info(updatedCentroid);

            outvalue.set(new Text(String.valueOf(updatedCentroid)));
            context.write(NullWritable.get(), outvalue);
        }
    }

    /**
     * Mapper for plotting graph
     */
    public static class KMeansGraphMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        IntWritable one  = new IntWritable(1);
        Text user = new Text();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            String[] users = line.split(",");
            user.set(users[1]);
            context.write(user, one);
        }
    }

    /**
     * Reducer for plotting graph
     */
    public static class KMeansGraphReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        IntWritable result = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
           int numberOfUsers = 0;
           for(IntWritable val : values)
           {
              numberOfUsers += val.get();
           }
           result.set(numberOfUsers);

           context.write(key, result);
        }
    }

    @Override
    public int run(String[] args) throws Exception
    {
        final int k = 4;
        long c1, c2, c3, c4;
        int i = 1;

        final Configuration configuration = getConf();
        final Job job  = Job.getInstance(configuration, "kmeans.Kmeans");
        job.setJarByClass(Kmeans.class);
        final Configuration jobConfiguration = job.getConfiguration();
        jobConfiguration.set("mapreduce.output.textoutputformat.separator",",");

        job.setMapperClass(KmeansFollowerMapper.class);
        job.setReducerClass(KmeansFollowerReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        Counters counter = job.getCounters();
        Counter counter1 = counter.findCounter(MAX.MAX_FOLLOWER);

        //Good start
        c1 = 0;
        c2 = c1 + counter1.getValue()/k;
        c3 = c2 + counter1.getValue()/k;
        c4 = c3 + counter1.getValue()/k;

        //bad start
        /*c1 = 0;
        c2 = 10;
        c3 = 20;
        c4 = 30;*/

        FileSystem hdfs = FileSystem.get(configuration );

        String filePath = "centroid";
        Path file = new Path(filePath+"_"+i+"/cen");
        if ( hdfs.exists( file )) { hdfs.delete( file, true ); }
        OutputStream os = hdfs.create( file);
        try (BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os))) {
            br.write(Long.toString(c1));
            br.write("\n");
            br.write(Long.toString(c2));
            br.write("\n");
            br.write(Long.toString(c3));
            br.write("\n");
            br.write(Long.toString(c4));
        }
        hdfs.close();

        long previousSSE = 0;
        long currentSSE = 0;
        //Job 2 configuration
        while(i <= 10) {

            if((previousSSE - currentSSE) == 0 && i != 1) {
                break;
            }

            final Configuration configuration2 = getConf();
            final Job job2  = Job.getInstance(configuration2, "kmeans.Kmeans");
            job2.setJarByClass(Kmeans.class);
            final Configuration jobConfiguration2 = job2.getConfiguration();
            jobConfiguration2.set("mapreduce.output.textoutputformat.separator","");

            //setting iteration number
            configuration2.setInt("iteration", i);

            job2.setMapperClass(KMeansMapper.class);
            job2.setReducerClass(KMeansReducer.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, new Path(args[1]));

            FileSystem fs = FileSystem.get(new Path(filePath+"_"+i).toUri(), configuration2);

            RemoteIterator<LocatedFileStatus> fileStatusListIterator =
                    fs.listFiles(new Path(filePath+"_"+i), true);

            while(fileStatusListIterator.hasNext()){
                LocatedFileStatus fileStatus = fileStatusListIterator.next();
                job2.addCacheFile(fileStatus.getPath().toUri());
            }
            int temp = i + 1;
            FileOutputFormat.setOutputPath(job2, new Path(filePath+"_"+temp));
            i++;

            job2.waitForCompletion(true);

            previousSSE = currentSSE;
            currentSSE = job2.getCounters().findCounter(MAX.SSE).getValue();
            logger.info("i: "+ i +" sse: "+ currentSSE);
            System.out.println("i: "+i+" sse: "+currentSSE);
        }

        //Configure job3 for Graph plotting

        final Configuration configuration3 = getConf();
        final Job job3  = Job.getInstance(configuration3, "kmeans.Kmeans");
        job3.setJarByClass(Kmeans.class);
        final Configuration jobConfiguration3 = job3.getConfiguration();
        jobConfiguration3.set("mapreduce.output.textoutputformat.separator","");
        job3.setMapperClass(KMeansGraphMapper.class);
        job3.setReducerClass(KMeansGraphReducer.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job3, new Path(args[1]));
        String filepath = "graph";
        FileOutputFormat.setOutputPath(job3, new Path(filepath));

        job3.waitForCompletion(true);


        return 0;
    }

    public static void main(final String[] args) {
        BasicConfigurator.configure();
        if(args.length != 2)
        {
            throw new Error("\"Two arguments required:\\n<input-dir> <output-dir>");
        }
        try
        {
            ToolRunner.run(new Kmeans(), args);
        }
        catch (final Exception e)
        {
            logger.error("", e);
        }
    }
}
