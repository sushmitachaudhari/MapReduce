package rsJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * @author Sushmita Chaudhari
 */
public class RepJoin extends Configured implements Tool {
    public enum TRIANGLE_COUNTER {

        T_COUNTER_R
    }

    private static final Logger logger = LogManager.getLogger(MAXFilter.MAXFilterMapper.class);

    public static class ReplicatedJoinMapper extends Mapper<Object, Text, Text, Text>
    {
        private HashMap<String, List<String>> userFollower = new HashMap<>();

        private Text outValue = new Text();

        @Override
        public void setup(Context context) throws IOException, InterruptedException
        {
            try{

                URI[] files= context.getCacheFiles();
                Path filepath = new Path(files[0]);
                BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(filepath.toString()))));
                List<String> followees = new ArrayList<>();
                String line;

                while((line = reader.readLine()) != null)
                {
                  String[] users = line.split(",");
                  if(userFollower.containsKey(users[0]))
                  {
                      followees = userFollower.get(users[0]);
                      {
                          followees.add(users[1]);
                      }

                     userFollower.put(users[0], followees);
                  }
                  else
                  {
                      followees.add(users[1]);
                      userFollower.put(users[0], followees);
                  }
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }

        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            String[] users = line.split(",");

            List<String> followees;
            if(userFollower.containsKey(users[1]))
            {
                followees = userFollower.get(users[1]);
                if(followees.contains(users[0]))
                {
                    context.getCounter(RepJoin.TRIANGLE_COUNTER.T_COUNTER_R).increment(1);
                }
            }

        }
    }

    @Override
    public int run(final String[] args) throws Exception
    {
        final org.apache.hadoop.conf.Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "RepJoin");
        job.addCacheFile(new Path("/Users/sushmitachaudhari/CS6240/Assigments/Assignment2-MR/input/edge").toUri());
        job.setJarByClass(RepJoin.class);

        job.setMapperClass(ReplicatedJoinMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        Counters counter = job.getCounters();
        Counter c1 = counter.findCounter(RepJoin.TRIANGLE_COUNTER.T_COUNTER_R);
        System.out.println("counter rep join:"+ c1.getDisplayName()+":"+c1.getValue());

        return 0;

    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new RepJoin(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }


}
