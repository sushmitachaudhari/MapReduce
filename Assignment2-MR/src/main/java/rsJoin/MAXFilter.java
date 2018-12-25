package rsJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Sushmita Chaudhari
 */

public class MAXFilter extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(MAXFilterMapper.class);

    public static class MAXFilterMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static final int MAX = 1000;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] users = line.split(",");

                if(Integer.parseInt(users[0]) < MAX && Integer.parseInt(users[1]) < MAX)
                {
                    context.write(value, new Text());
                }
            }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, " MAXFilter ");
        job.setJarByClass(MAXFilter.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
        // Delete output directory, only to ease local development; will not work on AWS. ===========
        //		final FileSystem fileSystem = FileSystem.get(conf);
        //		if (fileSystem.exists(new Path(args[1]))) {
        //			fileSystem.delete(new Path(args[1]), true);
        //		}
        // ================
        job.setMapperClass(MAXFilterMapper.class);
        //job.setCombinerClass(MAXFilterReducer.class);
        //job.setReducerClass(MAXFilterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new MAXFilter(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }


}
