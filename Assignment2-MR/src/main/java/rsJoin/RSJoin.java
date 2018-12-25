package rsJoin;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;


/** @author Sushmita Chaudhari
 */


public class RSJoin extends Configured implements Tool {

    public enum TRIANGLE_COUNTER {

        T_COUNTER
    }

    private static final Logger logger = LogManager.getLogger(MAXFilter.MAXFilterMapper.class);

    public static class UserJoinMapper extends Mapper<Object, Text, Text, Text>
    {
       private Text outkey = new Text();
       private Text outvalue = new Text();

       private Text outkey1 = new Text();
       private Text outvalue1 = new Text();


       //mapper for length-2 paths
       @Override
       public void map(Object key, Text value, Context context)
               throws IOException, InterruptedException
       {
           String line = value.toString();
           String[] users = line.split(",");

           outkey.set(users[0]);
           outvalue.set("L"+ value.toString());

           outkey1.set(users[1]);
           outvalue1.set("R"+ value.toString());

           context.write(outkey, outvalue);
           context.write(outkey1, outvalue1);
       }

    }

    //read the edges mapper to map the edges file so that we can later
    public static class UserJoinMapperTemp extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            outkey.set(line);
            outvalue.set("B");
            System.out.println("outkey from edges "+outkey+" "+outvalue);
            context.write(outkey, outvalue);
        }
    }

    //to read the temp file mapper
    public static class UserJoinMapperTemp1 extends Mapper<Object, Text, Text, Text>
    {
        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] users = line.split("\\s");

            String firstUser = users[0];
            String secondUser = users[1];

            String[] firstUsers = firstUser.split(",");
            String[] secondUsers = secondUser.split(",");

            outkey.set(firstUsers[1]+","+secondUsers[0]);
            outvalue.set("A");

            System.out.println("outkey, outvalue: "+ outkey+" "+outvalue);

            context.write(outkey, outvalue);

        }
    }

    public static class UserJoinReducer extends Reducer<Text, Text, Text, Text> {
        private ArrayList<Text> listLeft = new ArrayList<>();
        private ArrayList<Text> listRight = new ArrayList<>();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            listLeft.clear();
            listRight.clear();

            for (Text text : values) {
                if (text.charAt(0) == 'L') {
                    listLeft.add(new Text(text.toString().substring(1)));
                } else if (text.charAt(0) == 'R') {
                    listRight.add(new Text(text.toString().substring(1)));
                }
            }

            if (!listLeft.isEmpty() && !listRight.isEmpty()) {
                for (Text left : listLeft) {
                    for (Text right : listRight) {
                        context.write(left, right);
                    }
                }
            }

        }
    }

    public static class UserJoinReducerTemp extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
        {
            boolean flagA = false;
            boolean flagB = false;

            //check if values has A nad B then increase the count.
            for(Text value : values)
            {
                if(value.toString().equals("A"))
                {
                    flagA = true;
                }

                if(value.toString().equals("B"))
                {
                    flagB = true;
                }

            }

            if(flagA && flagB)
            {
                context.getCounter(TRIANGLE_COUNTER.T_COUNTER).increment(1);
            }

        }

    }


    @Override
    public int run(final String[] args) throws Exception {

        String temp = "Users/sushmitachaudhari/CS6240/Assigments/Assignment2-MR/temp";

        final org.apache.hadoop.conf.Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "RSJoin");
        job.setJarByClass(MAXFilter.class);
        final org.apache.hadoop.conf.Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
        // Delete output directory, only to ease local development; will not work on AWS. ===========
        //		final FileSystem fileSystem = FileSystem.get(conf);
        //		if (fileSystem.exists(new Path(args[1]))) {
        //			fileSystem.delete(new Path(args[1]), true);
        //		}
        // ================
        job.setMapperClass(RSJoin.UserJoinMapper.class);
        //job.setCombinerClass(MAXFilterReducer.class);
        job.setReducerClass(RSJoin.UserJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(temp));

        job.waitForCompletion(true);

        final org.apache.hadoop.conf.Configuration conf1 = getConf();
        final Job job1 = Job.getInstance(conf1, "RSJoin");
        job1.setJarByClass(MAXFilter.class);
        final org.apache.hadoop.conf.Configuration jobConf1 = job1.getConfiguration();
        jobConf1.set("mapreduce.output.textoutputformat.separator", "\t");

        MultipleInputs.addInputPath(job1, new Path(args[0]),
                TextInputFormat.class, UserJoinMapperTemp.class);

        MultipleInputs.addInputPath(job1, new Path(temp),
                TextInputFormat.class, UserJoinMapperTemp1.class);

        job1.setReducerClass(RSJoin.UserJoinReducerTemp.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);
        Counters counter = job1.getCounters();
        Counter c1 = counter.findCounter(TRIANGLE_COUNTER.T_COUNTER);
        System.out.println("counter:"+ c1.getDisplayName()+":"+c1.getValue());

        return 0;
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new RSJoin(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}
