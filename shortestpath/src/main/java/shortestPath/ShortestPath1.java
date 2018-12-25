package shortestPath;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author Sushmita Chaudhari
 */
public class ShortestPath1 extends Configured implements Tool {
    public enum COUNTER {

        LOOP_COUNTER,
        MAX

    }
    private static final Logger logger = LogManager.getLogger(ShortestPath1.class);

    /**
     * Mapper class for Adjacency list creation
     */
    public static class ADJMapper extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        private Text outkey1 = new Text();
        private Text outvalue1 = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] users = line.split(",");

            outkey.set(users[0]);
            outvalue.set('F'+ value.toString());

            outkey1.set(users[1]);
            outvalue1.set('T'+ value.toString());

            context.write(outkey, outvalue);
            context.write(outkey1, outvalue1);
        }
    }

    /**
     * Reducer for Adj list creator
     */
    public static class ADJReducer extends Reducer<Text, Text, Text, Text> {
        private ArrayList<Text> adjList = new ArrayList<>();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            adjList.clear();
            Text listADJ = new Text();
            Text value = new Text();

            String source = context.getConfiguration().get("source");

            for (Text text : values) {
                if (text.charAt(0) == 'F') {
                    adjList.add(new Text(text.toString().substring(1).split(",")[1]));
                }
            }

            //creation of object N:
            StringBuilder list1 = new StringBuilder();
            list1.append(listADJ);

            String infinity = Long.toString(Long.MAX_VALUE);
            // adjacency list
            for(int i = 0; i < adjList.size(); i++)
            {
                list1.append(adjList.get(i));
                if(i < adjList.size() - 1)
                {
                    list1.append(",");
                }
            }

            //add the active and distance flags
            if(key.toString().equals(source))
            {
                list1.append(" ");
                list1.append("true"); //if source node
                list1.append(" ");
                list1.append("0");
            }
            else
            {
                list1.append(" ");
                list1.append("false");
                list1.append(" ");
                list1.append(infinity);
            }
            value.set(list1.toString());
            context.write(key, value);
        }
    }

    /**
     * Mapper for shortest path
     */
    public static class ShortestPathMapper extends Mapper<Object, Text, Text, Text> {
        private Logger logger = Logger.getLogger(this.getClass());

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            String line = value.toString();
            String[] users = line.split("\\s");

            //emiting the id n, vertex N
            Text keyN = new Text();
            Text lineTextValue = new Text();

            keyN.set(users[0]);
            lineTextValue.set(line);

            context.write(keyN, lineTextValue);

            //second part of the algo
            String distance; //distance

            Text outkey = new Text();
            Text outvalue = new Text();

            if(users[2].equals("true"))
            {
                distance = users[3];
                String[] adjList = users[1].split(",");

                for(int i = 0; i < adjList.length; i++)
                {
                    outkey.set(adjList[i]);
                    distance = Long.toString(Long.parseLong(distance) + 1);

                    outvalue.set(distance);
                    context.write(outkey, outvalue);
                }
            }
        }
    }

    /**
     * Reducer for the Shortest Path
     */
    public static class ShortestPath1Reducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            long minDistance = Long.MAX_VALUE;
            Text vertexM = new Text();
            for(Text text : values)
            {
                boolean isNotVertex = text.find("true") == -1 && text.find("false") == -1;
                if(isNotVertex)
                {
                    long actualDistance = Long.parseLong(text.toString());
                    if( actualDistance < minDistance)
                    {
                        minDistance = actualDistance;
                    }
                }
                else
                {
                    vertexM.set(text);
                }
            }

            String vertexMLine = vertexM.toString();
            String[] vertexMArray = vertexMLine.split(" ");

            if(minDistance < Long.parseLong(vertexMArray[3]))
            {
                vertexMArray[2] = "true";
                //both counters
                context.getCounter(COUNTER.LOOP_COUNTER).increment(1);
                vertexMArray[3] = ""+minDistance;

                //update the global
                Counter max = context.getCounter(COUNTER.MAX);

                if(minDistance > max.getValue())
                {
                    context.getCounter(COUNTER.MAX).setValue(minDistance);
                }
            }
            String[] vertexMArrayTemp = Arrays.copyOfRange(vertexMArray, 1, vertexMArray.length);
            context.write(key, new Text(String.join(" ",vertexMArrayTemp)));
        }
    }
        @Override
        public int run(final String[] args) throws Exception {
            final org.apache.hadoop.conf.Configuration conf = getConf();
            final Job job = Job.getInstance(conf, " Shortest Path ");

            String temp = "/Users/sushmitachaudhari/Fall18/sushmitacha/ASSIGNMENT-3/shortestpath/temp/";

            job.setJarByClass(ShortestPath1.class);
            final org.apache.hadoop.conf.Configuration jobConf = job.getConfiguration();
            jobConf.set("mapreduce.output.textoutputformat.separator", " ");
            jobConf.set("source", "1");

            job.setMapperClass(ShortestPath1.ADJMapper.class);
            job.setReducerClass(ShortestPath1.ADJReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(temp));
            job.waitForCompletion(true);

            //job1
            Counter c1;
            long diameter = 0;
            int i = 0;

            do{
                final org.apache.hadoop.conf.Configuration conf1 = getConf();
                final Job job1 = Job.getInstance(conf1, "Shortest Path");
                job1.setJarByClass(ShortestPath1.class);

                final org.apache.hadoop.conf.Configuration jobConf1 = job1.getConfiguration();
                jobConf1.set("mapreduce.output.textoutputformat.separator", " ");

                job1.setMapperClass(ShortestPath1.ShortestPathMapper.class);
                job1.setReducerClass(ShortestPath1.ShortestPath1Reducer.class);

                job1.setOutputKeyClass(Text.class);
                job1.setOutputValueClass(Text.class);

                /* termination condition */
                if(i == 0)
                {
                    FileInputFormat.addInputPath(job1, new Path(temp));
                    FileOutputFormat.setOutputPath(job1, new Path(args[1]+i));
                }
                else
                {
                    String input = args[1]+(i-1);
                    String output1 = args[1]+i;
                    FileInputFormat.addInputPath(job1, new Path(input));
                    FileOutputFormat.setOutputPath(job1, new Path(output1));
                }
                job1.waitForCompletion(true);
                i++;
                Counters counter = job1.getCounters();
                c1 = counter.findCounter(COUNTER.LOOP_COUNTER);

                Counters counter2 = job1.getCounters();

                if(counter2.findCounter(COUNTER.MAX).getValue() >  diameter)
                {
                   diameter = counter2.findCounter(COUNTER.MAX).getValue();
                }
            }
            while(c1.getValue() > 0);

            logger.info("approx. graph diameter "+diameter);
            return 0;
        }

    public static void main(final String[] args) {
        BasicConfigurator.configure();
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir> <temp-dir>");
        }

        try
        {
            ToolRunner.run(new ShortestPath1(), args);
        }
        catch (final Exception e) {
            logger.error("", e);
        }
    }
}

