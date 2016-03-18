package org.myorg;

import java.io.IOException;
import java.io.File;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Job;


/*
 * Apache Hadoop MapReduce program to extract IP hits with a hit count > 100 from the Apache log.
 * The final output is sorted in descending order of hit count. The MapReduce (MR) is carried out
 * in two steps:
 *
 * MR Job 1: MapReduce job to read an IP from the Apache Log and output the IP and its total hit count.
 *
 * MR Job 2: MapReduce job to read the output of Job 1, sort it according to descending hit count and
 *           output the IP and its hit count.
 *
 */
public class ApacheIPHitCnt
{
    // Output directories
    private static final String ipCntDir = new String("/ipCnt"); // IP hit counts output here
    private static final String ipCntSortedDir = new String("/ipCntSorted"); // Sorted IP hit counts output here

    /*
     * Job 1 Mapper: IpMapper class to output IP records from the Apache Log. Outputs every IP entry with a hit
     * count of one.
     */
    public static class IpMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
    {
        // Regular expression to match an IP entry in the Apache log.
        private static final Pattern ipPattern = Pattern.compile("^([\\d\\.]+)\\s");

        // Reusable IntWritable for the IP count.
        private static final IntWritable one = new IntWritable(1);

        public void map(LongWritable fileOffset, Text lineContents,
                        OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
        {
            if (lineContents != null)
            {
                Text theIP = new Text();

                // Apply the regex to the line of the access log.
                Matcher matcher = ipPattern.matcher(lineContents.toString());

                // IP found
                if (matcher.find())
                {
                    theIP.set(matcher.group()); // Grab the IP
                    output.collect(theIP, one); // Output it with a count of 1
                }
            }
        }
    }

    /*
     * Job 1 Reducer: IpReducer class to count and output (total hits > 100) from a given IP in the Apache Log.
     *                Outputs an IP and its total hit count.
     */
    public static class IpReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text ip, Iterator<IntWritable> counts,
                           OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
        {
            int totalCount = 0;

            // Sanity check
            if ((ip == null) || (counts == null))
            {
                return;
            }

            // Loop over the count and tally it up.
            while (counts.hasNext())
            {
                IntWritable count = counts.next();
                totalCount += count.get();
            }

            // Output the IP and hit count if > 100.
            if (totalCount > 100)
            {
                output.collect(ip, new IntWritable(totalCount));
            }
        }
    }


    /*
     * Job 2 Mapper: IpSorterMapper class that takes the (key<IP>, value<count>) output from the Job 1 Reducer as its
     * input and outputs it as (key<count>, value<IP>) to obtain a descending sort on the count.
     */
    public static class IpSorterMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
    {
        public void map(LongWritable fileOffset, Text lineContents,
                        OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException
        {
            if (lineContents != null)
            {
                String line = lineContents.toString();
                StringTokenizer tokenizer = new StringTokenizer(line);

                // Need exactly two tokens (the IP, followed by its hit count).
                if (tokenizer.countTokens() == 2)
                {
                    Text ip = new Text();
                    ip.set(tokenizer.nextToken());
                    IntWritable count = new IntWritable(Integer.parseInt(tokenizer.nextToken()));

                    output.collect(count, ip); // Output as (count, IP)
                }
            }
        }
    }


    /*
     * Job 2 Reducer: IpSorterReducer class that takes the (key<count>, value<IP>) output from the Job 2 Mapper as its
     * input and outputs it as (key<IP>, value<count>).
     */
    public static class IpSorterReducer extends MapReduceBase implements Reducer<IntWritable, Text, Text, IntWritable>
    {
        public void reduce(IntWritable count, Iterator<Text> ip,
                           OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
        {
            // Simply reverse the reducer input from (count, IP) to (IP, count) and output it.
            if (count != null &&ip != null)
            {
                // Single key may be mapped to multiple values,
                // so output all.
                while (ip.hasNext())
                {
                    output.collect(ip.next(), count);
                }
            }
        }
    }

    /*
     * Main program to create and execute:
     *
     * Job 1: MapReduce job to read an IP from the Apache Log and output its total hit count.
     *
     * Job 2: MapReduce job to sort the output of Job 1 according to descending hit count.
     */
    public static void main(String[] args) throws Exception
    {
        // Configure Job 1.
        JobConf Job1Conf = new JobConf(ApacheIPHitCnt.class );

        if (Job1Conf != null)
        {
            Job1Conf.setOutputKeyClass(Text.class );
            Job1Conf.setOutputValueClass(IntWritable.class );

            Job1Conf.setMapperClass(IpMapper.class );
            Job1Conf.setCombinerClass(IpReducer.class );
            Job1Conf.setReducerClass(IpReducer.class );

            Job1Conf.setInputFormat(TextInputFormat.class );
            Job1Conf.setOutputFormat(TextOutputFormat.class );

            // Set input path to the Apache log.
            FileInputFormat.setInputPaths(Job1Conf, new Path(args[0]));

            // IP and hit count output in directory 'ipCnt'.
            FileOutputFormat.setOutputPath(Job1Conf, new Path(args[1] + ipCntDir));

            // Create Job 1 and wait for it to finish.
            Job Job1 = new Job(Job1Conf, "apacheIPCnt");

            if (Job1 != null)
            {
                Job1.waitForCompletion(true);
            }
        }

        // Configure Job 2.
        JobConf Job2Conf = new JobConf(ApacheIPHitCnt.class );

        if (Job2Conf != null)
        {
            Job2Conf.setJobName("apacheIPCntSort");

            // Configure mapper output.
            Job2Conf.setMapOutputKeyClass(IntWritable.class );
            Job2Conf.setMapOutputValueClass(Text.class );

            // Configure reducer output.
            Job2Conf.setOutputKeyClass(Text.class );
            Job2Conf.setOutputValueClass(IntWritable.class );

            Job2Conf.setMapperClass(IpSorterMapper.class );
            Job2Conf.setReducerClass(IpSorterReducer.class );

            // Set descending sort.
            Job2Conf.setOutputKeyComparatorClass(LongWritable.DecreasingComparator.class );

            Job2Conf.setInputFormat(TextInputFormat.class );
            Job2Conf.setOutputFormat(TextOutputFormat.class );

            // Input taken from the output of Job 1.
            FileInputFormat.setInputPaths(Job2Conf, new Path(args[1] + ipCntDir));

            // Sorted hit counts output in directory 'ipCntSorted'
            FileOutputFormat.setOutputPath(Job2Conf, new Path(args[1] + ipCntSortedDir));

            // Execute Job 2.
            JobClient.runJob(Job2Conf);
        }
    }
}
