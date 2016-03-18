//
//   CS755 Final Project
//
//   Anuj Sampat
//   asampat@bu.edu
//
//   MSD_Driver.java: MapReduce driver for launching a MR job on the Amazon EMR cluster.
//
//

package bu.as.cs755;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;


public class MSD_Driver
{
    public static enum MSD_COUNTER
    {
        TOTAL_DANCE,
        NUM_SONGS
    };


    @SuppressWarnings("deprecation")
    public static void main(String[] args)  throws Exception
    {
        Job theJob = null;
        JobClient client = new JobClient();
        JobConf   conf   = new JobConf(MSD_Driver.class );
        conf.setJobName("MSD_Job");

        conf.setMapperClass(MSD_Mapper.class );
        conf.setReducerClass(MSD_Reducer.class );
        conf.setOutputKeyComparatorClass(LongWritable.DecreasingComparator.class );

        conf.setMapOutputKeyClass(IntWritable.class );
        conf.setMapOutputValueClass(Text.class );

        conf.set("region", args[2]);
        conf.set("year", args[3]);
        conf.set("tempo", args[4]);
        conf.set("mode", args[5]);
        conf.set("key", args[6]);

        // take the input and output from the command line
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        try {
            theJob = new Job(conf, "MSD");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        try {
            if (theJob != null)
            {
                theJob.waitForCompletion(true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        long totalDance = 0;
        long numSongs   = 0;
        try {
            if (theJob.isComplete())
            {
                Counters jobCtrs = theJob.getCounters();

                if (jobCtrs != null)
                {
                    totalDance = jobCtrs.findCounter(MSD_COUNTER.TOTAL_DANCE).getValue();
                }

                if (jobCtrs != null)
                {
                    numSongs = jobCtrs.findCounter(MSD_COUNTER.NUM_SONGS).getValue();
                }
            }
        }
        catch (Exception ex) {}

        float avgDance = (float)(totalDance / numSongs);

        System.out.println("The avg danceability was: " + avgDance);
    }
}
