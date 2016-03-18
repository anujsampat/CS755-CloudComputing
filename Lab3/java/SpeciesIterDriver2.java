// Author - Jack Hebert (jhebert@cs.washington.edu)
// Copyright 2007
// Distributed under GPLv3
//
// Modified - Dino Konstantopoulos (dinok@bu.edu)
// Copyright 2010, BU MET CS 755 Cloud Computing
// Distributed under the "If it works, remolded by Dino Konstantopoulos,
// otherwise no idea who did! And by the way, you're free to do whatever
// you want to with it" dinolicense
//
package BU.MET.CS755;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.streaming.StreamInputFormat;


public class SpeciesIterDriver2
{
    private static int numExtraIterations = 2;	
    private static String  inputpath  = null;
    private static String  outputpath = null;
    private static JobConf conf = null;

    public static enum ITERATION_COUNTER
    {
        ITERATION_NUMBER,
        ITERATIONS_NEEDED,
        TOTAL_LINKS
    };

    public static void main(String[] args)
    {
        int iterCnt = 0;

        if (args.length < 2)
        {
            System.out.println("Usage: PageRankIter <input path> <output path>");
            System.exit(0);
        }

	// Generate list of species from XML, with an initial rank of 1 and
	// followed by the sub-species list.
        MRGraphBuilder(args, iterCnt);

	// Iterate and generate ranks for the species. 
        MRSpeciesRank(args, iterCnt);

	// Output the final ranks.
        MRSpeciesView(outputpath, args);
    }

    static boolean MRGraphBuilder(String args[], int iterCnt)
    {
        Job theJob = null;

        conf = new JobConf(SpeciesIterDriver2.class );
        conf.setJobName("Species Graph Builder");
        conf.setNumReduceTasks(5);
        conf.setOutputKeyClass(Text.class );
        conf.setOutputValueClass(Text.class );
        conf.setMapperClass(SpeciesGraphBuilderMapper.class );
        conf.setReducerClass(SpeciesGraphBuilderReducer.class );

	// Reading in XML.
        conf.setInputFormat(StreamInputFormat.class );
        conf.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader");

	// Look for the <page> record in the XML.
        conf.set("stream.recordreader.begin", "<page>");
        conf.set("stream.recordreader.end", "</page>");

        inputpath  = args[0];
        outputpath = args[1] + iterCnt;

        FileInputFormat.setInputPaths(conf, new Path(inputpath));
        FileOutputFormat.setOutputPath(conf, new Path(outputpath));

        try {
            theJob = new Job(conf, "SpeciesIter");
            theJob.submit();
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

        return true;
    }

    static boolean MRSpeciesRank(String args[], int iterCnt)
    {
        long newCounterVal   = 0;
        long totalLinks      = 1; // Initialize to 1 to prevent divide by zero
        long totalIterations = 0;
        Job  theJob = null;

        conf = new JobConf(SpeciesIterDriver2.class );
        conf.setJobName("Species Iter");
        conf.setNumReduceTasks(5);
        conf.setOutputKeyClass(Text.class );
        conf.setOutputValueClass(Text.class );
        conf.setMapperClass(SpeciesIterMapper2.class );
        conf.setReducerClass(SpeciesIterReducer2.class );

        boolean nextIterationNeeded = true;

        while (nextIterationNeeded || numExtraIterations != 0)
        {
            long iterationNumber = 0;

            if ((iterCnt == 0) || (iterCnt == 1))
            {
                inputpath = args[1] + "0";
            }
            else
            {
                inputpath = args[1] + iterCnt;
            }

            iterCnt++;

            conf.set("iterationNumber", Integer.toString(iterCnt));
            conf.set("totalLinks", Long.toString(totalLinks));

            outputpath = args[1] + iterCnt;

            FileInputFormat.setInputPaths(conf, new Path(inputpath));
            FileOutputFormat.setOutputPath(conf, new Path(outputpath));

            try {
                theJob = new Job(conf, "SpeciesIter");
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

            try {
                if (theJob.isComplete())
                {
                    Counters jobCtrs = theJob.getCounters();

                    if (jobCtrs != null)
                    {
                        newCounterVal = jobCtrs.findCounter(ITERATION_COUNTER.ITERATIONS_NEEDED).getValue();
                    }

                    // If reducer recorded change in species rank, repeat iteration.
                    if ((newCounterVal > 0) || (iterCnt == 1))
                    {
                        nextIterationNeeded = true;
                    }
                    else
                    {
                        nextIterationNeeded = false;
			numExtraIterations--; // Do one extra iteration
                    }

                    totalLinks = jobCtrs.findCounter(BU.MET.CS755.SpeciesIterDriver2.ITERATION_COUNTER.TOTAL_LINKS).getValue();
                }

                totalIterations += 1;

                if (totalIterations > 200)
                {
                    System.out.println("too many iterations!!");
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        System.out.println("Total iterations = " + totalIterations);

        return true;
    }

    static boolean MRSpeciesView(String input, String args[])
    {
        Job theJob = null;

        JobConf conf = new JobConf(SpeciesIterDriver2.class );
        conf.setJobName("Species Viewer");

        conf.setOutputKeyClass(FloatWritable.class );
        conf.setOutputValueClass(Text.class );

        inputpath  = input;
        outputpath = args[1] + "FinalRanks";

        FileInputFormat.setInputPaths(conf, new Path(inputpath));
        FileOutputFormat.setOutputPath(conf, new Path(outputpath));

        conf.setMapperClass(SpeciesViewerMapper.class );
        conf.setReducerClass(org.apache.hadoop.mapred.lib.IdentityReducer.class );

        try {
            theJob = new Job(conf, "SpeciesIter");
            theJob.waitForCompletion(true);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        return true;
    }
}
