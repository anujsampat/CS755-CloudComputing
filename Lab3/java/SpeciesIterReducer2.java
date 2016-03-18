//

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

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapred.JobConf;

import BU.MET.CS755.SpeciesIterDriver2.ITERATION_COUNTER;


public class SpeciesIterReducer2 extends MapReduceBase implements Reducer<WritableComparable, Text, Text, Text>
{
    private static long iterationNumber;

    public void reduce(WritableComparable key, Iterator values,
                       OutputCollector output, Reporter reporter) throws IOException
    {
        double score    = 0;
        String outLinks = "";
        double oldScore = 0;

        // Counting links
        reporter.incrCounter(BU.MET.CS755.SpeciesIterDriver2.ITERATION_COUNTER.TOTAL_LINKS, 1L);

        if (iterationNumber == 1)
        {
            return;
        }

        while (values.hasNext())
        {
            String curr = ((Text)values.next()).toString();

            int colon   = curr.indexOf(":");
            int space   = curr.indexOf(" ");
            int oldrank = curr.indexOf("oldrank");

            if ((colon > -1))
            {
                String presScore = curr.substring(0, colon);
                try {
                    score   += Double.parseDouble(presScore);
                    oldScore = score;
                    outLinks = curr.substring(colon + 1);
                    continue;
                } catch (Exception e) {
                }
            }

            if (space > -1)
            {
                outLinks = curr;
            }
            else if (oldrank > -1)
            {
                oldScore = new Double(curr.substring(oldrank + 8));
            }
            else
            {
                score += Double.parseDouble(curr);
            }
        }

        String toEmit;

        if (outLinks.length() > 0)
        {
            toEmit = (new Double(score)).toString() + ":" + outLinks;
        }
        else
        {
            toEmit = (new Double(score)).toString();
        }

        // Output the new page rank
        output.collect(key, new Text(toEmit));

        double delta = oldScore - score;

        // Check how much the new page rank has changed. If the change is less
        // than two decimal places, treat it as a converged value. If not,
        // we need to re-calculate the rank with one more iteration; inform the
        // driver about that by incrementing the iterations needed counter.
        if ((delta > 0.009) || (delta < -0.009))
        {
            Counter myCounter2 = reporter.getCounter(BU.MET.CS755.SpeciesIterDriver2.ITERATION_COUNTER.ITERATIONS_NEEDED);

            if (myCounter2 != null)
            {
                reporter.incrCounter(BU.MET.CS755.SpeciesIterDriver2.ITERATION_COUNTER.ITERATIONS_NEEDED, 1L);
            }
        }
    }

    public void configure(JobConf job)
    {
        iterationNumber = Long.parseLong(job.get("iterationNumber"));
    }
}
