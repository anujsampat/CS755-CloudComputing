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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.io.Text;


public class SpeciesIterMapper2 extends MapReduceBase implements Mapper<WritableComparable, Writable, Text, Text>
{
    private static long iterationNumber;
    private static long totalLinks = 1;

    public void map(WritableComparable key, Writable value,
                    OutputCollector output, Reporter reporter) throws IOException
    {
        // get the current page
        String data  = ((Text)value).toString();
        int    index = data.indexOf(":");
        
        if (index == -1)
        {
            String[] splits = value.toString().split("\t");

            if (splits.length == 0)
            {
                splits = value.toString().split(" ");

                if (splits.length == 0)
                {
                    return;
                }
            }

            String pagetitle = splits[0].trim();
            String pagerank  = splits[splits.length - 1].trim();
            
            output.collect(new Text(pagetitle), new Text("oldrank=" + pagerank));
            return;
        }

        // split into title and PR (tab or variable number of blank spaces)
        String toParse = data.substring(0, index).trim();
        String[] splits = toParse.split("\t");

        if (splits.length == 0)
        {
            splits = toParse.split(" ");

            if (splits.length == 0)
            {
                return;
            }
        }

        String pagetitle = splits[0].trim();
        String pagerank  = splits[splits.length - 1].trim();

        // parse current score
        double currScore = 0.0;
        try {
            currScore = Double.parseDouble(pagerank);
        } catch (Exception e) {
            currScore = 1.0;
        }

        // get number of outlinks
        data = data.substring(index + 1);
        String[] pages = data.split(" ");
        int numoutlinks = 0;

        if (pages.length == 0)
        {
            numoutlinks = 1;
        }
        else
        {
            for (String page : pages)
            {
                if (page.length() > 0)
                {
                    numoutlinks = numoutlinks + 1;
                }
            }
        }

        // collect each outlink, with the dampened PR of its inlink, and its inlink
        Text toEmit = new Text((new Double(.85 * currScore / numoutlinks)).toString());

        for (String page : pages)
        {
            if (page.length() > 0)
            {
                output.collect(new Text(page), toEmit);

                //output.collect(new Text(page), new  Text(" " + pagetitle));
            }
        }

        // collect the inlink with its damping factor, and all outlinks
        double selfRank = (1 - 0.85) / totalLinks;
        output.collect(new Text(pagetitle), new Text(Double.toString(selfRank)));

        output.collect(new Text(pagetitle), new Text(" " + data));

        // Output the rank of the current page prepended by the string "oldrank="
        // for the reducer to compare the newly calculated page rank against.
        output.collect(new Text(pagetitle), new Text("oldrank=" + Double.toString(currScore)));
    }

    public void configure(JobConf job)
    {
        iterationNumber = Long.parseLong(job.get("iterationNumber"));
        totalLinks = Long.parseLong(job.get("totalLinks"));
    }
}
