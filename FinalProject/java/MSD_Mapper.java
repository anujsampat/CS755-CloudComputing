//
//   CS755 Final Project 
//  
//   Anuj Sampat
//   asampat@bu.edu
//
//   MSD_Mapper.java: The Map() implementation to process the Million Song Dataset
//                    using Hadoop.
//
//

package bu.as.cs755;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters.Counter;


public class MSD_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
{
    private static String searchRegion;
    private static String searchYear;
    private static String searchTempo;
    private static String searchMode;
    private static String searchKey;


    @SuppressWarnings("deprecation")
    public void map(LongWritable key, Text value,
                    OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException
    {
        boolean regionMatch = true;
        boolean yearMatch   = true;
        boolean tempoMatch  = true;
        boolean modeMatch   = true;
        boolean keyMatch    = true;

        String[] songInfo = value.toString().split("\t");

        // artist latitude and longitude
        if ((songInfo[6].equalsIgnoreCase("nan") != true) && (songInfo[8].equalsIgnoreCase("nan") != true))
        {
            float latitude  = Float.parseFloat(songInfo[6]);
            float longitude = Float.parseFloat(songInfo[8]);
        }

        // song year
        if (songInfo[songInfo.length - 1].equalsIgnoreCase("nan") != true)
        {
            if (searchYear.equalsIgnoreCase("all") != true)
            {
                int year = Integer.parseInt(songInfo[songInfo.length - 1]);
                String[] yearRange = searchYear.split("-");
                int yearMin = Integer.parseInt(yearRange[0]);
                int yearMax = Integer.parseInt(yearRange[1]);

                if ((year >= yearMin) && (year <= yearMax))
                {
                    yearMatch = true;
                }
                else
                {
                    yearMatch = false;
                }
            }
        }

        // song tempo
        if (songInfo[48].equalsIgnoreCase("nan") != true)
        {
            if (searchTempo.equalsIgnoreCase("all") != true)
            {
                double tempo = Double.parseDouble(songInfo[48]);

                if (searchTempo.equalsIgnoreCase("slow") && ((tempo >= 0) && (tempo < 160)))
                {
                    tempoMatch = true;
                }
                else if (searchTempo.equalsIgnoreCase("medium") && ((tempo >= 160) && (tempo < 320)))
                {
                    tempoMatch = true;
                }
                else if (searchTempo.equalsIgnoreCase("fast") && ((tempo > 320) && (tempo <= 500)))
                {
                    tempoMatch = true;
                }
                else
                {
                    tempoMatch = false;
                }
            }
        }

        // song mode
        if (songInfo[29].equalsIgnoreCase("nan") != true)
        {
            if (searchMode.equalsIgnoreCase("all") != true)
            {
                int mode = Integer.parseInt(songInfo[29]);

                if (searchMode.equalsIgnoreCase("major") && (mode == 1))
                {
                    modeMatch = true;
                }
                else if (searchMode.equalsIgnoreCase("minor") && (mode == 0))
                {
                    modeMatch = true;
                }
                else
                {
                    modeMatch = false;
                }
            }
        }

        // song key
        if (songInfo[26].equalsIgnoreCase("nan") != true)
        {
            if (searchKey.equalsIgnoreCase("all") != true)
            {
                int songKey  = Integer.parseInt(songInfo[26]);
                int inputKey = Integer.parseInt(searchKey);

                if (songKey == inputKey)
                {
                    keyMatch = true;
                }
                else
                {
                    keyMatch = false;
                }
            }
        }

        float dance = 0;

        if (songInfo[22].equalsIgnoreCase("nan") != true)
        {
            dance = (Float.parseFloat(songInfo[22])); // song danceability
        }

        Counter msdCounter = reporter.getCounter(bu.as.cs755.MSD_Driver.MSD_COUNTER.TOTAL_DANCE);

        if (msdCounter != null)
        {
            reporter.incrCounter(bu.as.cs755.MSD_Driver.MSD_COUNTER.TOTAL_DANCE, (long)dance);
        }

        Counter msdCounter2 = reporter.getCounter(bu.as.cs755.MSD_Driver.MSD_COUNTER.NUM_SONGS);

        if (msdCounter2 != null)
        {
            reporter.incrCounter(bu.as.cs755.MSD_Driver.MSD_COUNTER.NUM_SONGS, 1L);
        }

        float hotness;

        if (songInfo[43].equalsIgnoreCase("nan") != true)
        {
            hotness = (Float.parseFloat(songInfo[43])) * 100; // song hotness (0 to 100%)
        }
        else
        {
            hotness = 0; // not known
        }

        IntWritable mapKey = new IntWritable(new Integer((int) hotness));

        // song name + artist name + song year + song tempo + song mode + danceability
        String mapVal = songInfo[51] + "\t" + songInfo[12] + "\t" + songInfo[songInfo.length - 1] +
                        "\t" + songInfo[48] + "\t" + songInfo[29] + "\t" + songInfo[22];

        if (regionMatch && yearMatch && tempoMatch && modeMatch)
        {
            output.collect(mapKey, new Text(mapVal));
        }
    }

    //Called by the Hadoop framework to configure the job parameters before the
    // mapper is run.
    public void configure(JobConf job)
    {
        searchRegion = job.get("region");
        searchYear   = job.get("year");
        searchTempo  = job.get("tempo");
        searchMode   = job.get("mode");
        searchKey    = job.get("key");
    }
}

