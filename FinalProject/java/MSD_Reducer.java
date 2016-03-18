//
//
//   CS755 Final Project
//
//   Anuj Sampat
//   asampat@bu.edu 
//
//   MSD_Reducer.java: The Reduce() function to process the Million Song Dataset
//                     using Hadoop.
//
//

package bu.as.cs755;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MSD_Reducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>
{
    public void reduce(IntWritable key, Iterator<Text> values,
                       OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException
    {
        while (values.hasNext())
        {
            int keyNeg = 1 * key.get();
            IntWritable outKey = new IntWritable(keyNeg);
            output.collect(outKey, values.next());
        }
    }
}

