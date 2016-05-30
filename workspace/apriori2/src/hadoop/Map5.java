package hadoop;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map5 extends MapReduceBase implements
Mapper<LongWritable, Text, Text, Text> {

		@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

			String v, k;
			int sepIndex;
			
			k = value.toString().trim();
			sepIndex = k.indexOf("\t");
			
			v = k.substring(sepIndex+1);
			k = k.substring(0, sepIndex);
			
			System.out.println(k + " -> " + v);
			
			output.collect(new Text(k), new Text(v));

	}
}