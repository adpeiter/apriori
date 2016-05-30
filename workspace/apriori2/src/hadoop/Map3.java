package hadoop;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map3 extends MapReduceBase implements
Mapper<LongWritable, Text, Text, Text> {

		@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

			if (APriori.printInputs)
				System.out.println("INPUT MAP 3: " + value);

			System.out.println("M3 " + value);
			
			String v = value.toString().trim();
			
			output.collect(new Text("1"), new Text(v));

	}
}
