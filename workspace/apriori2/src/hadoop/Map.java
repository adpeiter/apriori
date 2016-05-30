package hadoop;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map extends MapReduceBase implements
Mapper<LongWritable, Text, Text, Text> {

	private static int lineNumber = 0;

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		lineNumber++;
		
		if(APriori.printInputs)
			System.out.println("INPUT MAP 0 : " + key + ", " + value);
		
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		Text k;
		
		while (tokenizer.hasMoreTokens()) {
			k = new Text(tokenizer.nextToken());
			output.collect(k, new Text(Integer.toString(lineNumber)));
			if(APriori.printOutputs)
				System.out.println("OUTPUT MAP 0: " + k + ", " + lineNumber);
		}
	}
}