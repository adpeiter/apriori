package hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce extends MapReduceBase implements
Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {

		StringBuilder lines = new StringBuilder();
		
		while (values.hasNext()) {
			lines.append(" ").append(values.next().toString());
		}
		if(lines.length()>0)
			lines.delete(0, 1);
		
		output.collect(key, new Text(lines.toString()));
		
	}
}