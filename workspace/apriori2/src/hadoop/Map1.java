package hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map1 extends MapReduceBase implements
Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		int itemOccurs, tabIndex;
		Text itemCode;
		String line, a;
		
		line = value.toString();
		tabIndex = line.indexOf('\t');
		
		itemCode = new Text(line.substring(0, tabIndex));
		line = line.substring(tabIndex).trim();
		itemOccurs = org.apache.commons.lang.StringUtils.countMatches(line, " ") + 1;

		if (APriori.printInputs)
			System.out.println(itemCode + " >> " + itemOccurs);
		
		if (itemOccurs >= APriori.minOccursItem) {
		
			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens()) {
				a = tokenizer.nextToken();
				output.collect(new Text(a), itemCode);
			}
		}
	}
}
