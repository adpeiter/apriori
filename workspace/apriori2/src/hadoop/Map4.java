package hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map4 extends MapReduceBase implements
Mapper<LongWritable, Text, Text, Text> {

		@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

			ArrayList<String> combinationsForAssocRules = new ArrayList<String>();
			
			System.out.println(value);
			
			String v = value.toString().trim();
			Text vo = new Text(v);
			int nItens = StringUtils.countMatches(v, " ")+1;
			
			for(int i = 1; i < nItens; i++) {
				combinationsForAssocRules = Util.allCombinationsK(Util.stringSplit(value.toString().trim(), true), i);
				for(String s : combinationsForAssocRules) {
					output.collect(new Text(s), vo);
					System.out.println(vo + " -> " + s);
				}
			}

	}
}
