package hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce4 extends MapReduceBase implements
Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {
		
		// chave traz o conjunto precedente
		// valor traz o conjunto completo das combinações geradas no job3 
		ArrayList<String> consequentsSets, issuedConsequentsSets;
		ArrayList<Integer> precedentSet, consequentSet;
		
		precedentSet = Util.stringSplit(key.toString().trim(), true);
		consequentsSets = new ArrayList<String>();
		issuedConsequentsSets = new ArrayList<String>();
		
		while (values.hasNext()) {
			
			consequentSet = Util.stringSplit(values.next().toString(), true);
			consequentSet.removeAll(precedentSet);

			for (int i = 1; i <= consequentSet.size(); i++) {
				consequentsSets = Util.allCombinationsK(consequentSet, i);
				for(String s : consequentsSets) {
					if (!issuedConsequentsSets.contains(s)) {
						output.collect(key, new Text(s));
						issuedConsequentsSets.add(s);
					}
				}
			}
		}
	}
}