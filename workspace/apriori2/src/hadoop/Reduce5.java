package hadoop;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import java.nio.file.Paths;

public class Reduce5 extends MapReduceBase implements
Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {
		
		
		ArrayList<Integer> precedentSet, consequentSet, transactionSet;
		String cs;
		int hits, misses, occurs;
		double confidence;
		
		ArrayList<String> lines = (ArrayList<String>) Files.readAllLines(Paths.get(APriori.dataBaseFile), Charset.forName("UTF-8"));
		
		precedentSet = Util.stringSplit(key.toString().trim(), true);
		
		// ler o arquivo de dados;
		// percorrer para toda chave/valor para calcular a confiança
		
		while (values.hasNext()) {
			
			hits = misses = 0;
			cs = values.next().toString();
			consequentSet = Util.stringSplit(cs, true);
			
			for (String s : lines){
				transactionSet = Util.stringSplit(s.trim(), true);
				if (transactionSet.containsAll(precedentSet))
					if(transactionSet.containsAll(consequentSet))
						hits++;
					else
						misses++;
			}
			
			occurs = hits + misses;
			if (occurs > APriori.minOccursItem) {
				confidence = (double)hits / (double)occurs;
				if(confidence >= APriori.confidence)
					output.collect(new Text(key.toString() + " -> " + cs + " C: " + confidence), Util.TEXT_EMPTY);
			}
		}
		
	}
}