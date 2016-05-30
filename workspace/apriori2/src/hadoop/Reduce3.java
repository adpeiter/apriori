package hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce3 extends MapReduceBase implements
Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {

		ArrayList<String> combinations = new ArrayList<String>();
		ArrayList<String> distinctCombinations = new ArrayList<String>();
		ArrayList<Integer> la, lb;
		String a, b;
		int indexEval;
		
		System.out.println("Reduce 3");
		
		while (values.hasNext()) {
			a = values.next().toString();
			if (a.indexOf(Util.SEPARATOR) < 0)
				continue;
			combinations.add(a);
			if (!distinctCombinations.contains(a))
				distinctCombinations.add(a);
			
			System.out.println(a);
			
		}
		
		Collections.sort(distinctCombinations, new Comparator<String>() {
      @Override
      public int compare(String a, String b) {
          return Integer.compare(a.length(), b.length());
      }
		});
		
		indexEval = 0;

		while(true) {
			
			b = distinctCombinations.get(indexEval);
			lb = Util.stringSplit(b, true);
			
			for (int i = indexEval + 1; i < distinctCombinations.size(); i++) {
				
				a = distinctCombinations.get(i);	
				la = Util.stringSplit(a, true);
				
				if (APriori.printTransformations)
						System.out.println("Comparando conjuntos B = {" + b + "} e A = {" + a + "}...");
				
				if(la.containsAll(lb) && Collections.frequency(combinations, a) == Collections.frequency(combinations, b)) {
					distinctCombinations.remove(indexEval--);
						if (APriori.printTransformations)
							System.out.println("Removendo B (A contém B)...");
					break;
				}
			}
			if (++indexEval == distinctCombinations.size())
				break;
		}
		
		for(String s : distinctCombinations) {
			System.out.println(s + " " + Collections.frequency(combinations, s));
			output.collect(new Text(s), Util.TEXT_EMPTY);
		}
	
	}
}