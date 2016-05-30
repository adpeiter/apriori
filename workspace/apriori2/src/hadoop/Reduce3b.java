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

public class Reduce3b extends MapReduceBase implements
Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {

		String comb, a, b;
		int qtda, qtdb, indexLastSpace, indexEval;
		ArrayList<Integer> la, lb;
		ArrayList<String> combs = new ArrayList<String>();
		
		while (values.hasNext()){
			comb = values.next().toString().replace("\t", " ").trim();
			comb = Util.stringJoin(Util.stringSplit(comb, true), Util.SEPARATOR);
			combs.add(comb);
		}
		
		Collections.sort(combs, new Comparator<String>() {
      @Override
      public int compare(String a, String b) {
          return Integer.compare(a.length(), b.length());
      }
		});
		
		indexEval = 0;

		while(true) {
			
			b = combs.get(indexEval);
			indexLastSpace = b.lastIndexOf(Util.SEPARATOR);
			qtdb = Integer.parseInt(b.substring(indexLastSpace+1));
			b = b.substring(0, indexLastSpace-1);
			lb = Util.stringSplit(b, true);

			for (int i = indexEval + 1; i < combs.size(); i++) {
				
				a = combs.get(indexEval);
				indexLastSpace = a.lastIndexOf(Util.SEPARATOR);
				qtda = Integer.parseInt(a.substring(indexLastSpace+1));
				a = a.substring(0, indexLastSpace-1);
				la = Util.stringSplit(a, true);
				
				if (APriori.printTransformations)
						System.out.println("Comparando conjuntos B = {" + b + "} e A = {" + a + "}...");
				
				if(la.containsAll(lb) && qtda == qtdb) {
					combs.remove(indexEval--);
						if (APriori.printTransformations)
							System.out.println("Removendo B (A contém B)...");
					break;
				}
			}
			if (++indexEval == combs.size())
				break;
		}
		
		/*
		ArrayList<String> combinations = new ArrayList<String>();
		ArrayList<String> distinctCombinations = new ArrayList<String>();
		ArrayList<Integer> la, lb;
		String a, b;
		int indexEval;
		
		while (values.hasNext()) {
			a = values.next().toString();
			combinations.add(a);
			if (!distinctCombinations.contains(a))
				distinctCombinations.add(a);
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
		
		for(String s : distinctCombinations)
			output.collect(new Text(s), Util.TEXT_EMPTY);
	*/
	}
}