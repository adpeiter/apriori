package hadoop;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.io.Text;

public class Util {
	
	public static String STRING_EMPTY = "";
	public static char SEPARATOR = ' ';
	public static Text TEXT_EMPTY = new Text("");
	
	public static ArrayList<Integer> stringSplit(String items, boolean sort) {
		
		ArrayList<Integer> tmpItems = new ArrayList<Integer>();
		
		try {
			
			for (String i : items.toString().trim().split(" ")) {
				tmpItems.add(Integer.parseInt(i.trim()));
			}
		
			if (sort)
				Collections.sort(tmpItems);
			
		}
		catch (Exception ex) {
			tmpItems.clear();
		}
		
		return tmpItems;
		
	}
	
	public static String stringJoin(ArrayList<Integer> items, char separator) {
		
		try {
			return org.apache.commons.lang.StringUtils.join(items, separator);
		}
		catch (Exception ex) {
			return STRING_EMPTY;
		}
		
	}
	
	static void combinationUtil(int arr[], int data[], int start,
			int end, int index, int r, ArrayList<String> combs)
	{
		// Current combination is ready to be printed, print it
		if (index == r)
		{
			combs.add(Arrays.toString(data).replace(",", "").replace("[", "").replace("]", ""));
			return;
		}

		// replace index with all possible elements. The condition
		// "end-i+1 >= r-index" makes sure that including one element
		// at index will make a combination with remaining elements
		// at remaining positions
		for (int i=start; i<=end && end-i+1 >= r-index; i++)
		{
			data[index] = arr[i];
			combinationUtil(arr, data, i+1, end, index+1, r, combs);
		}
	}

	// The main function that prints all combinations of size r
	// in arr[] of size n. This function mainly uses combinationUtil()
	static ArrayList<String> allCombinationsK(ArrayList<Integer> list, int r)
	{
		ArrayList<String> combs=new ArrayList<String>();
		
		int arr[] = new int[list.size()];
		
		for (int k = 0; k<list.size(); k++)
			arr[k] = list.get(k);
		
		int data[]=new int[r];

		combinationUtil(arr, data, 0, arr.length-1, 0, r, combs);
		return combs;
		
	}
	
	public static ArrayList<ArrayList<String>> generateAllCombinations(String set) {
		
		ArrayList<ArrayList<String>> combinations = new ArrayList<ArrayList<String>>();
		
		ArrayList<Integer> items = stringSplit(set, true);
		combinations = new ArrayList<ArrayList<String>>();
		
		for(int i = 0; i < items.size(); i++)
			combinations.add(new ArrayList<String>());
		
		for(int e : items)
			combinations.get(0).add(Integer.toString(e));
		
		String lastComb, lastItem;
		int lastCommaPos, lastItemPos, combs; 
		
		combs = items.size();
		
		for (int k = 1; k < combinations.size(); k++) {
			for (int j = 0; j < combinations.get(k-1).size(); j++) {
				lastComb = combinations.get(k-1).get(j);
				lastCommaPos = lastComb.lastIndexOf(" ");
				lastItem = lastCommaPos < 0 ? lastComb : lastComb.substring(lastCommaPos + 1);
				lastItemPos = items.indexOf(Integer.parseInt(lastItem));
				if (lastItemPos >= 0 && lastItemPos < items.size()-1) {
					for (int t = lastItemPos+1; t < items.size(); t++, combs++)
						combinations.get(k).add(lastComb + " " + Integer.toString(items.get(t)));
				}
				//System.out.println("Combs: " + combs);
			}
		}
		
		return combinations;
		
	}

}