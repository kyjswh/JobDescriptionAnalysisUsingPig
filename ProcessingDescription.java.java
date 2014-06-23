package myudfs;
import java.io.*;
import java.util.*;
 
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;



//Input: a tuple containing Key-value pair: Key: Job_ID, Value: Bag of tokens
//Output: a tuple containing (Job_ID, (word1, word2, word3....)) After a series of operations

public class ProcessingDescription extends EvalFunc<Tuple>{

	public int getDistance(String s1, String s2){
		return Levenshtein.getLevenshteinDistance(s1,s2);
	}

	public String getStemmed(String str){
		Porter p = new Porter();
		return p.stripAffixes(str);
	}

	public List<String> getCacheFiles(){
		List<String> list = new ArrayList<String>(2); 
        list.add("/user/huser38/Homework5/jobs/stopwords-en.txt#StopWords"); 
        list.add("/user/huser38/Homework5/jobs/dictionary.txt#dictionary");
        return list; 
	}

	//Add a list of distributed cache files
	public Tuple exec(Tuple input) throws IOException{
		if(input == null){
			return null;
		}

		//Iterator<Tuple> it = input.iterator();
		TupleFactory tf = TupleFactory.getInstance();
		Tuple t = tf.newTuple();


		String Job_ID = (String)input.get(0);
		DataBag db = (DataBag)input.get(1);


		Iterator<Tuple> it = db.iterator();

		t.append(Job_ID);

		
		
		//Add all the stop words into an arraylist
		FileReader fr = new FileReader("./StopWords");
		BufferedReader d = new BufferedReader(fr);

		List<String> StopWordsList = new ArrayList<String>();
		String thisLine;
		while((thisLine = d.readLine()) != null){
			StopWordsList.add(thisLine);
		}

		fr.close();

		//Add all the dictionary words into an arrayList
		FileReader fr2 = new FileReader("./dictionary");
		BufferedReader d2 = new BufferedReader(fr2);

		List<String> DictionaryList = new ArrayList<String>();

		String thisLine2;
		while((thisLine2 = d2.readLine()) != null){
			DictionaryList.add(thisLine2);
		}

		TupleFactory tf2 = TupleFactory.getInstance();
		Tuple Words_Tuple = tf2.newTuple();


		//Actual Processing step starts
		while(it.hasNext()){
			Tuple tp = it.next();
			String word = (String)tp.get(0);
			word = word.toLowerCase();
			System.out.println(word);
			if(!StopWordsList.contains(word)){
				//Stemming
				String word_new = getStemmed(word);
				//String word_new = word;
				int min_distance = getDistance(word_new,DictionaryList.get(0));
				String curWord = DictionaryList.get(0);
				for(String s:DictionaryList){
					if(s == word_new){
						curWord = s;
						break;
					}
					else{
						if(min_distance  >  getDistance(word_new,s)){
							min_distance = getDistance(word_new,s);
							curWord = s;
						}
					}
				}
				Words_Tuple.append(curWord);	
			}
		}

		t.append(Words_Tuple);

		return t;
	}
	
}
