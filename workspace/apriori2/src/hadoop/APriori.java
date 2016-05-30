package hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class APriori {

	public static int transactions = 10;
	public static int minOccursItem;
	public static double confidence = 0.1;
	public static double support = 0.1;
	public static String dataBaseFile = "/home/adpeiter/testes_apriori/test2.dat";
	public static boolean printOutputs = false;
	public static boolean printInputs = false;
	public static boolean printTransformations = false;
	
	public static void main(String[] args) throws Exception {

		minOccursItem = (int)Math.ceil(transactions * support);

		System.out.println("Suporte mínimo: " + minOccursItem);
		
		JobConf conf = new JobConf(APriori.class);
		conf.setJobName("apriori");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		// Job 0
		// map emite todos os itens como chave, com a respectiva linha de ocorrência
		// reduce emite o par iten/lista de linhas de ocorrência
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(dataBaseFile));
		FileOutputFormat.setOutputPath(conf, new Path("/home/adpeiter/testes_apriori/result_0"));

		JobClient.runJob(conf);

		// Job 1
		// map emite apenas o par linha, item dos itens com tamanho da lista de ocorrências >= suporte
		// reduce junta todos os itens da mesma linha novamente, gerando uma coleção como o arquivo original, mas apenas com os itens que satisfazem o suporte mínimo
		conf.setMapperClass(Map1.class);
		conf.setReducerClass(Reduce1.class);

		FileInputFormat.setInputPaths(conf, new Path("/home/adpeiter/testes_apriori/result_0/part-00000"));
		FileOutputFormat.setOutputPath(conf, new Path("/home/adpeiter/testes_apriori/result_1"));

		JobClient.runJob(conf);

		// Job 2
		// map emite todas as linhas do arquivo gerado pelo Job anterior ordenando os itens de forma crescente em cada linha
		// reduce gera todas as combinações de cada linha (combinação como chave)
		conf.setMapperClass(Map2.class);
		conf.setReducerClass(Reduce2.class);

		FileInputFormat.setInputPaths(conf, new Path("/home/adpeiter/testes_apriori/result_1/part-00000"));
		FileOutputFormat.setOutputPath(conf, new Path("/home/adpeiter/testes_apriori/result_2"));

		JobClient.runJob(conf);

		// Job 3
		// map emite o par (combinação, 1) (combinações como chave)
		// reduce grava as combinações com a quantidade de ocorrências (apenas as com suporte mínimo)
		conf.setMapperClass(Map3.class);
		conf.setReducerClass(Reduce3.class);

		FileInputFormat.setInputPaths(conf, new Path("/home/adpeiter/testes_apriori/result_2/part-00000"));
		FileOutputFormat.setOutputPath(conf, new Path("/home/adpeiter/testes_apriori/result_3"));

		JobClient.runJob(conf);
	
		// Job 4
		conf.setMapperClass(Map4.class);
		conf.setReducerClass(Reduce4.class);

		FileInputFormat.setInputPaths(conf, new Path("/home/adpeiter/testes_apriori/result_3/part-00000"));
		FileOutputFormat.setOutputPath(conf, new Path("/home/adpeiter/testes_apriori/result_4"));

		JobClient.runJob(conf);

		// Job 5
		conf.setMapperClass(Map5.class);
		conf.setReducerClass(Reduce5.class);

		FileInputFormat.setInputPaths(conf, new Path("/home/adpeiter/testes_apriori/result_4/part-00000"));
		FileOutputFormat.setOutputPath(conf, new Path("/home/adpeiter/testes_apriori/result_5"));

		JobClient.runJob(conf);

		System.out.println("Concluído...");

	}
}