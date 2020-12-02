package apriori;

public class FreqItemSetMain {
	public static void main(String[] args) throws Exception {
//		String[] forPreJob = {"hdfs://localhost:9000/user/201700301147/apriori/BX-Book-Ratings.txt", "hdfs://localhost:9000/user/201700301147/apriori/PreJobOutput"};
//		PreJob.main(forPreJob);
//		
//		String[] forFreqItemSet = {"hdfs://localhost:9000/user/201700301147/apriori/PreJobOutput", "hdfs://localhost:9000/user/201700301147/apriori/FreqItemSetOutput"};
//		FreqItemSet.main(forFreqItemSet);
//		
//		String[] forFreqItemSet2 = {"hdfs://localhost:9000/user/201700301147/apriori/PreJobOutput", "hdfs://localhost:9000/user/201700301147/apriori/FreqItemSetOutput2"};
//		FreqItemSet2.main(forFreqItemSet2);
		
		String[] forCountCL = {"hdfs://localhost:9000/user/201700301147/apriori/FreqItemSetOutput", "hdfs://localhost:9000/user/201700301147/apriori/FreqItemSetOutput2","hdfs://localhost:9000/user/201700301147/apriori/CountCLOutput"};
		CountCL.main(forCountCL);
	}

}
