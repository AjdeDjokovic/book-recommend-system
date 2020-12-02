package two;


public class FreqItemSetMain {
	public static void main(String[] args) throws Exception {
		String[] forPreJob = {"hdfs://localhost:9000/user/201700301147/apriori2/BX-Book-Ratings.txt", "hdfs://localhost:9000/user/201700301147/apriori2/PreJobOutput"};
		PreJob.main(forPreJob);
		
		String[] forFreqItemSet = {"hdfs://localhost:9000/user/201700301147/apriori2/PreJobOutput", "hdfs://localhost:9000/user/201700301147/apriori2/FreqItemSetOutput"};
		FreqItemSet.main(forFreqItemSet);
		
		String[] forFreqItemSet2 = {"hdfs://localhost:9000/user/201700301147/apriori2/PreJobOutput", "hdfs://localhost:9000/user/201700301147/apriori2/FreqItemSetOutput2"};
		FreqItemSet2.main(forFreqItemSet2);
		
		String[] forCountCL = {"hdfs://localhost:9000/user/201700301147/apriori2/FreqItemSetOutput", "hdfs://localhost:9000/user/201700301147/apriori2/FreqItemSetOutput2","hdfs://localhost:9000/user/201700301147/apriori2/CountCLOutput"};
		CountCL.main(forCountCL);
		
//		String[] forPreJob = {"hdfs://localhost:9000/apriori2/BX-Book-Ratings.txt", "hdfs://localhost:9000/apriori2/PreJobOutput"};
//		PreJob.main(forPreJob);
//		
//		String[] forFreqItemSet = {"hdfs://localhost:9000/apriori2/PreJobOutput", "hdfs://localhost:9000/apriori2/FreqItemSetOutput"};
//		FreqItemSet.main(forFreqItemSet);
//		
//		String[] forFreqItemSet2 = {"hdfs://localhost:9000/apriori2/PreJobOutput", "hdfs://localhost:9000/apriori2/FreqItemSetOutput2"};
//		FreqItemSet2.main(forFreqItemSet2);
		
//		String[] forCountCL = {"hdfs://localhost:9000//apriori2/FreqItemSetOutput", "hdfs://localhost:9000/apriori2/FreqItemSetOutput2","hdfs://localhost:9000/apriori2/CountCLOutput"};
//		CountCL.main(forCountCL);
	}

}
