### 图书推荐系统
1. 概述
	* apriori文件夹为数据操作。hadoop进行数据处理，apriori算法获得频繁项集。
	* 数据集 [http://www2.informatik.uni-freiburg.de/~cziegler/BX/](下载)
	* test文件夹为java web项目。maven管理
	
2. 安装
	
	* 安装maven
	
	* 可直接使用处理完成的数据，导入freq_item.sql数据库文件
	* freq_item数据库中多个表为不同置信度、支持度阈值时的数据结果，任选其一即可
	* 注意更改java web项目中数据库用户名等，本项目连接username:user1,password:1
	* 进入test目录，启动java web项目
		```shell
		mvn tomcat:7 run
		```
	
3. 自行处理数据
  * 如想自己处理数据，需要自行配置hadoop环境需重更改apriori文件夹中FreqItemSetMain类中数据地址字符串等
  * 可根据需要更改置信度、支持度阈值

***
### Book recommendation system
1. Overview

	* The apriori folder is a data operation. Hadoop is used for data processing, and Apriori algorithm is used to obtain frequent itemsets.

	* Data set [ http://www2.informatik.uni-freiburg.de/~cziegler/BX/ ](download)

	* The test folder is a Java Web project. Maven management

2. Installation

	* Installation maven

	* The processed data can be directly used to import freq_ item.sql Database file

	* freq_ If there are multiple tables in the item database with different confidence levels and support thresholds, you can choose one of them

	* Pay attention to change the database user name in the Java Web project, and connect the project username:user1 , password:1
	
	* Enter the test directory and start the Java Web project
		```shell
		mvn tomcat:7 run
		```
	
3. Self processing data

	* If you want to process data by yourself, you need to configure Hadoop environment by yourself, and you need to change the data address string in the freqitemsetmain class in the apriori folder
	* The confidence and support thresholds can be changed as needed
