chmod +x *.sh

echo "Put trainsition.txt and pr.txt into HDFS input/trans and HDFS input/pr0"
hdfs dfs -mkdir -p input
hdfs dfs -mkdir -p input/trans
hdfs dfs -mkdir -p input/pr0

hdfs dfs -mkdir output
hdfs dfs -rm -r output/res0

hdfs dfs -put transition.txt input/trans/
hdfs dfs -put pr.txt input/pr0

hadoop com.sun.tools.javac.Main *.java
jar cf pagerank.jar *.class

hadoop jar pagerank.jar Driver input/trans input/pr output/res $1 $2 

echo "PageRank results are in HDFS input/pr_i. i is the times of convergence you entered."
