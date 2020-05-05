# Google PageRank Implementation with Hadoop MapReduce

## Background
PageRank is an algorithm used by Google Search to rank web pages in their search engine results. 
PageRank is a way of measuring the importance of website pages. 
PageRank works by counting the number and quality of links to a page 
to determine a rough estimate of how important the website is.

For more information about PageRank, please see [wiki](https://en.wikipedia.org/wiki/PageRank).

In this project, there are two main assumptions.
1. Quantity Assumption: More important websites are likely to receive more links from other websites.
2. Quality Assumption: Website with higher PageRank will pass higher weight.

## Implementation with MapReduce
There are two input txt files, one is 'transition.txt' to represent the relationship between websites,
another one is 'pr.txt' to represent the initial PageRank value for each website.

We use 2 MapReduce.
- MR1: Calculate the multiplication of transition matrix cell and PR matrix cell.
- MR2: Sum up cell for each page.

We also consider two common edge cases: dead ends and spider traps in PageRank.
To solve it, Teleporting with parameter 'beta' is exploited.

## Run PageRank
Same as project "hadoop_mapreduce_auto_complete", we run this on docker container.
The docker configuration is totally same.

In the container, start the Hadoop environment.
```shell script
./start-hadoop.sh
```
Run PageRank project.
```shell script
cd src/main/java
./run_pagerank.sh
```

