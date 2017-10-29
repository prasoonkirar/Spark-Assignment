import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

	sc=SparkContext('local[*]', 'CountHashTags');
	lines=sc.textFile("tweets-1.txt");
	transOne=lines.map(lambda x: x.replace(',',' ').replace('.',' ').replace('-',' ').replace(':',' ').replace('?',' '));
	transTwo=transOne.flatMap(lambda x:x.split(" "));
	filterTags=transTwo.filter(lambda h: '#' in h);
	hashTags=filterTags.map(lambda w:(w,1));
	counts=hashTags.reduceByKey(lambda x,y:x+y);
	sorte=counts.sortBy(lambda x:x[1] ,False);
	hashTag=sc.parallelize(sorte.take(100));
	hashTag.coalesce(1).saveAsTextFile("outputHash")


