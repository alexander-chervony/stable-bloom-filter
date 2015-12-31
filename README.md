#### Scala implementation of Stable Bloom Filter

StableBloomFilterTest description:
	- ```"several items put in filter"``` - simplistic unit test
	- ```"error rates are within acceptable boundaries"``` - experiment with algorythm parameters on your real data (use files with real data instead of resource file)
	- ```"count distinct lines"``` - code to analyze how many duplicates are in your real data (in our case we had timestamped stream of tuples so there is a code that works with time, ignoring line changes caused by timestamp solely)