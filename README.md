#### Stable Bloom Filter implementation in Scala

StableBloomFilterTest description:

- ```"several items put in filter"```: simplistic unit test

- ```"count distinct lines"```: code to analyze how many duplicates are in your real data (in our case we had timestamped stream of tuples so there is a code that works with time, ignoring line changes caused by timestamp solely)

- ```"error rates are within acceptable boundaries"```: experiment with algorythm parameters on your real data (use files with real data instead of resource file). Original doc has good examples of the parameters on different data sets: https://webdocs.cs.ualberta.ca/~drafiei/papers/DupDet06Sigmod.pdf
