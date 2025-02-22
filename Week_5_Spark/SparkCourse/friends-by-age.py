from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)

def parseLine(line):
    fields = line.split(",")
    age = int(fields[2])
    numfriends = int(fields[3])
    return (age, numfriends)

lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
rdd = lines.map(parseLine)

# Map to (age, (numFriends, 1)) to prepare for aggregation
totalsByAge = rdd.map(lambda x: (x[0], (x[1], 1)))  # x[0]=age, x[1]=numfriends

# Reduce by key (age) to sum up total friends and counts
totalsByAge = totalsByAge.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))

# Compute the average number of friends per age
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

# Collect and print the results
results = averagesByAge.collect()
for result in results:
    print(result)
