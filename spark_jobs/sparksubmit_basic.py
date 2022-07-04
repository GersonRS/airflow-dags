from pyspark import SparkConf, SparkContext

with open("archive.txt", "w") as arq:
    arq.write("aabdpqbaoibdbeuabdfiwobawbfbaofdbwbdfoababaowbbwfoqabab")
sc = SparkContext(conf=SparkConf()).getOrCreate()
logData = sc.textFile("archive.txt").cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
