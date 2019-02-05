from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
   
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    negativeCounts=[]
    positiveCounts=[]
    
    for element in counts:
        for key in element:
            if key[0] == "positive":
                positiveCounts.append(key[1])
            elif key[0] =="negative":
                negativeCounts.append(key[1])
                
                
    
    boundary = max(max(positiveCounts), max(negativeCounts)) * 1.1
    
    
    fig = plt.figure()
    plt.plot(positiveCounts, 'bo-')
    plt.plot(negativeCounts, 'go-')
    plt.xlabel("Time Step (10s)")
    plt.ylabel("Word counts")
    plt.axis([0, 12, 0, boundary])
    plt.legend(['positive', 'negative'], loc='upper left')
	#plt.show()
    fig.savefig('plot.png', bbox_inches='tight')
    



def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    # YOUR CODE HERE
    file = open(filename)
    text = file.read()
    listofwords = text.split('\n')
    return listofwords



def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])
    
    

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    
    def posOrNeg(word):
        if word in pwords:
            return ("positive", 1)
        
        if word in nwords:
            return ("negative", 1)
        
        return ("unknown", 1)
        
    def updateFunction(newVals, runningCount):
        if runningCount is None:
            runningCount = 0
        return sum(newVals, runningCount)
    
    wc = tweets.flatMap(lambda x: x.split(" ")).map(posOrNeg).reduceByKey(lambda x,y: x+y)
    
    runningCounts = wc.updateStateByKey(updateFunction)
    runningCounts.pprint()
    
    
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, co#unts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    wc.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
