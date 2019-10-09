from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# parse text to json object
def parse2json(line):
    return json.loads(line)

# get only the fields subreddit, score, and author
def geInfo(line):
    yield (line["subreddit"], line["score"], line["author"])

def main(inputs, output):
    # get only data from subreddits that contain an 'e' in their name
    text = sc.textFile(inputs).map(parse2json).flatMap(geInfo).filter(lambda x: 'e' in x[0])
    #text = sc.textFile(inputs).map(parse2json).flatMap(geInfo).filter(lambda x: 'e' in x[0]).cache()

    # and the rows with score greater than zero
    text.filter(lambda x: float(x[1]) > 0).map(json.dumps).saveAsTextFile(output + '/positive')

    # and the rows with score less than or equal to zero
    text.filter(lambda x: float(x[1]) <= 0).map(json.dumps).saveAsTextFile(output + '/negative')

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit etl')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
