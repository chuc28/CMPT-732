from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# parse text to json object
def parse2json(line):
    return json.loads(line)

# get (subreddit, (count, score)) RDD
def get_kv(line):  
    yield(line["subreddit"], (1, line["score"]))

# add two pairs
def add_pairs(p1, p2):
    return (p1[0] + p2[0]), (p1[1] + p2[1])

# calculate the average score
def divide(p):
    return (p[0], p[1][1] / p[1][0])

# get key value
def get_key(kv):
    return kv[0]

# produce JSON as output
def set_format(record):
    return json.dumps(record)

# compute the relative score 
def get_score(b_value, comment):
    avg = float(b_value[comment[0]])
    score = float(comment[1]["score"])
    author = comment[1]["author"]
    return (score / avg, author)

def main(inputs, output):
    # parse to json object and cache it
    line = sc.textFile(inputs)
    text = line.map(parse2json).cache()
    
    # form (subreddit, avg) RDD
    json_obj = text.flatMap(get_kv).reduceByKey(add_pairs).map(divide).filter(lambda x: float(x[1]) > 0)
    # convert to Python object
    python_obj = dict(json_obj.collect())
    #broadcast a value and sent to each executor
    bro_obj = sc.broadcast(python_obj)

    # form a pair RDD with the subreddit as keys and comment data as values 
    commentdata = text.map(lambda c: (c['subreddit'], c))

    # join two RDDs
    # sort by the relative score (descending) and output to files
    comment = commentdata.map(lambda x: get_score(bro_obj.value, x)).sortBy(lambda x: -x[0]).map(set_format) 
    comment.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('relative score bcast')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
