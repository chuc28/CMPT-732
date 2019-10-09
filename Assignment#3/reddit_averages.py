from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+


def get_kv(line):  
    dict = json.loads(line)
    yield(dict["subreddit"], (1, dict["score"]))

def add_pairs(p1, p2):
    return (p1[0] + p2[0]), (p1[1] + p2[1])

def divide(p):
    return (p[0], p[1][1] / p[1][0])

def get_key(kv):
    return kv[0]

def set_format(record):
    return json.dumps(record)

def main(inputs, output):
    text = sc.textFile(inputs)
    json_obj = text.flatMap(get_kv).reduceByKey(add_pairs).map(divide).sortBy(get_key).map(set_format)
    json_obj.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit averages')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
