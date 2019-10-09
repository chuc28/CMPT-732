from pyspark import SparkConf, SparkContext
import sys, random
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def add_to_one(num):  
    iterations = 0
    random.seed()
    for i in range(num):
        sum = 0.0
        while sum < 1.0:
            sum += random.random()
            iterations += 1
    return iterations

def addAll(a, b):
    return a + b

def main(inputs):
    partition = 100
    partial_sample = [int(inputs) // partition] * partition
    iteration = sc.parallelize(partial_sample, partition).map(add_to_one).reduce(addAll)
    print(iteration / int(inputs))

if __name__ == '__main__':
    conf = SparkConf().setAppName('euler')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    main(inputs)


    
