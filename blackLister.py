import datetime
from operator import add
import boto3
from kafka import KafkaConsumer
from pyspark import SparkContext, SparkConf
import time
import logging

import yaml


with open("../creds.yaml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

s3_resourse = boto3.resource('s3')
ACCESS_KEY = cfg['aws']['access_key']
SECRET_KEY = cfg['aws']['secret_key']
S3_REGION = cfg['aws']['s3_region']
VERBOSE = False



list = "lists/blacklist.txt"

pyspark_log = logging.getLogger('pyspark')
pyspark_log.setLevel(logging.ERROR)

logging.basicConfig(filename = 'blacklist-logs/app.log', level=logging.DEBUG, filemode='w', format='%(name)s - %(levelname)s - %(message)s')

class LogRow:

    def __init__(self):
        self.row = {}

    @staticmethod
    def sliceUntil(begin_char,stop_char, msg):
        chunk=""
        offset = 0
        for i, char in enumerate(msg):
            offset += 1
            if char == begin_char:
                for i, char in enumerate(msg[offset:]):  #start slicing from first quotation
                    offset += 1
                    if char != stop_char:
                        chunk += char
                    else:
                        break
                break
        return(chunk,offset)

    def parseRow(self, msg):
        ip = ""
        for i, char in enumerate(msg):
            pointer = i
            if char[0] != " ":
                ip += char[0]
            else:
                break
        #parse date
        date = msg[pointer+6:pointer+32]
        pointer = pointer+32
        #parse http
        http, offset= self.sliceUntil("\"","\"", msg[pointer:])
        pointer+=offset

        #parse response
        response, offset = self.sliceUntil(" ","-", msg[pointer:])
        response = response[:-2]
        pointer += offset


        #parse dash

        dash, offset = self.sliceUntil("\"", "\"", msg[pointer-2:])
        os = msg[pointer+3:]
        row = {
            "ip": ip,
            "date": date,
            "http": http,
            "response": response,
            "os": os
        }
        self.ip = ip
        self.date = date
        self.http = http
        self.response = response
        self.os = os
        return(row)



class BlackLister():
    def __init__(self, log):


        self.verbose = True
        self.rdd = log
        self.parsed = self.parseLog()


        self.dateTime = self.dateIpTuple()
        self.dateTuple = self.dateTuple()
        self.dateFreq  = self.dateFreq()
        self.meanFreq = self.meanDateFreq()
        self.sdFreq = self.sdDateFreq()

        self.ipTuple = self.ipTuple()
        self.ipFreqTuple = self.ipFreq()
        self.meanIpHits = self.meanIpHits()
        self.sdIpHits  = self.sdIpHits()


        self.black_list = self.blackList()



    """
    parses log input and returns and RDD with date, http, ip key value pairs
    """
    def parseLog(self):
        lr = LogRow()
        rdd = self.rdd.map(lambda line: lr.parseRow(line))
        print('Log has been parsed')
        return(rdd)

    """
    Create a (datetime, ip) pair rdd from parsed log rdd
    """
    def dateIpTuple(self):
        rdd = self.parsed.map(lambda x: (x['date'], x['ip']))
        print('(Date,IP) Tuple Created')
        return (rdd)

    """
    Create a (datetime,1) from parsed log rdd
    """
    def dateTuple(self):
        rdd = self.parsed.map(lambda x: (x['date'], 1))
        print('(Date,1) Tuple Created')
        print(rdd.take(2))
        return(rdd)

    """
    Create an ipTuple (ip, 1) form parsed log rdd
    """
    def ipTuple(self):
        rdd = self.parsed.map(lambda x: (x['ip'],1))
        print('(IP,1) Tuple Created')
        print(rdd.take(2))
        return(rdd)
    """
    Create a (date, freq) tuple
    """
    def dateFreq(self):
        rdd = self.dateTuple.reduceByKey(add)
        print('(Date, Freq) Tuple Created')
        print(rdd.take(2))
        return(rdd)

    """
        Returns an IP hits, Frequency tuple
    """

    def ipFreq(self):
        rdd = self.ipTuple.reduceByKey(add).sortBy(lambda x: x[1], False)
        print('(IP,Frequency) Computed')
        print(rdd.take(2))
        return rdd

    """
    Given a dateFreq Find the mean frequency hits by date
    """
    def meanDateFreq(self):
        print('mean date starting')
        rdd = self.dateFreq
        print(rdd.take(2))
        rdd = rdd.map(lambda x: x[1])
        print(rdd.take(2))
        mean = rdd.mean()
        print('Mean Date Frequency Computed: %f' % mean)
        return(mean)

    """
    Given a dateFreq Find the SD frequency hits by date
    """

    def sdDateFreq(self):
        sd = self.dateTuple.map(lambda x: float(x[1])).stdev()
        print('SD Date Frequency Computed: %f ' % sd)
        return(sd)

    """
    Calculates the mean number of times that each IP hits the server.  This will be used later
    to make sure we do not filter legitimate traffic
    """
    def meanIpHits(self):
        rdd = self.ipFreqTuple.map(lambda x: float(x[1]))
        mean = rdd.mean()
        print('Mean IP Hits Computed: %f' % mean)
        return(mean)

    """
    Standard deviation of IP hits
    """

    def sdIpHits(self):
        sd = self.ipFreqTuple.map(lambda x: x[1]).stdev()
        print('SD IP Hits Computed: %f' % sd)
        return(sd)



    """
    Selects time series entrys that are greater than 2 SD
    """

    @staticmethod
    def select(x, mean, sd):
        diff = float(x[1]) - mean.value
        if (diff > sd.value * 2):
            return (True)
        else:
            return (False)
    """
    Selects points suspicious traffic spikes 
    """
    def selectSuspicious(self):
        mean = self.meanFreq
        sd = self.sdFreq
        rdd = self.dateFreq
        rdd = rdd.filter(lambda x: (x[1]-mean) > (sd * 2))
        print('Suspicious IP RDD Computed')
        return(rdd)

    """
    Joins suspicious traffic table with suspicious IP hits table and then cross references 
    IP addresses who have heavy traffic
    """

    def blackList(self):
        # join and create tuple for blacklisted ips
        date_tuple_select = self.selectSuspicious()
        print('Date Tuple Select')
        print(date_tuple_select.take(2))
        rdd = self.dateIpTuple()
        date_black = date_tuple_select.join(rdd)
        print(date_black.take(2))
        ip_black = date_black.map(lambda x: (x[1][1], x[1][0]))
        print('Black Listed IPs')
        print(ip_black.take(2))



        ip_black_freq = ip_black.join(self.ipFreqTuple)
        ip_black_freq = ip_black_freq.distinct()
        ip_black_freq = ip_black_freq.map(lambda x: (x[0], x[1][1]))

        # filter out IP's that are appearing more than standard deviation of times
        tolerance = self.meanIpHits + self.sdIpHits
        black_list = ip_black_freq.filter(lambda x: x[1] > tolerance)

        print('Black List Computed')
        print(black_list.take(2))
        return(black_list)

    @staticmethod
    def writeIp(x, file):
        f = open(file, mode="a+", encoding="utf-8")
        f.write(x + "\n")
        f.close()

    @staticmethod
    def toDynamo(item):
        table.put_item(Item={'ip' : item})



    """
    Used to write blacklist to file
    """

    def write2file(self,list):
        with open(list, mode="a+", encoding="utf-8") as f:
            black_list = self.black_list \
                .map(lambda x: x[0]) \
                .distinct() \
                .collect()
            for ip in black_list:
                f.write(ip + "\n")
        print('Blacklisted IPs written to File ')

    def write2file(self,list):
        with open(list, mode="a+", encoding="utf-8") as f:
            black_list = self.black_list \
                .map(lambda x: x[0]) \
                .distinct() \
                .collect()
            for ip in black_list:
                f.write(ip + "\n")
        print('Blacklisted IPs written to File ')

    def write2DB(self):
        print('writing to db....')
        self.black_list \
            .map(lambda x: x[0]) \
            .distinct() \
            .map(self.write2Dynamo) \
            .collect()


    @staticmethod
    def write2Dynamo(item, verbose = VERBOSE):
        DB = boto3.resource(service_name='dynamodb', region_name=S3_REGION, aws_access_key_id=ACCESS_KEY,
                            aws_secret_access_key=SECRET_KEY)
        table = DB.Table(table_name)
        table.put_item(
            Item={
                    'ip': item
            }
        )
        if verbose:
            print('writing....')
            print('ip:', item)


if __name__ == "__main__":

    table_name = "black-list"

    conf = SparkConf().setAppName("demo")
    sc = SparkContext(conf=conf)


    kafka_topic = 'test'
    group_id = 'log_consumer_group'

    #create Kafka consumer you must increase size of max poll

    consumer = KafkaConsumer(kafka_topic, auto_offset_reset='earliest', max_poll_records = 1000000, max_partition_fetch_bytes= 52428800)
    lr = LogRow()

    #loop through
    while True:
        start = time.time()
        window = consumer.poll(timeout_ms=6000)
        if(window):
            for tp, messages in window.items():
                queue_rdd = sc.parallelize(messages)
                queue_rdd = queue_rdd.map(lambda x: x.value.decode('ascii'))
                print("Count:" + str(queue_rdd.count()))
            bl = BlackLister(queue_rdd)
            bl.write2DB()
            #bl.write2file('lists/blacklist.txt')
        print('waiting.....')
        end = time.time()
        total_time = end-start
        print("Application Time: %i", total_time)
        time.sleep(5)








































