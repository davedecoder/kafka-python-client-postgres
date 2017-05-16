import psycopg2
import argparse, sys
import simplejson as json
from confluent_kafka import Consumer, KafkaError, KafkaException
from pyClient import PyClient
from datetime import datetime, date, time

class PyConsumer(PyClient):
    

    def __init__ (self, conf):
        """
        \p conf is a config dict passed to confluent_kafka.Consumer()
        """
        super(PyConsumer, self).__init__(conf)
        self.conf['on_commit'] = self.on_commit
        self.consumer = Consumer(**conf)
        self.consumed_msgs = 0
        self.consumed_msgs_last_reported = 0
        self.consumed_msgs_at_last_commit = 0
        self.use_auto_commit = False
        self.use_async_commit = False
        self.max_msgs = -1
        self.assignment = []
        self.cur = None
        self.conn = None
        self.assignment_dict = dict()
        self.createDBConn()

    def createDBConn (self):
        try:
            with open('dbConfig.json') as dbConfig:
                d = json.load(dbConfig)
                try:
                    dbname      = d['dbname']
                    user        = d['user']
                    host        = d['host']
                    password    = d['password']           
                    try:
                        self.conn = psycopg2.connect("dbname= '" + dbname + "' user= '" + user + "' host='"+host+"' password='" + password + "'")
                        
                    except psycopg2.OperationalError as e:
                        self.dbg("UNABLE TO CONNECT TO DB")
                        self.dbg(e)         
                        self.sig_term()
                    
                except:
                    self.dbg("PARSING JSON ERROR ")
        except IOError:
            self.dbg("UNABLE TO OPEN DBCONFIG")
            self.dbg(e)         
            self.sig_term()



    def find_assignment (self, topic, partition):
        skey = '%s %d' % (topic, partiton)
        return self.assignment_dict.get(skey)

    def on_assign (self,consumer, partitions):
        """ Rebalance on assign callback """
        old_assignment = self.assignment
        self.assignment = [AssignedPartition(p.topic, p.partition) for p in partitions]
        # Move over our last seen offsets so that we can report a proper
        # minOffset even after a rebalance loop.
        for a in old_assignment:
            b = self.find_assignment(a.topic, a.partition)
            b.min_offset = a.min_offset
        self.assignment_dict = {a.skey: a for a in self.assignment}

    def on_revoke (self, consumer, partitions):
        """ Rebalance on_revoke callback """
        self.assignment = list()
        self.assignment_dict = dict()
        self.do_commit(immediate=True)

    def on_commit (self, err, partitions):
        """ Offsets Committed callback """
        if err is not None and err.code() == KafkaError._NO_OFFSET:
            self.dbg('on_commit(): no offsets to commit')
            return

        d = {
            'name': 'offset-commited',
            'offsets': []
        }

        if err is not None:
            d['success'] = False
            d['error'] = str(err)
        else:
            d['success'] = True
            d['error'] = ''

        for p in partitions:
            pd = {'topic': p.topic, 'partition': p.partition,
                    'offset': p.offset, 'error': str(p.error)}
            d['offsets'].append(pd)

        self.send(d)



    def do_commit (self, immediate=False, async=None):
        if (self.use_auto_commit or
            self.consumed_msgs_at_last_commit + (0 if immediate else 1000) > 
            self.consumed_msgs):
            return
        if async is None:
            async_mode = self.use_async_commit
        else:
            async_mode = async

        self.dbg('Committing %d messages (Async=%s)' %
                    (self.consumed_msgs - self.consumed_msgs_at_last_commit, async_mode))

        try:
            self.consumer.commit(async= async_mode)
        except KafkaException as e:
            if e.args[0].code() == KafkaError._WAIT_COORD:
                self.dbg('Ignoring commit failure, still waiting for coordinator')
            elif e.args[0].code() == KafkaError._NO_OFFSET:
                self.dbg('No offsets to commit')
            else:
                raise
        self.consumed_msgs_at_last_commit = self.consumed_msgs

    def insertToDB (self, jsonMsg):
        
        try:
            target_id       = jsonMsg['target_id']
            uuid            = jsonMsg['uuid']
            parent_uuid     = jsonMsg['parent_uuid']
            question_uuid   = jsonMsg['question_uuid']
            result_start    = jsonMsg['result_start']
            result_end      = jsonMsg['result_end']
            flow_version    = jsonMsg['flow_version']
            flow_uuid       = jsonMsg['flow_uuid']
            answer         = json.dumps(jsonMsg['answer'])
            geo             = jsonMsg['geo']
            event_uuid      = jsonMsg['event_uuid']
            time_stamp      = datetime.now()            
            
        except:
            self.dbg("PARSING JSON ERROR ")
            return

        try:
            self.cur = self.conn.cursor()
            self.cur.execute("INSERT INTO flow_item_result (target_id, uuid, parent_uuid, question_uuid, result_start, result_end, flow_version, flow_uuid, answer, geo, event_uuid, time_stamp) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (target_id, uuid, parent_uuid, question_uuid, result_start, result_end, flow_version, flow_uuid, answer, geo, event_uuid, time_stamp))
        except psycopg2.Error as e:
            self.dbg("POSTGRES ERROR: " + e.diag.severity)
        finally:
            self.conn.commit()
            self.cur.close()
        


    def msg_consume (self, msg):
        """ Handle consumed mesage or error event """

        if msg.value():
            msgValue = msg.value().decode('utf-8')
            if len(msgValue) > 0 :
                print('Received message: %s' % msgValue)
                jsonMsg = json.loads(msgValue)                
                #self.insertToDB(jsonMsg)

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # ignore EOF
                pass
            else:
                self.err('Consume failed: %s' % msg.error(), term=True)
            return
        self.consumed_msgs += 1

        self.do_commit(immediate = False)




class AssignedPartition(object):
    """ Local state container for assigned partition """
    def __init__(self, topic, partition):
        super(AssignedPartition, self).__init__()
        self.topic = topic
        self.partition = partition
        self.skey = '%s %d' % (self.topic, self.partition)
        self.consumed_msgs = 0
        self.min_offset = -1
        self.max_offset = 0

    def to_dict (self):
        """ Return a dict of this partitions state """
        return {'topic': self.topic, 'partition': self.partition,
                'minOffset': self.min_offset, 'maxOffset': self.max_offset}

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Python Consumer')
    parser.add_argument('--topic', action='append', type=str, required=True)
    parser.add_argument('--group-id', dest='group.id', required=True)
    parser.add_argument('--broker-list', dest='bootstrap.servers', required=True)
    parser.add_argument('--session-timeout', type=int, dest='session.timeout.ms', default=6000)
    parser.add_argument('--enable-autocommit', action='store_true', dest='enable.auto.commit', default=False)
    parser.add_argument('--max-messages', type=int, dest='max_messages', default=-1)
    parser.add_argument('--assignment-stategy', dest='partition.assignment.strategy')
    parser.add_argument('--reset-policy', dest='topic.auto.offset.reset', default='earliest')
    parser.add_argument('--consumer.config', dest='consumer_config')

    args = vars(parser.parse_args())

    conf = {'broker.version.fallback': '0.9.0',
            'default.topic.config': dict()}

    PyClient.set_config(conf, args)

    pc = PyConsumer(conf)
    pc.use_auto_commit = args['enable.auto.commit']
    pc.max_msgs = args['max_messages']

    pc.consumer.subscribe(args['topic'],
                            on_assign=pc.on_assign, on_revoke=pc.on_revoke)

    try:
        while pc.run:
            msg = pc.consumer.poll(timeout=1.0)
            if msg is None:
                # Timeout 
                # Commit every poll timeout instead of on every message
                # Also commit on every 1000 messages, whichever comes first
                pc.do_commit(immediate=True)
                continue
            #Handle message or error event
            pc.msg_consume(msg)
    except KeyboardInterrupt:
        pass
    pc.dbg('Closing Consumer ' + str(datetime.now()))
    if not pc.use_auto_commit:
        pc.do_commit(immediate=True, async=False)
    pc.consumer.close()
    pc.dbg('Done')

