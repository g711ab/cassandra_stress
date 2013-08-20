import pycassa
import uuid
import os
import time
import sys

def create_column_families():
    sys = pycassa.system_manager.SystemManager('esb-a-test.sensors.elex.be')
    #sys.create_keyspace('test', pycassa.system_manager.NETWORK_TOPOLOGY_STRATEGY, {'erfurt': '1', 'sensors': '1', 'sofia': '1', 'diegem': '1'})
    sys.create_column_family('test', 'INDEXCF', super=False, comparator_type=pycassa.system_manager.UTF8_TYPE)
    sys.create_column_family('test', 'DATACF', super=False, comparator_type=pycassa.system_manager.UTF8_TYPE)
    sys.close()

#create_column_families()

index = {}

def populate():
    cp = pycassa.ConnectionPool('test', server_list=['esb-a-test.sensors.elex.be', 'esb-b-test.sensors.elex.be'])


    indexcf = pycassa.ColumnFamily(cp, 'INDEXCF')
    datacf  = pycassa.ColumnFamily(cp, 'DATACF')

    for i in range(100000):
        ref = str(uuid.uuid1())
        data = {}
        for x in range(100):
            data['blub%d' % x] = 'world'
        datacf.insert(ref, data)
        index['col%d' % i] = ref
        if i % 10 == 0:
            print i
    
    indexcf.insert('index', index)

def slice_query(indexcf, datacf, start, end):
    cols = indexcf.get('index', column_start=start, column_finish=end, column_count=10000, read_consistency_level=pycassa.ConsistencyLevel.ONE)
    for _, ref in cols.iteritems():
        res = datacf.get(ref)
        yield res

def get_test():
    child = False

    for i in range(10):
        pid = os.fork()
        if pid == 0:
            child = True
            print '[%d] Starting proc' % i

            cp = pycassa.ConnectionPool('test', server_list=['esb-a-test.sensors.elex.be', 'esb-b-test.sensors.elex.be'])


            indexcf = pycassa.ColumnFamily(cp, 'INDEXCF')
            datacf  = pycassa.ColumnFamily(cp, 'DATACF')

            before = time.time()
            l = list(slice_query(indexcf, datacf, 'a', 'z'))
            delta = time.time() - before
            print '[%d] Got results in %d' % (i, delta)
            break

    if not child:
        time.sleep(10000)

if len(sys.argv) < 2:
    print 'pass populate or get'
else:
    if sys.argv[1] == 'populate':
        populate()
    elif sys.argv[1] == 'get':
        get_test()
    else:
        print 'unknown argument'