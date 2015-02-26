#import logging
#log = logging.getLogger('zen.zenopentsdb')
import Globals
from Products.ZenModel.ZenPack import ZenPack as ZenPackBase
from Products.ZenUtils.Utils import unused
import os,re,time,pika
from Products.ZenCollector.daemon import *
from ZenPacks.community.OpenTSDB.QueueHandler import *
from Products.ZenUtils.Utils import monkeypatch,prepId
unused(Globals)

#######################
#
#rabbitmqctl add_vhost /opentsdb
#rabbitmqctl add_user opentsdb opentsdb
#rabbitmqctl set_permissions -p /opentsdb opentsdb '.*' '.*' '.*'
#
#######################


#setattr(CollectorDaemon, 'queue', None)

@monkeypatch('Products.ZenCollector.daemon.CollectorDaemon')
def putMQ(self, queue, msg):
    from ZenPacks.community.OpenTSDB.QueueHandler import QueueHandler
    q = QueueHandler()
    q.messages = [ msg ]
    try:  q.put(queue)
    except:  pass

@monkeypatch('Products.ZenCollector.daemon.CollectorDaemon')
def pushToQueue(self, timestamp, value, path):
    ''' build data structure and push to RabbitMQ '''
    if timestamp == 'N': timestamp = time.time()
    data = { 'timestamp' : int(timestamp), 'value': value }
    self.putMQ(path, str(data))


# writeRRD method for 4.x
@monkeypatch('Products.ZenCollector.daemon.CollectorDaemon')
def writeRRD(self, path, value, rrdType, rrdCommand=None, cycleTime=None,
             min='U', max='U', threshEventData={}, timestamp='N', allowStaleDatapoint=True):
    
    now = time.time()
    
    hasThresholds = bool(self._thresholds.byFilename.get(path))
    if hasThresholds:
        rrd_write_fn = self._rrd.save
    else:
        rrd_write_fn = self._rrd.put

    # only modification to this method
    if value: self.pushToQueue(timestamp, value, path)
    
    # save the raw data directly to the RRD files
    value = rrd_write_fn(
        path,
        value,
        rrdType,
        rrdCommand,
        cycleTime,
        min,
        max,
        timestamp=timestamp,
        allowStaleDatapoint=allowStaleDatapoint,
    )
    # check for threshold breaches and send events when needed
    if hasThresholds:
        if 'eventKey' in threshEventData:
            eventKeyPrefix = [threshEventData['eventKey']]
        else:
            eventKeyPrefix = [path.rsplit('/')[-1]]
        
        for ev in self._thresholds.check(path, now, value):
            parts = eventKeyPrefix[:]
            if 'eventKey' in ev:
                parts.append(ev['eventKey'])
            ev['eventKey'] = '|'.join(parts)
            
            # add any additional values for this threshold
            # (only update if key is not in event, or if
            # the event's value is blank or None)
            for key,value in threshEventData.items():
                if ev.get(key, None) in ('',None):
                    ev[key] = value
            
            self.sendEvent(ev)

class ZenPack(ZenPackBase):
    """ 
    """
    packZProperties = [
        ('zOpenTSDBServer', 'localhost', 'string'),
        ('zOpenTSDBPort', '4242', 'string'),
        #('zOpenTSDBCollect', True, 'boolean')
        ]

