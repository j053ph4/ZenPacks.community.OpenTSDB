import Globals
from Products.ZenModel.ZenPack import ZenPack as ZenPackBase
from Products.ZenUtils.Utils import unused
import os,re,time,pika
from Products.ZenUtils.Utils import monkeypatch,prepId
from Products.ZenModel.ZVersion import VERSION as ZENOSS_VERSION
from Products.ZenUtils.Version import Version

unused(Globals)
""" Add device relations
"""

@monkeypatch('Products.ZenCollector.daemon.CollectorDaemon')
def putMQ(self, server, queueName, msg):
    ''' push mesage onto RabbitMQ queue'''
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=server))
        channel = connection.channel()
        channel.queue_declare(queue=queueName)
        channel.basic_publish(exchange='', routing_key=queueName,body=msg)
        channel.close()
        connection.close()
    except:
        pass

@monkeypatch('Products.ZenCollector.daemon.CollectorDaemon')
def pushToQueue(self, timestamp, value, path):
    '''
        build data structure and push to RabbitMQ
    '''
    if timestamp == 'N':
        timestamp = time.time()
    data = {
            'timestamp' : int(timestamp),
            'value': value,
            }
    self.putMQ('localhost',path, str(data))


# modify the writeRRD method depending on Zenoss version
if Version.parse('Zenoss ' + ZENOSS_VERSION) >= Version.parse('Zenoss 4'):
    #print "modifying writeRRD for Zenoss 4.x"
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
        self.pushToQueue(timestamp, value, path)
        
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
else:
    # writeRRD method for 3.x
    # print "modifying writeRRD for Zenoss 3.x"
    from Products.ZenEvents.ZenEventClasses import Critical, Clear
    from Products.ZenRRD.CommandParser import ParsedResults

    @monkeypatch('Products.ZenCollector.daemon.CollectorDaemon')
    def writeRRD(self, path, value, rrdType, rrdCommand=None, cycleTime=None,
                 min='U', max='U', threshEventData=None):
        now = time.time()
        
        # only modification to this method
        self.pushToQueue('N', value, path)
        
        # save the raw data directly to the RRD files
        value = self._rrd.save(path,
                               value,
                               rrdType,
                                rrdCommand,
                               cycleTime,
                               min,
                               max)
        
        # check for threshold breaches and send events when needed
        for ev in self._thresholds.check(path, now, value):
            ev['eventKey'] = path.rsplit('/')[-1]
            if threshEventData:
                ev.update(threshEventData)
            self.sendEvent(ev)


    @monkeypatch('Products.ZenRRD.zenperfsnmp.zenperfsnmp')
    def putMQ(self, server, queueName, msg):
        ''' push mesage onto RabbitMQ queue'''
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=server))
            channel = connection.channel()
            channel.queue_declare(queue=queueName)
            channel.basic_publish(exchange='', routing_key=queueName,body=msg)
            channel.close()
            connection.close()
        except:
            pass
    
    
    @monkeypatch('Products.ZenRRD.zenperfsnmp.zenperfsnmp')
    def pushToQueue(self, timestamp, value, path):
        '''
            build data structure and push to RabbitMQ
        '''
        if timestamp == 'N':
            timestamp = time.time()
        data = {
                'timestamp' : int(timestamp),
                'value': value,
                }
        self.putMQ('localhost',path, str(data))
        
    @monkeypatch('Products.ZenRRD.zenperfsnmp.zenperfsnmp')
    def storeRRD(self, device, oid, value):
        """
        Store a value into an RRD file

        @param device: remote device name
        @type device: string
        @param oid: SNMP OID used as our performance metric
        @type oid: string
        @param value: data to be stored
        @type value: number
        """
        oidData = self.proxies[device].oidMap.get(oid, None)
        if not oidData: return

        raw_value = value
        min, max = oidData.minmax
        try:
            # only modification to this method
            self.pushToQueue('N', value, oidData.path)
            value = self.rrd.save(oidData.path,
                                  value,
                                  oidData.dataStorageType,
                                  oidData.rrdCreateCommand,
                                  min=min, max=max)
        except Exception, ex:
            summary= "Unable to save data for OID %s in RRD %s" % \
                              ( oid, oidData.path )
            self.log.critical( summary )

            message= """Data was value= %s, type=%s, min=%s, max=%s
RRD create command: %s""" % \
                     ( value, oidData.dataStorageType, min, max, \
                       oidData.rrdCreateCommand )
            self.log.critical( message )
            self.log.exception( ex )

            import traceback
            trace_info= traceback.format_exc()

            evid= self.sendEvent(dict(
                dedupid="%s|%s" % (self.options.monitor, 'RRD write failure'),
                severity=Critical,
                device=self.options.monitor,
                eventClass=Status_Perf,
                component="RRD",
                oid=oid,
                path=oidData.path,
                message=message,
                traceback=trace_info,
                summary=summary))

            # Skip thresholds
            return

        if self.options.showdeviceresults:
            self.log.info("%s %s results: raw=%s RRD-converted=%s"
                          " type=%s, min=%s, max=%s" % (
                   device, oid, raw_value, value, oidData.dataStorageType, min, max))

        for ev in self.thresholds.check(oidData.path, time.time(), value):
            eventKey = oidData.path.rsplit('/')[-1]
            if ev.has_key('eventKey'):
                ev['eventKey'] = '%s|%s' % (eventKey, ev['eventKey'])
            else:
                ev['eventKey'] = eventKey
            self.sendThresholdEvent(**ev)
    
    # for zencommand.py
    @monkeypatch('Products.ZenRRD.zencommand.zencommand')
    def putMQ(self, server, queueName, msg):
        ''' push mesage onto RabbitMQ queue'''
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=server))
            channel = connection.channel()
            channel.queue_declare(queue=queueName)
            channel.basic_publish(exchange='', routing_key=queueName,body=msg)
            channel.close()
            connection.close()
        except:
            pass
    
    
    @monkeypatch('Products.ZenRRD.zencommand.zencommand')
    def pushToQueue(self, timestamp, value, path):
        '''
            build data structure and push to RabbitMQ
        '''
        if timestamp == 'N':
            timestamp = time.time()
        data = {
                'timestamp' : int(timestamp),
                'value': value,
                }
        self.putMQ('localhost',path, str(data))
    @monkeypatch('Products.ZenRRD.zencommand.zencommand')
    def parseResults(self, cmd):
        """
        Process the results of our command-line, send events
        and check datapoints.

        @param cmd: command
        @type: cmd object
        """
        self.log.debug('The result of "%s" was "%r"', cmd.command, cmd.result.output)
        results = ParsedResults()
        try:
            parser = cmd.parser.create()
        except Exception, ex:
            self.log.exception("Error loading parser %s" % cmd.parser)
            import traceback
            self.sendEvent(dict(device=cmd.deviceConfig.device,
                           summary="Error loading parser %s" % cmd.parser,
                           component="zencommand",
                           message=traceback.format_exc(),
                           agent="zencommand",
                          ))
            return
        parser.processResults(cmd, results)

        for ev in results.events:
            self.sendEvent(ev, device=cmd.deviceConfig.device)

        for dp, value in results.values:
            self.log.debug("Storing %s = %s into %s" % (dp.id, value, dp.rrdPath))
            value = self.rrd.save(dp.rrdPath,
                                  value,
                                  dp.rrdType,
                                  dp.rrdCreateCommand,
                                  cmd.cycleTime,
                                  dp.rrdMin,
                                  dp.rrdMax)
            self.log.debug("RRD save result: %s" % value)
            
            # only modification to this method
            self.pushToQueue('N', value, dp.rrdPath)
            
            for ev in self.thresholds.check(dp.rrdPath, time.time(), value):
                eventKey = cmd.getEventKey(dp)
                if 'eventKey' in ev:
                    ev['eventKey'] = '%s|%s' % (eventKey, ev['eventKey'])
                else:
                    ev['eventKey'] = eventKey
                ev['component'] = dp.component
                self.sendEvent(ev)


class ZenPack(ZenPackBase):
    """ 
    """
    packZProperties = [
        ('zOpenTSDBServer', 'localhost', 'string'),
        ('zOpenTSDBPort', '4242', 'string'),
        ]

