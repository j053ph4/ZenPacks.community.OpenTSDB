import logging
log = logging.getLogger('zen.zenopentsdb')
import Globals
import zope.component
import zope.interface
from twisted.internet import defer
from Products.ZenCollector.daemon import CollectorDaemon
from Products.ZenCollector.interfaces import ICollectorPreferences, IScheduledTask, IEventService, IDataService
from Products.ZenCollector.tasks import SimpleTaskFactory, SimpleTaskSplitter, TaskStates
from Products.ZenUtils.observable import ObservableMixin
from Products.ZenEvents.Event import Warning, Clear
from Products.ZenUtils.Utils import unused
from twisted.internet import base, defer, reactor
from Products.ZenUtils.Driver import drive
from twisted.python.failure import Failure
import time

from ZenPacks.community.OpenTSDB.services.OpenTSDBService import OpenTSDBService
import pika,socket,re

unused(Globals)
unused(OpenTSDBService)


class ZenOpenTSDBPreferences(object):
    zope.interface.implements(ICollectorPreferences)

    def __init__(self):
        self.collectorName = 'zenopentsdb'
        self.configurationService = "ZenPacks.community.OpenTSDB.services.OpenTSDBService"
        # How often the daemon will collect each device. Specified in seconds.
        self.cycleInterval = 5 * 60
        # How often the daemon will reload configuration. In seconds.
        self.configCycleInterval = 30  #minutes
        self.options = None

    def buildOptions(self, parser):
        """
        Required to implement the ICollectorPreferences interface.
        """
        parser.add_option('--bundle', dest='bundle',
                        action="store_false",
                        help='Update OpenTSDB in a single session'
                        )

    def postStartup(self):
        """
        Required to implement the ICollectorPreferences interface.
        """
        pass


class ZenOpenTSDBTask(ObservableMixin):
    zope.interface.implements(IScheduledTask)
    
    CLEAR_EVENT = dict(component="zenopentsdb", severity=Clear, eventClass='/Cmd/Fail')
    WARNING_EVENT = dict(component="zenopentsdb", severity=Warning, eventClass='/Cmd/Fail')
    
    def __init__(self, taskName, deviceId, interval, taskConfig):
        super(ZenOpenTSDBTask, self).__init__()
        self._taskConfig = taskConfig
        self._eventService = zope.component.queryUtility(IEventService)
        self._dataService = zope.component.queryUtility(IDataService)
        self._preferences = zope.component.queryUtility(ICollectorPreferences, 'zenopentsdb')

        self._tsdbserver = self._taskConfig.tsdbServer
        self._tsdbport = self._taskConfig.tsdbPort
        self._tsd = socket.socket()
        
        self._mqserver = self._taskConfig.mqServer
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mqserver))
        
        self.name = taskName
        self.configId = deviceId
        self.interval = interval
        self.state = TaskStates.STATE_IDLE
        self.bundlemessages = self._preferences.options.bundle
    
    
    def _connectMQ(self):
        '''
            connect to RabbitMQ
        '''
        log.debug("_connectMQ")
        def inner(driver):
            try:
                self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._mqserver))
                yield defer.succeed(None)
            except:
                yield defer.fail("Could not connect to RabbitMQ")
        return drive(inner)
    
    def _connectOpenTSDB(self):
        '''
            connect to OpenTSDB
        '''
        log.debug("_connectOpenTSDB")
        def inner(driver):
            try:
                self._tsd = socket.socket()
                self._tsd.connect((self._tsdbserver,int(self._tsdbport)))
                yield defer.succeed(None)
            except:
                yield defer.fail("Could not connect to OpenTSDB")
        return drive(inner)
    
    def _connectCallback(self, result):
        '''
            Callback for a successful asynchronous connection request.
        '''
        log.debug("_connectCallback")
    
    def _collectCallback(self, result):
        '''
            Callback used to begin performance data collection asynchronously after
            a connection or task setup.
        '''
        log.debug("_collectCallback")
        d = self.fetch()
        d.addCallbacks(self._collectSuccessful, self._failure)
        d.addCallback(self._collectCleanup)
        return d
    
    def _collectSuccessful(self, result):
        '''
            Callback for a successful asynchronous performance data collection
            request.
        '''
        log.debug("_collectSuccessful")
        self._eventService.sendEvent(ZenOpenTSDBTask.CLEAR_EVENT, device=self.configId, summary="Device collected successfully")

    def _collectCleanup(self, result):
        '''
            Callback after a successful collection to perform any cleanup after the
            data has been processed.
        '''
        log.debug("_collectCleanup")
        self.state = TaskStates.STATE_CLEANING
        self.close()
        return defer.succeed(None)
    
    def close(self):
        '''
        '''
        try:
            self._tsd.close()
            self._connection.close()
        except:
            pass
        
    def _failure(self, result):
        '''
            Errback for an unsuccessful asynchronous connection or collection
            request.
        '''
        log.debug("_failure")
        err = result.getErrorMessage()
        log.error("Failed with error: %s" % err)
        self._eventService.sendEvent(ZenOpenTSDBTask.WARNING_EVENT, device=self.configId, summary="Error collecting performance data: %s" % err)
        self.close()
        return result
    
    def _finished(self, result):
        '''
            post collection activities
        '''
        if not isinstance(result, Failure):
            log.info("Successful scan of %s completed", self.configId)
        else:
            log.error("Unsuccessful scan of %s completed, result=%s", self.configId, result.getErrorMessage())
        return result
    
    def cleanup(self):
        '''
            required post-collection method
        '''
        log.debug("cleanup")
        self.state = TaskStates.STATE_COMPLETED
        return defer.succeed(None)
    
    def doTask(self):
        '''
            main task loop
        '''
        log.debug("doTask")
        self.state = TaskStates.STATE_RUNNING
        log.debug("%d datapoints to update on %s" % (len(self._taskConfig.datapoints),self.configId))
        # establish RabbitMQ connection
        d = self._connectMQ()
        d.addCallbacks(self._connectCallback, self._failure)
        # establish OpenTSDB connection
        d = self._connectOpenTSDB()
        d.addCallbacks(self._connectCallback, self._failure)
        # perform RabbitMQ/OpenTSDB synchronization
        d.addCallback(self._collectCallback)
        d.addBoth(self._finished)
        return d
    
    def fetch(self):
        '''
            data synchronization between RabbitMQ and OpenTSDB
        '''
        log.debug("fetch")
        def inner(driver):
            log.debug("  Fetching data for %s (%d datapoints)", self.configId, len(self._taskConfig.datapoints))
            try:
                self.totalmessages = 0
                start = time.time()
                for datapoint in self._taskConfig.datapoints:
                    self._queue = datapoint['path']
                    self.consumeMessages( datapoint['metadata'])
                # report some statistics
                end = time.time() - start
                avgtime = self.totalmessages / end
                log.info("  %s: updated %d msgs in %f secs avg: %f msg/s" % (self.configId, self.totalmessages, end, avgtime))
                
                yield defer.succeed(None)
            except:
                yield defer.fail("collection failed for %s" % self.configId)
        return drive(inner)
    
    def consumeMessages(self,metadata):
        '''
            takes messages from a RabbitMQ queue and pushes them to OpenTSDB
        '''
        start = time.time()
        log.debug("consumeMessages")
        self._channel =  self._connection.channel()
        status = self._channel.queue_declare(queue=self._queue)
        if status.method.message_count  > 0:
            log.debug("  %s:%s has %d messages" % (metadata['tags']['device'], metadata['name'], status.method.message_count))

        if self.bundlemessages == True:
            # create one message w/all put statements
            self.message = ''
            # track ids of each message for ack after update to tsdb
            self.messageids = []
        # method used by pika to handle messages
        def callback(ch, method, properties, body):
            if self.bundlemessages == True:
                self.message += self.tsdbFormat(metadata,eval(body))
                self.messageids.append(method.delivery_tag)
            else:
                #log.debug("body: %s" % body)
                self.totalmessages += 1
                # format put command
                message = self.tsdbFormat(metadata,eval(body))
                # push to tsdb
                self.tsdbUpdate(message,method=method)
                
            # stop consuming if empty
            if status.method.message_count == method.delivery_tag:
                self._channel.stop_consuming()
        
        # start consume loop
        self._channel.basic_consume(callback, queue=self._queue)
        # if bundling messages, send them all now
        if self.bundlemessages == True:
            if len(self.message) > 1:
                #log.debug("%d messages of length: %d " % ( len(self.messageids), len(self.message) ))
                self.tsdbUpdate(self.message, ids=self.messageids)
            self.totalmessages += len(self.messageids)
        #log.debug("closing channel")
        self._channel.close()

    def tsdbUpdate(self,message,method=None,ids=[]):
        '''
            sends updates to OpenTSDB, and acks RabbitMQ message(s) if successful
        '''
        log.debug("tsdbSend")
        def inner(driver):
            try:
                messageLength = len(message)
                if messageLength > 1:
                    result = self._tsd.send(message)
                    if result != messageLength:
                        yield defer.fail("OpenTSDB failed delivery: %d of %d bytes sent" % (result,messageLength))
                if self.bundlemessages == True:
                    for id in ids:
                        self._channel.basic_ack(delivery_tag = id)
                else:
                    self._channel.basic_ack(delivery_tag = method.delivery_tag)
                log.debug("  successful message delivery length: %d" % messageLength)
                yield defer.succeed(None)
            except:
                yield defer.fail("OpenTSDB send failed for %s" % self.configId)
        return drive(inner)
            
    def tsdbFormat(self, metadata, perfdata):
        '''
            format message as a put command
        '''
        msg = 'put %s %s %s' % (metadata['name'], str(perfdata['timestamp']), str(perfdata['value']))
        for k,v in metadata['tags'].items():
            msg += ' %s=%s' % (str(k),str(v))
        msg += '\n'
        return msg

if __name__ == '__main__':
    myPreferences = ZenOpenTSDBPreferences()
    myTaskFactory = SimpleTaskFactory(ZenOpenTSDBTask)
    myTaskSplitter = SimpleTaskSplitter(myTaskFactory)

    daemon = CollectorDaemon(myPreferences, myTaskSplitter)
    daemon.run()
