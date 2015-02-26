import logging
log = logging.getLogger('zen.zenopentsdbpush')
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
from ZenPacks.community.OpenTSDB.services.OpenTSDBPushService import OpenTSDBPushService
from ZenPacks.community.OpenTSDB.QueueHandler import *
import socket,time

unused(Globals)
unused(OpenTSDBPushService)

class ZenOpenTSDBPushPreferences(object):
    zope.interface.implements(ICollectorPreferences)
    def __init__(self):
        self.collectorName = 'zenopentsdbpush'
        self.configurationService = "ZenPacks.community.OpenTSDB.services.OpenTSDBPushService"
        # How often the daemon will collect each device. Specified in seconds.
        self.cycleInterval = 300
        # How often the daemon will reload configuration. In minutes.
        self.configCycleInterval = 360
        self.options = None

    def buildOptions(self, parser): pass
    
    def postStartup(self): pass

class ZenOpenTSDBPushTask(ObservableMixin):
    zope.interface.implements(IScheduledTask)
    
    CLEAR_EVENT = dict(component="zenopentsdbpush", severity=Clear, eventClass='/Cmd/Fail')
    WARNING_EVENT = dict(component="zenopentsdbpush", severity=Warning, eventClass='/Cmd/Fail')
    
    def __init__(self, taskName, deviceId, interval, taskConfig):
        super(ZenOpenTSDBPushTask, self).__init__()
        self._taskConfig = taskConfig
        self._eventService = zope.component.queryUtility(IEventService)
        self._dataService = zope.component.queryUtility(IDataService)
        self._preferences = zope.component.queryUtility(ICollectorPreferences, 'zenopentsdbpush')
        self._tsdbserver = self._taskConfig.tsdbServer
        self._tsdbport = self._taskConfig.tsdbPort
        self._tsd = None
        self.name = taskName
        self.configId = deviceId
        self.interval = interval
        self.state = TaskStates.STATE_IDLE
            
    def _connectCallback(self, result):  pass
    
    def _collectCallback(self, result):
        ''' '''
        log.debug("_collectCallback")
        d = self.fetch()
        d.addCallbacks(self._collectSuccessful, self._failure)
        d.addCallback(self._collectCleanup)
        return d
    
    def _collectSuccessful(self, result):
        ''' '''
        log.debug("_collectSuccessful")
        self._eventService.sendEvent(ZenOpenTSDBPushTask.CLEAR_EVENT, device=self.configId, summary="Device collected successfully")

    def _collectCleanup(self, result):
        ''' '''
        log.debug("_collectCleanup")
        self.state = TaskStates.STATE_CLEANING
        return defer.succeed(None)
        
    def cleanup(self):
        ''' '''
        log.debug("cleanup")
        self.state = TaskStates.STATE_COMPLETED
        return defer.succeed(None)
    
    def _failure(self, result):
        ''' '''
        #log.debug("_failure")
        err = result.getErrorMessage()
        log.error("Failed with error: %s" % err)
        self._eventService.sendEvent(ZenOpenTSDBPushTask.WARNING_EVENT, device=self.configId, summary="Error collecting performance data: %s" % err)
        return result

    def _finished(self, result):
        ''' '''
        if not isinstance(result, Failure): log.info("Successful scan of %s completed", self.configId)
        else: log.error("Unsuccessful scan of %s completed, result=%s", self.configId, result.getErrorMessage())
        self.close()
        return result

    def _connect(self):
        '''connect to RabbitMQ'''
        def inner(driver):
            if self.connectTSDB() is False: yield defer.fail("Could not connect to OpenTSDB")
            else: yield defer.succeed(None)
        return drive(inner)
    
    def doTask(self):
        ''' main task loop '''
        log.debug("doTask")
        self.state = TaskStates.STATE_RUNNING
        # establish TSDB connection
        d = self._connect()
        d.addCallbacks(self._connectCallback, self._failure)
        # perform synchronization
        d.addCallback(self._collectCallback)
        d.addBoth(self._finished)
        return d
    
    def connectTSDB(self):
        ''' connect to OpenTSDB '''
        log.debug("connectTSDB")
        try:
            log.debug("connecting to TSDB: %s on port %s" % (self._tsdbserver,self._tsdbport))
            self._tsd = socket.socket()
            self._tsd.connect((self._tsdbserver,int(self._tsdbport)))
            return True
        except: 
            log.warn("Could not connect to OpenTSDB")
            return False
    
    def close(self):
        ''' '''
        log.debug("close")
        if self._tsd is not None:
            try: self._tsd.close()
            except: log.warn("could not close OpenTSDB socket")
        self._tsd = None
            
    def fetch(self):
        ''' data synchronization between RabbitMQ and OpenTSDB '''
        log.debug("fetch")
        def inner(driver):
            try:
                self.consumeDatapoints()
                yield defer.succeed(None)
            except:
                yield defer.fail("collection failed for %s" % self.configId)
        return drive(inner)
    
    def consumeDatapoints(self):
        '''
           push messages from opentsdb queue to the TSDB server
        '''
        log.debug("consumeDatapoints")
        start = time.time()
        try:
            q = QueueHandler() 
            q._delete = False
            messages = q.get('opentsdb')
            for message in messages:
                result = self._tsd.send(message)
                if result != len(message):
                    log.warn('failed TSDB delivery for %s...requeuing' % message)
                    q.messages = messages
                    q.put("opentsdb")
            end = time.time() - start
            avg = len(messages)/(end+.01)
            log.info("pushed %s datapoints in %0.1f seconds at %0.1f msg/s" % (len(messages),end,avg))
        except:
            log.warn("problem collecting data")
     
if __name__ == '__main__':
    myPreferences = ZenOpenTSDBPushPreferences()
    myTaskFactory = SimpleTaskFactory(ZenOpenTSDBPushTask)
    myTaskSplitter = SimpleTaskSplitter(myTaskFactory)

    daemon = CollectorDaemon(myPreferences, myTaskSplitter)
    daemon.run()

