import logging
log = logging.getLogger('zen.zenopentsdbqueue')
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
from ZenPacks.community.OpenTSDB.services.OpenTSDBQueueService import OpenTSDBQueueService
from ZenPacks.community.OpenTSDB.QueueHandler import *
import time
#import pika,socket,time
#from pika import exceptions
unused(Globals)
unused(OpenTSDBQueueService)

class ZenOpenTSDBQueuePreferences(object):
    zope.interface.implements(ICollectorPreferences)
    def __init__(self):
        self.collectorName = 'zenopentsdbqueue'
        self.configurationService = "ZenPacks.community.OpenTSDB.services.OpenTSDBQueueService"
        # How often the daemon will collect each device. Specified in seconds.
        self.cycleInterval = 1800 #10 * 60
        # How often the daemon will reload configuration. In minutes.
        self.configCycleInterval = 360
        self.options = None

    def buildOptions(self, parser): pass
    
    def postStartup(self): pass

class ZenOpenTSDBQueueTask(ObservableMixin):
    zope.interface.implements(IScheduledTask)
    
    CLEAR_EVENT = dict(component="zenopentsdbqueue", severity=Clear, eventClass='/Cmd/Fail')
    WARNING_EVENT = dict(component="zenopentsdbqueue", severity=Warning, eventClass='/Cmd/Fail')
    
    def __init__(self, taskName, deviceId, interval, taskConfig):
        super(ZenOpenTSDBQueueTask, self).__init__()
        self._taskConfig = taskConfig
        self._eventService = zope.component.queryUtility(IEventService)
        self._dataService = zope.component.queryUtility(IDataService)
        self._preferences = zope.component.queryUtility(ICollectorPreferences, 'zenopentsdbqueue')
        self.name = taskName
        self.configId = deviceId
        self.interval = interval
        self.state = TaskStates.STATE_IDLE
        self.messages = []
        
    def _connectCallback(self, result):  pass
    
    def _collectCallback(self, result):
        ''' '''
        log.debug("_collectCallback")
        d = self.fetch()
        d.addCallbacks(self._collectSuccessful, self._failure)
        d.addCallback(self.reQueue)
        d.addCallback(self._collectCleanup)
        return d
    
    def _collectSuccessful(self, result):
        ''' '''
        log.debug("_collectSuccessful")
        self._eventService.sendEvent(ZenOpenTSDBQueueTask.CLEAR_EVENT, device=self.configId, summary="Device collected successfully")
    
    def _collectCleanup(self, result):
        ''' '''
        log.debug("_collectCleanup")
        #self.reQueue()
        self.state = TaskStates.STATE_CLEANING
        return defer.succeed(None)
    
    def cleanup(self):
        ''' '''
        log.debug("cleanup")
        self.state = TaskStates.STATE_COMPLETED
        return defer.succeed(None)
    
    def _failure(self, result):
        ''' '''
        log.debug("_failure")
        err = result.getErrorMessage()
        log.error("Failed with error: %s" % err)
        self._eventService.sendEvent(ZenOpenTSDBQueueTask.WARNING_EVENT, device=self.configId, summary="Error collecting performance data: %s" % err)
        return result
    
    def _finished(self, result):
        ''' '''
        log.debug("_finished")
        self.messages = []
        if not isinstance(result, Failure): log.info("Successful scan of %s completed" % (self.configId))
        else: log.error("Unsuccessful scan of %s completed, result=%s", self.configId, result.getErrorMessage())
        return result
    
    def _connect(self):
        '''connect to RabbitMQ'''
        log.debug("_connect")
        def inner(driver): yield defer.succeed(None)
        return drive(inner)
    
    def doTask(self):
        ''' main task loop '''
        log.debug("doTask")
        self.state = TaskStates.STATE_RUNNING
        d = self._connect()
        d.addCallbacks(self._connectCallback, self._failure)
        # perform synchronization
        d.addCallback(self._collectCallback)
        d.addBoth(self._finished)
        return d
    
    def fetch(self):
        ''' data synchronization between RabbitMQ and OpenTSDB '''
        log.debug("fetch")
        def inner(driver):
            try:
                messages = self.pullData()
                #self.reQueue(messages)
                yield defer.succeed(None)
            except: yield defer.fail("collection failed for %s" % self.configId)
        return drive(inner)
    
    def pullData(self):
        '''
            consume messages from each datapoint's queue, 
            format and push to opentsdb queue
        '''
        start = time.time()
        #self.messages = []
        for datapoint in self._taskConfig.datapoints:
            zqueue = QueueHandler()
            results = zqueue.get(datapoint['path'])
            for r in results:  
                msg = self.tsdbFormat(datapoint['metadata'], eval(r))
                self.messages.append(msg)
        end = time.time() - start
        avg = len(self._taskConfig.datapoints)/(end+.001)
        log.info("pullData %s queues for %s in %0.1f seconds at %0.1f queues/s" % (len(self._taskConfig.datapoints),self.configId, end, avg))
        #return messages
    
    def reQueue(self, result):
        log.debug("reQueue %s messages" % len(self.messages))
        start = time.time()
        tsdbqueue = QueueHandler()
        tsdbqueue.messages = self.messages
        tsdbqueue.put('opentsdb')
        #self.messages = []
        end = time.time() - start
        avg = len(self.messages)/(end+.001)
        log.info("reQueue %s messages for %s in %0.1f seconds at %0.1f queues/s" % (len(self._taskConfig.datapoints),self.configId, end, avg))
        return defer.succeed(None)
    
    def tsdbFormat(self, metadata, perfdata):
        '''
            format message as a put command
        '''
        args = ['put',metadata['name'], str(perfdata['timestamp']), str(perfdata['value'])]
        for k,v in metadata['tags'].items(): args.append('%s=%s' % (str(k),str(v)))
        args.append('\n')
        return ' '.join(args)

if __name__ == '__main__':
    myPreferences = ZenOpenTSDBQueuePreferences()
    myTaskFactory = SimpleTaskFactory(ZenOpenTSDBQueueTask)
    myTaskSplitter = SimpleTaskSplitter(myTaskFactory)

    daemon = CollectorDaemon(myPreferences, myTaskSplitter)
    daemon.run()

