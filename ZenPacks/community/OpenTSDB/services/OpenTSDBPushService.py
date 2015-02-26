"""
OpenTSDBPushService
ZenHub service for providing configuration to the zenopentsdbput collector daemon.
"""
import logging
log = logging.getLogger('zen.zenopentsdbpush')

import Globals,re
from Products.ZenCollector.services.config import CollectorConfigService


class OpenTSDBPushService(CollectorConfigService):
    """
    ZenHub service for the zenopentsdb collector daemon.
    """
    
    def __init__(self, dmd, instance):
        self.uniqs = []
        deviceProxyAttributes = (
                                 'zOpenTSDBServer',
                                 'zOpenTSDBPort',
                                 )
        CollectorConfigService.__init__(self, dmd, instance, deviceProxyAttributes)
    
    def _filterDevice(self, device):
        '''only need config for one (or more if multiple tsdb servers)device'''
        log.debug("examining device %s" % device)
        filter = CollectorConfigService._filterDevice(self, device)
        #return filter
        dataset = device.zOpenTSDBServer
        if dataset not in self.uniqs and device.productionState!= -1:
            self.uniqs.append(dataset)
            log.debug("found device %s" % device)
            return filter

    def _createDeviceProxy(self, device):
        proxy = CollectorConfigService._createDeviceProxy(self, device)
        log.debug("creating proxy for device %s" % device.id)
        proxy.configCycleInterval = 300
        proxy.device = device.id
        proxy.tsdbServer = device.zOpenTSDBServer
        proxy.tsdbPort = device.zOpenTSDBPort
        return proxy

if __name__ == '__main__':
    from Products.ZenHub.ServiceTester import ServiceTester
    tester = ServiceTester(OpenTSDBPushService)
    def printer(config):
        print config.datapoints
    tester.printDeviceProxy = printer
    tester.showDeviceInfo()

