"""
OpenTSDBQueueService
ZenHub service for providing configuration to the zenopentsdbqueue collector daemon.
    This provides the daemon with a dictionary of datapoints for every device.
"""
import logging
log = logging.getLogger('zen.zenopentsdbqueue')

import Globals,re
from Products.ZenCollector.services.config import CollectorConfigService


class OpenTSDBQueueService(CollectorConfigService):
    """
    ZenHub service for the zenopentsdb collector daemon.
    """
    def __init__(self, dmd, instance):
        deviceProxyAttributes = (
                                 'zOpenTSDBServer',
                                 'zOpenTSDBPort',
                                 )
        CollectorConfigService.__init__(self, dmd, instance, deviceProxyAttributes)
        
    def _filterDevice(self, device):
        filter = CollectorConfigService._filterDevice(self, device)
        return filter
    
    def _createDeviceProxy(self, device):
        log.debug('creating proxy for %s' % device.id)
        proxy = CollectorConfigService._createDeviceProxy(self, device,)
        proxy.configCycleInterval = 600
        
        proxy.datapoints = []
        proxy.tsdbServer = device.zOpenTSDBServer
        proxy.tsdbPort = device.zOpenTSDBPort
        perfServer = device.getPerformanceServer()
        proxy.mqServer = perfServer.id
        proxy.primary_url_path = device.getPrimaryUrlPath()
        self._getDataPoints(proxy, device, device.id, None, perfServer)
        
        for component in device.getMonitoredComponents():
            self._getDataPoints( proxy, component, component.device().id, component.id, perfServer)
        
        log.debug("found %d datapoints" % len(proxy.datapoints))
        return proxy
    
    def _getDataPoints(self, proxy, deviceOrComponent, deviceId, componentId, perfServer):
        #log.debug("getting datapoints for object %s " % deviceOrComponent)
        for template in deviceOrComponent.getRRDTemplates():
            dataSources = [ds for ds
                           in template.getRRDDataSources()
                           if ds.enabled]
            #log.debug("found %d datasources" % len(dataSources))
            for ds in dataSources:
                for dp in ds.datapoints():
                    path = '/'.join((deviceOrComponent.rrdPath(), dp.name()))
                    basename = "%s.%s" % (ds.id,dp.id)
                    metricname = re.sub(' ','_', basename)
                    dpInfo = dict(
                        tpId = template.id,
                        devId = deviceId,
                        compId = componentId,
                        dsId = ds.id,
                        dpId = dp.id,
                        metadata = {
                                    'name': metricname,
                                    'tags' : {
                                              'device': re.sub(' ','_',deviceId),
                                              'datasource' : re.sub(' ','_',ds.id),
                                              'datapoint': re.sub(' ','_',dp.id),
                                              'template': re.sub(' ','_',template.id),
                                              'rrdtype': dp.rrdtype,
                                              },
                                     },
                        path=path,
                        rrdType=dp.rrdtype,
                        rrdCmd=dp.getRRDCreateCommand(perfServer),
                        minv=dp.rrdmin,
                        maxv=dp.rrdmax,
                        )
                    if componentId:
                        dpInfo['metadata']['tags']['metatype'] = deviceOrComponent.meta_type
                        dpInfo['metadata']['tags']['component'] = re.sub(' ','_',str(componentId))
                        dpInfo['componentDn'] = getattr(
                            deviceOrComponent, 'dn', None)
                    proxy.datapoints.append(dpInfo)

if __name__ == '__main__':
    from Products.ZenHub.ServiceTester import ServiceTester
    tester = ServiceTester(OpenTSDBQueueService)
    def printer(config):
        print config.datapoints
    tester.printDeviceProxy = printer
    tester.showDeviceInfo()

