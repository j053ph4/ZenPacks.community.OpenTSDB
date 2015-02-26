import pika
from pika import exceptions

class QueueHandler(object):
    ''''''
    _host = None
    _vhost = None
    _user = None
    _password = None
    _parameters = None
    _connection = None
    _exchange = ''
    _channel = None
    _queue = None
    _key = 'raw'
    _delete = False
    _publish = True
    results = []
    count = 0
    message = None
    messages = []
    
    def __init__(self, host='localhost', vhost='/opentsdb', user='opentsdb', password='opentsdb'):
        self._host = host
        self._vhost = vhost
        self._user = user
        self._password = password
        self._parameters = pika.ConnectionParameters(host=self._host, 
                                                    virtual_host=self._vhost, 
                                                    credentials=pika.PlainCredentials(self._user, self._password))
    
    def on_connected(self, connection): connection.channel(self.on_channel_open)
    
    def on_channel_open(self, new_channel):
        """Called when our channel has opened"""
        self._channel = new_channel
        if self._publish is True:
            self._channel.queue_declare(queue=self._queue, callback=self.on_queue_publish)
        else:
            self._channel.queue_declare(queue=self._queue, callback=self.on_queue_consume)
    
    def on_queue_publish(self, frame):
        """Called when RabbitMQ has told us our Queue has been declared, frame is the response from RabbitMQ"""
        for msg in self.messages:
            self._channel.basic_publish(exchange=self._exchange, routing_key=self._queue, body=msg)   
        self.close()
    
    def on_queue_consume(self, frame):
        """Called when RabbitMQ has told us our Queue has been declared, frame is the response from RabbitMQ"""
        self.count = frame.method.message_count
        if self.count > 0: 
            self._channel.basic_consume(self.handle_delivery, queue=self._queue)
        else:
            if self._delete is True:
                self._channel.queue_delete(queue=self._queue, nowait=True)
            self.close()
    
    def handle_delivery(self, channel, method, header, body):
        """Called when we receive a message from RabbitMQ"""
        self.results.append(body)
        if self.count == method.delivery_tag: 
            # ack multiple messages
            self._channel.basic_ack(delivery_tag=method.delivery_tag, multiple=True)
            self.close()
    
    def connect(self):
        self._connection = pika.SelectConnection(parameters=self._parameters, on_open_callback=self.on_connected)
        try:  self._connection.ioloop.start()
        except:  self.close()
    
    def close(self):
        '''close the connection'''
        #print 'close'
        try: self._connection.close()
        except:  pass
        try: self._connection.ioloop.start()
        except:  pass
        self._connection = None
        
    def put(self, queue):
        '''put a new message into the queue'''
        self._publish = True
        self._queue = queue
        self.connect()
    
    def get(self, queue):
        '''consume a queue'''
        self._publish = False
        self._queue = queue
        self.results = []
        self.connect()
        return self.results

    
