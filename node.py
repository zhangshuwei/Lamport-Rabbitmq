#!/usr/bin/env python
import pika
import uuid
import sys
import time
from multiprocessing import Process, Pipe, Value, Array, Lock
class Node(object):
	def __init__(self, id, exchanges):
		self.id = id
		self.exchange_id = "exchange_" + str(self.id)
		# declare a queue to receive meg from other exchange
		self.queue_name = "queue_" + str(self.id)
		self.corr_id = str(uuid.uuid4())
		
	def start(self, exchanges):
		#set up connection
		self.setup_connection() 
		#set up queues
		self.setup_queue(exchanges)
		#set up exchanges and bind queue to each exchange       
		self.setup_exchange(exchanges)
		#set up callback queue
		self.setup_callbackQueue()

	def setup_connection(self):
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
		self.channel = self.connection.channel()
	
	def setup_queue(self, exchanges):
		nb_exchange = int(exchanges)
		for i in range(1, nb_exchange+1):
			self.queue_id = "queue_" + str(i)
			self.channel.queue_declare(self.queue_id)
		self.channel.basic_consume(self.on_receive, queue=self.queue_name)

	def setup_exchange(self, exchanges):
		# declare exchange 
		self.channel.exchange_declare(exchange=self.exchange_id, exchange_type="fanout")
		for e in range(1, int(exchanges)+1):
			exchange_id = "exchange_" + str(e)
			self.channel.exchange_declare(exchange=exchange_id, exchange_type="fanout")
			if e != int(self.id):
				self.channel.queue_bind(exchange=exchange_id, queue=self.queue_name) 

	def setup_callbackQueue(self):
		# declare callback queue                   
		result = self.channel.queue_declare(exclusive=True)
		self.callback_queue = result.method.queue
		self.channel.basic_consume(self.on_getting_response, no_ack=True,
									queue=self.callback_queue)

	def on_getting_response(self, ch, method, props, body):
		if self.corr_id == props.correlation_id:
			self.response = body
		print("Get response: %s" % self.response)

	def send_request(self, message):
		self.channel.basic_publish(exchange=self.exchange_id,
			routing_key='',
			properties=pika.BasicProperties(
					reply_to = self.callback_queue,
					correlation_id = self.corr_id,),
			body=message)     
		print("Send request: %s" % message)

	def on_receive(self, ch, method, props, body):                
		response = "Ack from " + str(self.id)        
		self.channel.basic_publish(exchange='',
						routing_key=props.reply_to,
						properties=pika.BasicProperties(correlation_id = \
															props.correlation_id),
						body=str(response))
		ch.basic_ack(delivery_tag = method.delivery_tag)
		print("Get request: ")
		print(body)

if __name__ == "__main__":
	if len(sys.argv) != 3:
        	print("usage: python node.py id numberofnodes")	
	site_id = sys.argv[1]
	nb_exchange = sys.argv[2]
	site = Node(site_id, nb_exchange)
	site.start(nb_exchange)	
	p1 = Process(target=site.channel.start_consuming, args=())
	p1.start()
	while(True):
		func = raw_input('Enter a number:\n 1->send request, 2->listen: \n')
		if func == '':
			continue
		if(int(func) == 1):
			message = "Send request from " + str(site_id)
			site.send_request(message)
		else:
			pass
	p1.join()		
		
	
	
	
	
    
