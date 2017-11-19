#!/usr/bin/env python
import pika
import uuid
import sys
import time
import json
import heapq 
from multiprocessing import Process, Manager, Value, Lock


class Node(object):
	def __init__(self, id, exchanges):
		self.id = int(id)
		self.exchange_id = "exchange_" + str(self.id)
		self.nb_site = int(exchanges)
		# declare a queue to receive meg from other exchange
		self.queue_name = "queue_" + str(self.id)
		self.corr_id = str(uuid.uuid4())
		self.num_response = 0

	def start(self, exchanges):
		#set up initial share value
		self.setup_share_val()
		#set up connection
		self.setup_connection() 
		#set up queues
		self.setup_queue(exchanges)
		#set up exchanges and bind queue to each exchange       
		self.setup_exchange(exchanges)
		#set up callback queue
		self.setup_callbackQueue()
	
	def setup_share_val(self):
		# define a lock
		self.lock = Lock()
		self.timestamp = Value('i', 0)
		self.waiting_q = Manager().list()
	
	def setup_connection(self):
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
		self.channel = self.connection.channel()
	
	def setup_queue(self, exchanges):
		nb_exchange = int(exchanges)
		for i in range(1, nb_exchange+1):
			self.queue_id = "queue_" + str(i)
			self.channel.queue_declare(self.queue_id,durable=True)
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
		result = self.channel.queue_declare(exclusive=True,durable=True)
		self.callback_queue = result.method.queue
		self.channel.basic_consume(self.on_getting_response, no_ack=True,
									queue=self.callback_queue)

	def on_getting_response(self, ch, method, props, body):
		if self.corr_id == props.correlation_id:
			self.response = body
			ack_content = json.loads(body)
			self.num_response += 1
			print("Get ack: %s" % self.response)
			self.lock.acquire() # lock it
			self.timestamp.value = max(self.timestamp.value, ack_content["timestamp"]) + 1
			self.lock.release() # release it
			heapq.heapify(list(self.waiting_q))
			if((self.num_response == self.nb_site - 1) and (self.waiting_q[0]['id'] == self.id)):
				print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>critique exception!>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
				print("yehp! Enter critique exception!")
				time.sleep(5)
				self.timestamp.value += 1
				message = {"timestamp": self.timestamp.value, "id": self.id, "message_type": "release"}
				self.send_request(message)
				self.num_response = 0
		
		print("After ack, Now my timestamp is: %i \n" % self.timestamp.value)
	
	def send_request(self, message):
		self.lock.acquire() # lock it		
		self.timestamp.value += 1
		message["timestamp"] = self.timestamp.value
		if(message["message_type"] == "request"):
			print("Send request: %s" % message)
			self.waiting_q.append(message) # put the request in the waiting queue
		elif(message["message_type"] == "release"):
			print("Send release: %s" % message)
			print("before pop")
			print(list(self.waiting_q))
			if(len(self.waiting_q) > 0):
				ele = heapq.heappop(list(self.waiting_q)) #
				print("after pop is")
				print(ele)
				print('>>>>>>>>>>>>>>>>>>>>>>>>>>release!>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
				ele = heapq.heappop(list(self.waiting_q)) #
				ele = heapq.heappop(list(self.waiting_q)) #
				ele = heapq.heappop(list(self.waiting_q)) #
				print(list(self.waiting_q))
				print("now waiting list")
		self.lock.release() # release it    
		
		message_json = json.dumps(message)
		self.channel.basic_publish(exchange=self.exchange_id,
			routing_key='',
			properties=pika.BasicProperties(
					reply_to = self.callback_queue,
					correlation_id = self.corr_id,
					delivery_mode=2,),
			body=message_json)
		
		print("After sending, timestamp is ", self.timestamp.value)

	def on_receive(self, ch, method, props, body):
		request_content = json.loads(body)
		self.lock.acquire() # lock it
		if(request_content["message_type"] == "request"):
			print("Get request: ")
			print("Id received is:", request_content["id"])
			self.waiting_q.append(request_content) # put the request in the waiting queue
			response = {"id": self.id, "timestamp": self.timestamp.value, "message_type": "response"}        
			self.channel.basic_publish(exchange='',
						routing_key=props.reply_to,
						properties=pika.BasicProperties(correlation_id = \
															props.correlation_id,
														delivery_mode=2,),# persisitant message
						body=json.dumps(response)) 
			ch.basic_ack(delivery_tag = method.delivery_tag)
		elif(request_content["message_type"] == "release"):
			if(len(self.waiting_q) > 0):
				heapq.heappop(list(self.waiting_q))
			print("Get release: ", request_content)
			print("Id received is:", request_content["id"])
		self.timestamp.value = max(self.timestamp.value, request_content["timestamp"]) + 1
		self.lock.release() # release it
		
		
				
		print("Timestamp received is:", request_content["timestamp"])
		
		print("After receive, Now my timestamp is:", self.timestamp.value)


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
		func = raw_input('Enter a number:\n 1->send request, 2->quit: \n')
		if func == "" or (func != "1" and func != "2"):
			print("Enter error")
			continue
		if(int(func) == 2):
			print("quit the connection")
			p1.terminate()
			site.connection.close()
			sys.exit(0)
		if(int(func) == 1):
			message = {"timestamp":site.timestamp.value, "id":site.id, "message_type": "request"}
			print("site timestamp is ", site.timestamp.value)
			site.send_request(message)
		else:
			continue
	#p1.join()		
