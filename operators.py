#!/usr/bin/env/python

import os, time, sys
import threading, Queue

DEFAULT_TIMEOUT = 5
class Source(threading.Thread):

	def __init__(self, filename, line_q):
		super(Source, self).__init__()
		self.file = open(filename, 'r')
		self.line_q = line_q
		self.stoprequest = threading.Event()

	def run(self):
		while not self.stoprequest.set():
			try:
				self.line_q.put(self._next_line())
				time.sleep(1)		
			except Queue.Empty:
				continue

	def join(self, timeout=DEFAULT_TIMEOUT):
		self.stoprequest.set()
		super(Source, self).join(timeout)

	def _next_line(self):
		return self.file.readline()	

class Mapper(threading.Thread):
	"""
		Mapper operator. Read data and update mapper queue. 
		It runs in a streaming manner. For now it simulates
		stream of data.
	"""
	def __init__(self, mapf, task_q, map_q):
		super(Mapper, self).__init__()
		self.mapf = mapf
		self.task_q = task_q
		self.map_q = map_q
		self.stoprequest = threading.Event()

	def run(self):
		while not self.stoprequest.set():
			try:				
				for item in self._map_line():
					self.map_q.put(item)	
				time.sleep(1)				
			except Queue.Empty:
				continue

	def join(self, timeout=DEFAULT_TIMEOUT):
		self.stoprequest.set()
		super(Mapper, self).join(timeout)

	def _map_line(self):
		return self.mapf(self.task_q.get());

class Reducer(threading.Thread):
	"""
		Reducer operator.
	"""
	def __init__(self, reducef, task_q, update_q):
		super(Reducer, self).__init__()
		self.reducef = reducef
		self.task_q = task_q
		self.update_q = update_q
		self.stoprequest = threading.Event()
		self.lock = threading.Lock()

	def run(self):
		while not self.stoprequest.set():
			try:
				key, value = self._reduce_map()
				self.update_q.put((key, value))
				print "%s changed to %s" % (key, value)
			except Queue.Empty:
				continue		

	def join(self, timeout=DEFAULT_TIMEOUT):
		self.stoprequest.set()
		super(Reducer, self).join(timeout)

	def _reduce_map(self):
		key, values, state = self.task_q.get()
		return self.reducef(key, values, state)







