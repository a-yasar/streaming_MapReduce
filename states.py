from collections import namedtuple
from sets import Set
import os, time, sys
import threading, Queue
import os

DEFAULT_TIMEOUT = 5
FreqStruct = namedtuple("freqStruct", "state freq delta")

class StateManager(threading.Thread):
	def __init__(self, mem_limit, map_q, reduce_q, update_q ):
		super(StateManager, self).__init__()
		self.mem_limit = mem_limit
		self.update_q = update_q
		self.stoprequest = threading.Event()
		self.in_mem_state = {}
		self.currentBucket = 1	#bucket id
		self.numOfItemProcessed = 0	#number of items processed so far in stream
		self.processing_keys = Set()
		self.map_q = map_q
		self.reduce_q = reduce_q
		self.lock = threading.Lock()

	def run(self):
		while not self.stoprequest.set():
			try:
				key, value = self.map_q.get()
				if key in self.processing_keys:
					self.map_q.put((key,value))
				else:
					self.processing_keys.add(key)
					state = self.get_state(key)
					self.reduce_q.put((key, value, state))

				if not self.update_q.empty():
					self._update_state()

			except Queue.Empty:
				continue

	def join(self, timeout=DEFAULT_TIMEOUT):
		self.stoprequest.set()
		super(StateManager, self).join(timeout)
		for key in self.in_mem_state:
			state_path = './states/%s'%(key)
			f = open(state_path, 'w')
			f.write('%s\n'%(str(self.in_mem_state[key])))
			f.close
		self._update_state()

	def get_state(self, key):

		if key in self.in_mem_state:
			return self.in_mem_state[key].state
		else:
			state_path = './states/%s'%(key)
			if os.path.isfile(state_path):
				f = open(state_path, 'r')
				state = f.readline()
				f.close()
				return state
		return None

	def _update_state(self):
		for i in xrange(self.update_q.qsize()):
			(key, state) = self.update_q.get()
			self.numOfItemProcessed += 1
			if key in self.in_mem_state:
				s = self.in_mem_state[key] 
				s._replace(state = state, freq = s.freq + 1)
				self.in_mem_state[key] = s
			elif not key in self.in_mem_state and len(self.in_mem_state) < self.mem_limit:
				self.in_mem_state[key] = FreqStruct(state = state, freq = 1, delta = self.currentBucket-1)

			if self.numOfItemProcessed % self.mem_limit == 0:
				tmpList = []
				for k,v in self.in_mem_state.iteritems():
					if v.freq + v.delta <= self.currentBucket:
						tmpList.append(k)
						state_path = './states/%s'%(k)
						f = open(state_path, 'w')
						f.write('%s\n'%(str(v.state)))
						f.close
				for k in tmpList:
					del self.in_mem_state[k]		
				self.currentBucket += 1			
	
			self.processing_keys.remove(key)
					