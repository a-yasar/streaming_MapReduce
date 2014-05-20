from optparse import OptionParser
from operators import Source, Mapper, Reducer
from states import StateManager
import os, logging, imp, Queue

def parse_arguments(args):
	if(len(args) != 3):
		print 'Invalid number of arguments'
		print 'Usage: %s TASKFILE FILE' % (args[0])
		print 'Arguments:'
		print '   TASKFILE \t Script that contains map and update functions'
		print '   FILE \t File that will be processed'
		return (None, None)
	else:
		taskfile = args[1]
		filename = args[2]
	return (taskfile, filename)

def prepare_taskfile(taskfile):
	path = os.path.dirname(taskfile)
	taskmodulename = os.path.splitext(os.path.basename(taskfile))[0]
	fp, pathname, description = imp.find_module(taskmodulename, [path])

	try:
		return imp.load_module(taskmodulename, fp, pathname, description)
	finally:
		if fp:
			fp.close()


def main(args):
    taskfile, filename = parse_arguments(args)
    if not taskfile or not filename:
    	return
    else:
		taskmodule = prepare_taskfile(taskfile)
		line_q = Queue.Queue()
		map_q = Queue.Queue()
		reduce_q = Queue.Queue()
		update_q = Queue.Queue()

		source = Source(filename, line_q = line_q)
		mapper = Mapper(taskmodule.mapf, line_q, map_q)
		state_man = StateManager(3, map_q, reduce_q, update_q)
		reducer = Reducer(taskmodule.reducef, reduce_q, update_q)

		source.daemon = True
		mapper.daemon = True
		reducer.daemon = True
		state_man.daemon = True

		source.start()
		mapper.start()
		state_man.start()
		reducer.start()
	
		source.join()
		mapper.join()
		reducer.join()   
		state_man.join()
	

if __name__ == "__main__":
	import sys
	main(sys.argv)