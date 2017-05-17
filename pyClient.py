import signal, socket, os, sys, time, json, re, datetime

class PyClient(object):

	def __init__(self, conf):

		super(PyClient, self).__init__()
		self.conf = conf
		self.conf['client.id'] = 'python@' + socket.gethostname()
		self.run = True
		signal.signal(signal.SIGTERM, self.sig_term)
		self.dbg('Pid is %d' % os.getpid())

	def sig_term(self, sig=None, frame=None):
		self.dbg('SIGTERM')
		self.run = False

	@staticmethod
	def _timestamp():
		return time.strftime('%H:%M:%S', time.localtime())

	def dbg(self, s):
		sys.stderr.write('%% %s DEBUG: %s\n' % (self._timestamp(), s))

	def err (self, s, term=False):
		""" Error printout, if term=True the process will terminate immediately. """
		sys.stderr.write('%% %s ERROR: %s\n' % (self._timestamp(), s))
		if term:
			sys.stderr.write('%% FATAL ERROR ^\n')
			sys.exit(1)

	def send (self, d):
		""" Send dict as JSON to stdout for consumtion """
		d['_time'] = str(datetime.datetime.now())
		self.dbg('SEND: %s' % json.dumps(d))
		sys.stdout.write('%s\n' % json.dumps(d))
		sys.stdout.flush()

	@staticmethod
	def set_config (conf, args):
		""" Set client config properties"""
		for n,v in args.items():
			if v is None:
				continue
			#Things to ignore
			if '.' not in n:
				#App config, skip
				continue
			if n.startswith('topic.'):
				# Set "topic.<...>" properties on default topic conf dict
				conf['default.topic.config'][n[6:]] = v
			elif n == 'partition.assignment.strategy':
				#Convert Java class name to config value.
				# "org.apache.kafka.clients.consumer.RangeAssignor" -> "range"
				conf[n] = re.sub(r'org.apache.kafka.clients.consumer.(\w+)Assignor',
								lambda x: x.group(1).lower(), v)
			else:
				conf[n] = v
				