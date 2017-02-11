import threading
import random

class Test(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.test = []

	def add(self, arg=None):
		self.test.append('{} added {}'.format(self.name, random.random()))

	def run(self):
		thread = threading.Thread(target=self.target, args=(5, ))
		thread.start()

	def target(self, arg):
		self.add()

	def __str__(self):
		return str(self.test)


threads = [Test() for t in range(5)]

for t in threads:
	t.start()

for t in threads:
	print(t.name, str(t))

for t in threads:
	t.join()
