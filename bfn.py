#!/usr/bin/env python3

import os
import time
import sys
import random
import socket
import threading
import queue
from bfn_config import config


def get_msg_id():
	return random.randint(100000, 1000000)

def parse_addr(addr):
	return (addr.split(':')[0], int(addr.split(':')[1]))

def unparse_addr(ip, port):
	return '{}:{}'.format(ip, port)

class Peer(threading.Thread):
	def __init__(self, addr, basedir=None):
		threading.Thread.__init__(self) 
		self.addr = addr
		self.neighbors = set()
		self.pong_queue = dict()
		self.answer_msg_id = None

		if not basedir:
			self.basedir = os.path.dirname(os.path.realpath(__file__))
		else:
			self.basedir = basedir

	def run(self):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		 
		#Bind socket to local host and port
		try:
			s.bind(self.addr)
		except socket.error as msg:
			print('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
			sys.exit()
			 
		s.listen(10)
		print('Peer Socket bound and listening on {}'.format(str(self.addr)))

		#now keep talking with the client
		while 1:
			#wait to accept a connection - blocking call
			conn, addr = s.accept()
			#start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.
			handle_request = threading.Thread(target=self.clientthread, args=(conn, addr))
			handle_request.start()

		s.close()
		 
	#Function for handling connections. This will be used to create threads
	def clientthread(self, conn, addr):
		#infinite loop so that function do not terminate and thread do not end.
		while True:
			 
			#Receiving from client
			data = conn.recv(1024).decode()
			if not data: 
				break

			data = data.strip()

			if 'PING' in data:
				if data.count(' ') != 4:
					print('Badly formatted PING message: {}'.format(data))
					break

				magic, msg_id, peer_addr, ttl, hop = data.split(' ')
				peer_addr = parse_addr(peer_addr)

				print('[{}] Received PING from Peer {} with TTL={} and ID={}'.format(str(self.addr),
					 peer_addr, ttl, msg_id))

				self.neighbors.add(peer_addr)

				# add all neighbors from which we received a ping to the pong queue when we
				# get an pong answer
				if not msg_id in self.pong_queue:
					self.pong_queue[msg_id] = set()
				self.pong_queue[msg_id].add(peer_addr)	

				self.answer_msg_id = msg_id

				# flood PING if TTL > 0
				ttl = int(ttl)-1
				hop = int(hop)+1

				if ttl > 0:
					self.ping_neighbors(ttl, hop, msg_id, peer_addr)

				# if the maximum depth is reached, send back pong messages
				# with the ip from the receiving host as payload
				if ttl == 0:
					self.pong(peer_addr, msg_id, self.addr)

			elif 'PONG' in data:
				if data.count(' ') != 2:
					print('Badly formatted PONG message: {}'.format(data))
					break

				magic, pong_msg_id, peer_addr = data.split(' ')
				peer_addr = parse_addr(peer_addr)
				print('[{}] Received PONG from Peer {} with ID={}'.format(str(self.addr), peer_addr, pong_msg_id))
				
				# A PONG is forwarded from clients
				# to clients which sent the PING with the corresponding msg_id
				for peer in self.pong_queue.get(pong_msg_id, []):
					self.pong(peer, pong_msg_id, peer_addr)

					if self.answer_msg_id:
						self.pong(peer, pong_msg_id, self.addr)
						self.answer_msg_id = None

			elif 'QUERY' in data:
				if data.count(' ') != 6:
					print('Badly formatted QUERY message: {}'.format(data))
					break

				magic, msg_id, ttl, hop, peer_addr, destination, filename = data.split(' ')

				destination = parse_addr(destination)
				peer_addr = parse_addr(peer_addr)
				fmt = '[{}] Received QUERY with TTL={} and HOP={} with ID={} for FILENAME={}'
				print(fmt.format(str(self.addr), ttl, hop, msg_id, filename))

				# flood QUERY if TTL > 0
				ttl = int(ttl)-1
				hop = int(hop)+1

				# if we find the filename locally, send back QueryHit
				retval = self.check_query_hit(filename)
				if retval:
					print('[{}] found FILE={} and will transmit to {}'.format(str(self.addr), filename, destination))
					self.sendfile(destination ,msg_id, filename, retval)


				if ttl > 0:
					self.query(ttl=ttl, hop=hop,
						 msg_id=msg_id, destination=destination, filename=filename, exclude_peer=peer_addr)


			elif 'DOWNLOAD' in data:
				if data.count(' ') != 2:
					print('Badly formatted DOWNLOAD message: {}'.format(data))
					break

				magic, msg_id, filename = data.split(' ')

				conn.sendall('READYOK\n'.encode())
				file = conn.recv(1024*10).decode()
				print('[{}] Downloaded file {}'.format(str(self.addr), filename))
				with open(filename, 'w') as f:
					f.write(file)

			# close the connection when the request has been handled
			conn.close()
			break

	def ping_neighbors(self, ttl=1, hop=0, msg_id=None, exclude_ping=None):
		for neighbor in self.neighbors:
			# don't ping the neighbor from which we received the PING 
			if exclude_ping:
				if neighbor != exclude_ping:
					self.ping(neighbor, ttl, hop, msg_id)
			else:
				self.ping(neighbor, ttl, hop, msg_id)

	def ping(self, neighbor=None, ttl=1, hop=0, msg_id=None):
		if neighbor != self.addr: # don't ping ourselves
			self.neighbors.add(neighbor)
			if not msg_id:
				msg_id = get_msg_id()
			# create an INET, STREAMing socket
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.connect(neighbor)
			peer_addr = unparse_addr(*self.addr)
			s.sendall('PING {} {} {} {}\n'.format(msg_id, peer_addr, ttl, hop).encode())
			#s.close()


	def pong(self, recipient, msg_id, peer):
		self.neighbors.add(recipient)
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect(recipient)
		peer_addr = unparse_addr(*peer)
		s.send('PONG {} {}\n'.format(msg_id, peer_addr).encode())
		#s.close()

	def query(self, ttl=1, hop=0, msg_id=None, filename='', peer_addr=None, destination=None, exclude_peer=None):
		if not destination:
			destination = self.addr

		to_query = self.neighbors
		if exclude_peer:
			to_query.remove(exclude_peer)

		for neighbor in to_query:
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.connect(neighbor)
			peer_addr = unparse_addr(*self.addr)
			dest = unparse_addr(*destination)
			s.sendall('QUERY {} {} {} {} {} {}\n'.format(msg_id, ttl, hop, peer_addr, dest, filename).encode())
			#s.close()

	def sendfile(self, peer, msg_id, filename, contents):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect(peer)
		s.sendall('DOWNLOAD {} {}\n'.format(msg_id, filename).encode())
		response = s.recv(1024).decode()
		response = response.strip()
		if response == 'READYOK':
			s.sendall(contents.encode())

	def check_query_hit(self, filename):
		for root, subFolders, files in os.walk(self.basedir):
			if filename in files:
				with open(os.path.join(root, filename), 'r') as fin:
					return fin.read()
		return False


	def print_neighbors(self):
		print('Peer {} has neighbors: {}'.format(str(self.addr), str(self.neighbors)))


def create_local_overlay_by_config():
	overlay = config['overlay']
	BASE_PORT = 56009
	peers = {}
	for host, neighbors in overlay.items():
		addr = ('127.0.0.1', BASE_PORT-(host*100))
		peer = Peer(addr)
		peer.start()
		peers[host] = peer

	peers[8].basedir = '/tmp/'

	time.sleep(1)

	for host, neighbors in overlay.items():
		for n in neighbors:
			addr = ('127.0.0.1', BASE_PORT-(n*100))
			peers[host].ping(addr)

	time.sleep(1)

	for peer in peers.values():
		peer.print_neighbors()

	peers[1].query(ttl=3, msg_id=get_msg_id(), filename='test.txt')

	for peer in peers.values():
		peer.join()

	print('should not reach here')


def main():
	create_local_overlay_by_config()


if __name__ == '__main__':
	main()
