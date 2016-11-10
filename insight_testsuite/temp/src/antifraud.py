import csv
import sys
from sys import argv
import sets
import collections
import time

class AntiFraud():

	def __init__(self,bulk,stream,output1,output2,output3):
		self._bulk = bulk
		self._stream = stream
		self._op1 = output1
		self._op2 = output2
		self._op3 = output3
		self._bulkdata = self.clean_data(self._bulk)
		self._streamdata = self.clean_data(self._stream)

	def clean_data(self,file):
		# Removes any invalid lines and returns clean data
		data = []
		with open(file,'rU') as ip:
			reader = csv.reader(ip)
			fields = 5 # time, id1, id2, amount, message
			for row in reader:
				if len(row) != fields:
					continue
				else:
					data.append(list(row))
		data = data[1:]
		return data

	def feature1(self, nodes):
		# Checks for a direct connection between two vertices
		# If found then writes trusted to output1.txt, else writes unverified.
		with open(self._op1,'wb') as op:
			for item in self._streamdata:
				if (item[1].lstrip() in nodes) and (item[2].lstrip() in nodes[item[1].lstrip()]):
					op.write("trusted\n")
				else:
					op.write("unverified\n")
		print "Feature-1 done..."

	def feature2(self, nodes):
		# Checks for a connection through a friend.
		# If found then writes trusted to output2.txt, else writes unverified.
		present = None
		with open(self._op2,'wb') as op:
			for item in self._streamdata:
				if (item[1].lstrip() in nodes) and (item[2].lstrip() in nodes[item[1].lstrip()]):
					op.write("trusted\n")
				elif (item[1].lstrip() in nodes) and (item[2].lstrip() not in nodes[item[1].lstrip()]):
					for x in nodes[item[1].lstrip()]:
						if x in nodes and item[2].lstrip() in nodes[x]:
							present = True
							break
						else:
							present = False
					if present is True:
						op.write("trusted\n")
					else:
						op.write("unverified\n")
				else:
					op.write("unverified\n")
		print "Feature-2 done..."

	def connection(self,graph,source, dest):
		# A bredth first search to check the connection between two verices.
		# Returns True if it finds a connection within level 4 from root.
		start = time.time()
		level = 0
		present = None
		visited = set()
		q = collections.deque()
		q.append(source)
		q.append(None)
		while (q and level<5):
			next = q.popleft()
			if next not in visited and next in graph:
				if next is None:
					level = level+1
					q.append(None)
				if next is not None:
					visited.add(next)
					appendset = graph[next]
					if dest in appendset:
						present = True
						break
					else:
						q.extend(graph[next] - visited)
						present = False
			else:
				present = False
				continue
		return present

	def feature3(self,nodes):
		# checks for a connection in degree of 4.
		# If found then writes trusted to output3.txt, else writes unverified.
		with open(self._op3,'wb') as op:
			for item in self._streamdata:
				if item[1].lstrip() in nodes and item[2].lstrip() in nodes[item[1].lstrip()]:
					op.write("trusted\n")
				else:
					present = self.connection(nodes,item[1].lstrip(),item[2].lstrip())
					if present:
						op.write("trusted\n")
					else:
						op.write("unverified\n")
		print "Feature-3 is done..."

	def main(self):
		# Creates a graph as an adjacency list.
		# nodes acts as adjacency list for representing undirected graph.
		# Keys include vertices and values include the vertex's neighbors.
		nodes = dict()
		for item in self._bulkdata:
			if item[1].lstrip() not in nodes:
				if item[2].lstrip() not in nodes:
					nodes[item[1].lstrip()] = set()
					nodes[item[1].lstrip()].add(item[2].lstrip())
					nodes[item[2].lstrip()] = set()
					nodes[item[2].lstrip()].add(item[1].lstrip())
				else:
					nodes[item[1].lstrip()] = set()
					nodes[item[1].lstrip()].add(item[2].lstrip())
					nodes[item[2].lstrip()].add(item[1].lstrip())
			else:
				if item[2].lstrip() not in nodes:
					nodes[item[2].lstrip()] = set()
					nodes[item[2].lstrip()].add(item[1].lstrip())
					nodes[item[1].lstrip()].add(item[2].lstrip())
				else:
					nodes[item[1].lstrip()].add(item[2].lstrip())
					nodes[item[2].lstrip()].add(item[1].lstrip())
		self.feature1(nodes)
		print "Feature-1 is written to %s " % self._op1
		self.feature2(nodes)
		print "Feature-2 is written to %s " % self._op2
		self.feature3(nodes)
		print "Feature-3 is written to %s " % self._op3

if __name__=="__main__":
	print "Starting AntiFraud detection..."

	batch = argv[1]
	stream = argv[2]
	output1 = argv[3]
	output2 = argv[4]
	output3 = argv[5]
	print "Feature-1====> Connection Degree: 1"
	print "Feature-2====> Connection Degree: 2"
	print "Feature-3====> Connection Degree: 4"
	AntiFraud(batch,stream,output1,output2,output3).main()
