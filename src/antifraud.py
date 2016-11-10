import csv
import sys
from sys import argv
import sets
import collections

class AntiFraud():
	
	def __init__(self,bulk,stream):
		self._bulk = bulk	
		self._stream = stream
		self._bulkdata = self.clean_data(self._bulk)
		self._streamdata = self.clean_data(self._stream)
		# self._network = Graph()
		
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
		return data
		
	def feature1(self, nodes):
		# Direct connection between two nodes
		with open("output1.txt",'wb') as op:
			for item in self._streamdata:
				if (item[1].lstrip() in nodes) and (item[2].lstrip() in nodes[item[1].lstrip()]):
					op.write("trusted\n")
				else:
					op.write("unverified\n")
		print "Feature-1 done..."
		
	def feature2(self, nodes):
		present = None
		print "Stream Data Len: %d" % len(self._streamdata)
		with open("output2.txt",'wb') as op:
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
		#print "Loops lp1: %d\t lp2: %d\t lp3: %d\t lp4: %d\t lp5: %d\n" % (lp1,lp2,lp3,lp4,lp5)
		
	def connection(self,graph,source, dest):
		level = 0
		present = None
		
		q = collections.deque()
		q.append(source)
		q.append(None)
		while (q and level<5):
			next = q.popleft()
			# present = False
			if next is None:
				level = level+1
				q.append(None)
			if next is not None:
				appendset = graph[next]
				if dest in appendset:
					present = True
					break
				else:
					for x in appendset:
						q.append(x)
					present = False
		print "Level at: %d" % level
		if present:
			print "Element looking for is found.."
		else: 
			print "Tough break.."
		return present
	
	def feature3(self,nodes):
		index = 0
		print "Entering Feature-3 method"
		with open("output3.txt",'wb') as op:
			for item in self._streamdata:
				if item[1].lstrip() in nodes and item[2].lstrip() in nodes[item[1].lstrip()]:
					op.write("trusted\n")
				else:
					present = self.connection(nodes,item[1].lstrip(),item[2].lstrip())
					if present:
						op.write("trusted\n")
						print "trusted"
					else:
						op.write("unverified\n")
						print "unverified"
				
		print "Feature-3 is done..."
		
	def main(self):
		nodes = dict()
		for item in self._bulkdata:
			if item[1].lstrip() not in nodes:
				nodes[item[1].lstrip()] = set()
				nodes[item[1].lstrip()].add(item[2].lstrip())
			else:
				nodes[item[1].lstrip()].add(item[2].lstrip())
		# nodes acts as adjacency list for representing graph. Keys include vertices and values include the vertex's neighbors.
		self.feature1(nodes)
		print "Feature-1 is written to output1.txt"
		self.feature2(nodes)
		print "Feature-2 is written to output2.txt"
		self.feature3(nodes)
		print "Feature-3 is written to output3.txt"
		
if __name__=="__main__":
	batch = argv[1]
	stream = argv[2]
	AntiFraud(batch,stream).main()