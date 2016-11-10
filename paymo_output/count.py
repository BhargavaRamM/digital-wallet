import sys

def main():
	count = 0
	pls = 0
	with open("output3.txt",'r') as ip:
		data = ip.readlines() 
		for line in data:
			if line == "unverified\n":
				count = count+1
			else:
				pls = pls + 1
	print "Total Count of UVs: %d" % count
	print "Total Count of Ts: %d" % pls

if __name__=="__main__":
	main()
