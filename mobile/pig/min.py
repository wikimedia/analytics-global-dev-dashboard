import sys

f = open('evan_test').read()

for line in sys.stdin:
	sys.stdout.write(line)
