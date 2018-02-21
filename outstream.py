import sys
import time


for i in range(5):
    time.sleep(1)
    print sys.argv[1]
    # ~ sys.stdout.flush()

while True:
    time.sleep(1)
