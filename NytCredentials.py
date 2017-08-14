import os
import sys

try:
    NYTimesApi = os.environ['NYTKey']
except:
    sys.stderr.write("NYT * environment variables not set \n")
    sys.exit(1)
