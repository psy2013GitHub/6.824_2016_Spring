

import sys

for line in sys.stdin:
    if len(line.strip()) > 0:
       nline = line.split(':', 1)[0]
       tmp = line.split('rf[%s].log:'%sys.argv[1], 1)[1]
       print nline, len(tmp.split('{'))
