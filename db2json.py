#!/usr/bin/python

import string
import time
from model import *

con = Session().connection()
nodes_count=con.execute("select count(*) from node;").fetchall()
data=con.execute("select count(*) as c,agent from node group by agent order by c desc limit 200").fetchall()
#data=con.execute("select count(*) as c,agent from node where timestamp > %d group by agent order by c desc limit 200" % int(time.time() - 24 * 60 * 60 )).fetchall()

count=10
  
fracs=[r[0] for r in data]
s =0 
for f in fracs[count:]:
   s += int(f)

fracs = fracs[:count] 
fracs.append(s)
   
#labels=[str(r[1]).split('/')[1] for r in data]
labels = []
for agent in data[:count]:
 try:
    if agent==None:
       labels.append("")
    else:
       labels.append(str(agent[1]).split('/')[1][8:])
    pass
 except:
    print agent
    print agent[1]

labels = labels[:count]
labels.append("others")

# Convert query to objects of key-value pairs
 
import collections
import json
objects_list = []
for index, name in enumerate(labels):
    d = collections.OrderedDict()
    objects_list.append([name,fracs[index]])
 
out={}
out['count'] = nodes_count[0][0]
out['data'] = objects_list
j = json.dumps(out)
objects_file = '/srv/btc/chart.json'
f = open(objects_file,'w')
print >> f, j
