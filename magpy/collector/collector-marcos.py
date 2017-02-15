#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import division

import magpy.stream as st
from magpy.database import *
from magpy.opt import cred as mpcred
from magpy.transfer import scptransfer

from magpy.collector.subscribe2marcos import MARCOSsubscription

dbhost = mpcred.lc('cobsdb','host')
dbuser = mpcred.lc('cobsdb','user')
dbpasswd = mpcred.lc('cobsdb','passwd')
dbname = mpcred.lc('cobsdb','db')

remotedb = mysql.connect(host=dbhost,user=dbuser,passwd=dbpasswd,db='testdb')

localdb = mysql.connect(host=dbhost,user=dbuser,passwd=dbpasswd,db='cobsdb')

subscription = MARCOSsubscription(remotedb=remotedb,localdb=localdb,interval=1)
subscription.datainfoid = 'GP20S3EW_111201_0001_0001'
#subscription.keylist = ['x','y','z']
subscription.filepath = '/home/leon/test.bin'

subscription.run()


# Run the process
#import multiprocessing
#p1 = multiprocessing.Process(target=self.storeData, args=(array,paralst,))
#p1 = multiprocessing.Process(target=subscription.run())
#p1.start()
#p1.join()

#subscription2 = MARCOSsubscription(remotedb=remotedb,localdb=localdb,interval=1)
#subscription2.datainfoid = 'POS1_N432_0001_0001'
#subscription2.keylist = ['f','df','var1']

#p2 = multiprocessing.Process(target=subscription2.run())
#p2.start()
#p2.join()

#subscription2.run()


# Eventually define logging

