#!/usr/bin/env python2.7
"""
MBpython testing script

Author: Kelsey Jordahl
Version: pre-alpha
Copyright: Kelsey Jordahl 2010
License: GPLv3
Time-stamp: <Fri Nov 19 08:12:27 EST 2010>

    This program is free software: you can redistribute it and/or
    modify it under the terms of the GNU General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.  A copy of the GPL
    version 3 license can be found in the file COPYING or at
    <http://www.gnu.org/licenses/>.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

"""

import sys
import os
import subprocess                       # could use os.system instead
import argparse
import psycopg2
import re
from datetime import datetime, date, time
import mb

def main(args):
    print "hostname:", args.hostname
    print "schema:", args.schema
    print "table:", args.table
    print "datalist:", args.datalist
    print "dbname:", args.dbname
    print "username:", args.username
    fulltable = args.schema + "." + args.table

    try:
        # get list of unprocessed datafiles
        p = subprocess.Popen(['mbdatalist','-U','-I',args.datalist],stdout=subprocess.PIPE)

    except:
        exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
        sys.exit("Datalist open failed!\n ->%s" % (exceptionValue))
        
    records = 0
    lines = p.stdout.read().split('\n')
    for line in lines:
        fields = line.split();
        if fields:
            d = mb.Datafile(fields[0]);
            d.useproc()
            if d.inffile:
                print "Records:", d.records
                records += d.records
                print "starttime:", d.starttime
                print "endtime:", d.endtime
                print "procfile:", d.procfile
                print "NAVDRAFT:", d.parameter('NAVDRAFT')
                print "EDITSAVEFILE:", d.parameter('EDITSAVEFILE')
            else:
                print "no inffile for", d.filename
                print "starttime:", d.starttime
            d.setformat(fields[1])
            print "MB format:", d.format
    print "Total records: ", records

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MB-System python tools')
#    parser.add_argument('-o', '--output')
    parser.add_argument('-v', dest='verbose', action='store_true')
    parser.add_argument('-H', '--hostname', dest='hostname', default='localhost', help='postgreSQL server hostname (default "localhost")')
    parser.add_argument('-s', '--schema', dest='schema', default='multibeam', help='postgreSQL schema (default "multibeam")')
    parser.add_argument('-d', '--dbname', dest='dbname', default='gis_test', help='postgreSQL database name (default "gis_test")')
    parser.add_argument('-t', '--table', dest='table', default='datafiles', help='postgreSQL table name (default "datafiles")')
    parser.add_argument('-u', '--username', dest='username', default='gis', help='postgreSQL username (default "gis")')
    parser.add_argument('-I', '--datalist', dest='datalist', default='datalist.mb-1', help='MB datalist file (default "datalist.mb-1")')
    args = parser.parse_args()
    main(args)
