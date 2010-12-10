#!/usr/bin/env python2.7
"""
MBpython cruise metadata

A simple program to get some cruise metadata for cruises in a PostGIS database.
Intended for use on a table that contains datafiles loaded by the accompanying
program mbnav2sql.py, but should work for any table that contains a field
called "cruise_id".

Currently reads metadata from the Marine Geoscience Data System (MGDS)
at Lamont-Doherty Earth Observatory (LDEO) <http://www.marine-geo.org>.
Only a few fields are actively parsed.

Author: Kelsey Jordahl
Version: alpha
Copyright: Kelsey Jordahl 2010
License: GPLv3
Time-stamp: <Fri Dec 10 11:24:26 EST 2010>

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
    print "dbname:", args.dbname
    print "username:", args.username
    fulltable = args.schema + "." + args.table

    # connect to PostGIS database
    conn_string = "host=%s dbname=%s user=%s" % ( args.hostname, args.dbname, args.username )
    print "Connecting to database\n	->%s" % (conn_string)
    try:
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        print "Connected!\n"
    except:
        exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
        sys.exit("Database connection failed!\n ->%s" % (exceptionValue)) 

    # get count of lines in database
    cursor.execute("SELECT DISTINCT cruise_id FROM %s ORDER BY cruise_id;" % (fulltable))
    count = 0
    cruises = cursor.fetchall()
    n = len(cruises)
    for tup in cruises:
        (cruiseid,) = tup
        c = mb.Cruise(cruiseid)
        c.mgds()
        if c.platform:
            count += 1
            print cruiseid, c.platform
            print c.project
            if c.ports:
                # this will be a list
                print 'Ports:', c.ports
    print 'Found', count, 'of', n, 'cruises.'
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MB-System python tools')
#    parser.add_argument('-o', '--output')
    parser.add_argument('-v', dest='verbose', action='store_true')
    parser.add_argument('-H', '--hostname', dest='hostname', default='localhost', help='postgreSQL server hostname (default "localhost")')
    parser.add_argument('-s', '--schema', dest='schema', default='multibeam', help='postgreSQL schema (default "multibeam")')
    parser.add_argument('-d', '--dbname', dest='dbname', default='gis_test', help='postgreSQL database name (default "gis_test")')
    parser.add_argument('-t', '--table', dest='table', default='datafiles', help='postgreSQL table name (default "datafiles")')
    parser.add_argument('-u', '--username', dest='username', default='gis', help='postgreSQL username (default "gis")')
    args = parser.parse_args()
    main(args)
