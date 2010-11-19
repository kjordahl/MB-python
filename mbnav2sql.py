#!/usr/bin/env python2.7
"""
load MB .fnv files from datalist and load into PostGIS database

Author: Kelsey Jordahl
Version: pre-alpha
Copyright: Kelsey Jordahl 2010
License: GPLv3
Time-stamp: <Thu Nov 18 22:43:02 EST 2010>

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
        
    d = mb.Datafile("/Volumes/Data/multibeam/surveys/LDEO/EW9106/hs_ew9106_305.d01.mb24")
    print d.filename
    print d.parfile
    
    # connect to PostGIS database
    conn_string = "host='" + args.hostname + "' dbname='" + args.dbname + "' user='" + args.username + "'"
    print "Connecting to database\n	->%s" % (conn_string)
    try:
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        print "Connected!\n"
    except:
        exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
        sys.exit("Database connection failed!\n ->%s" % (exceptionValue)) 

#    print p.stdout.read()
#    sys.exit("bail for testing")
    
    # create the table
    #CREATE SCHEMA multibeam AUTHORIZATION kels;   # if schema doesn't exist
    cursor.execute("BEGIN;")
    sql = "DROP TABLE IF EXISTS " + fulltable + ";"
    print sql
    cursor.execute(sql)
    # for GEOGRAPHY
    #sql = "CREATE TABLE " + table + " (file_id SERIAL PRIMARY KEY, track GEOGRAPHY);"
    # for GEOMETRY
    sql = "CREATE TABLE " + fulltable + " (file_id SERIAL PRIMARY KEY, filename VARCHAR(50), directory VARCHAR(200), mbformat INT, starttime TIMESTAMP, endtime TIMESTAMP, records INT, cruiseid VARCHAR(30));"
    cursor.execute(sql);
    print sql
    sql = "SELECT AddGeometryColumn('" + args.schema + "','" + args.table + "','the_geom','4326','GEOMETRY',2);"
    print sql
    cursor.execute(sql);

    id = 1;
    records = 0
    lines = p.stdout.read().split('\n')
    numfiles = len(lines)
    for line in lines:
        fields = line.split();
        if fields:
            d = mb.Datafile(fields[0]);
            print "file", id, "of", numfiles, ":", os.path.basename(d.filename)
            if d.inffile:
                print "Records:", d.records
                records += d.records
                print "starttime:", d.starttime
                print "endtime:", d.endtime
            else:
                print "no inffile for", d.filename
            d.setformat(fields[1])
            print "MB format:", d.format, "\n"
        sql = d.sql(fulltable)

        # only insert into database if valid string is returned
        if sql:
            try:
                cursor.execute(sql);
            except:
                exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
                sys.exit("SQL command failed!\n ->%s" % (exceptionValue))

            id = id + 1

    try:
        conn.commit()
        cursor.close()
        conn.close()
        print "%d bad nav points ignored" % d.showbadnav()

    except:
        #        print datetime.combine(d, t), float(lon), float(lat)
        exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
        sys.exit("Barf!\n ->%s" % (exceptionValue)) 

# end main()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='add MB data files to PostGIS database')
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
