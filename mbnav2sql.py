#!/usr/bin/env python2.7
"""
Read multibeam sonar metadata and navigation files from a datalist and
load into a PostGIS database.  Uses tools and ancillary files of
MB-System <http://www.ldeo.columbia.edu/res/pi/MB-System>

Author: Kelsey Jordahl
Version: pre-alpha
Copyright: Kelsey Jordahl 2010
License: GPLv3
Time-stamp: <Sat Dec  4 10:21:36 EST 2010>

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
import mb                               # MB tools for python

def main(args):
    # precision for track simplification (~10 meters depending on latitude)
    dx = 0.0001  # degrees
    if args.verbose:
        print args
    print "hostname:", args.hostname
    print "schema:", args.schema
    print "table:", args.table
    print "datalist:", args.datalist
    print "dbname:", args.dbname
    print "username:", args.username
    fulltable = args.schema + "." + args.table # not a great variable name
    print "fulltable:", fulltable
    # check to see that names are reasonable (alphanumeric)
    if not args.hostname.isalnum() or not args.schema.isalnum() or not args.table.isalnum() or not args.username.isalnum():
        sys.exit("Bad input!")          # TODO: improve this error message
    try:
        # get list of unprocessed datafiles
        p = subprocess.Popen(['mbdatalist','-U','-I',args.datalist],stdout=subprocess.PIPE)

    except:
        exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
        sys.exit("Datalist open failed!\n ->%s" % (exceptionValue))
        
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

    # create the table
    cursor.execute("BEGIN;")
    if args.drop:
        sql = "DROP TABLE IF EXISTS %s;" % (fulltable)
        print sql
        # this would seem better, but can't substitute table name:
        #        cursor.execute("DROP TABLE IF EXISTS (%s);",(fulltable,))
        cursor.execute(sql);
        try:
            sql = """CREATE TABLE %s (file_id SERIAL PRIMARY KEY,
                filename VARCHAR(100),
                directory VARCHAR(200),
                mbformat INT,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                records INT,
                cruise_id VARCHAR(30),
                track GEOGRAPHY);""" % (fulltable)
            cursor.execute(sql);
            print sql
        except:
            sys.exit('Create table failed!')
        # create a geometry column, even if it will not be populated
        sql = "SELECT AddGeometryColumn(%s,%s,'the_geom','4326','LINESTRING',2);"
        print sql
        cursor.execute(sql,(args.schema,args.table));

    id = 1;
    records = 0
    lines = p.stdout.read().split('\n')
    # remove empty lines (found at end)
    lines = filter(None,lines)
    numfiles = len(lines)
    for line in lines:
        sql = ""
        fields = line.split();
        if args.verbose:
            print "datalistline:", line
        if fields:
            d = mb.Datafile(fields[0]);
            if not args.unproc:
                d.useproc()  # use processed files unless specified otherwise
            print "file", id, "of", numfiles, ":", d.filename
            if d.inffile:
                records += d.records
                if args.verbose:
                    print "Records:", d.records
                    print "starttime:", d.starttime
                    print "endtime:", d.endtime
            else:
                print "no inffile for", d.filename
            d.setformat(fields[1])
            if args.cruiseid.lower() == 'none':
                d.cruiseid = None
            else:
                if args.cruiseid.lower() == 'auto':
                    d.cruiseid = os.path.basename(d.dirname)
                else:
                    d.cruiseid = args.cruiseid
            if args.verbose:
                print "Cruise ID:", d.cruiseid
                print "MB format:", d.format
            sql = ""
            d.sql(args,cursor)

        # only insert into database if valid string was returned
        tic = datetime.now()
        if args.geom:
            if args.verbose:
                print "Updating geometry field..."
            sql = 'UPDATE %s SET the_geom = ST_SimplifyPreserveTopology(track::geometry,%s);' % (fulltable, str(dx))
            cursor.execute(sql);
        if args.simplify:
            if args.verbose:
                print "Simplifying geography field...", "\n"
            if args.geom:
                # use GEOMETRY column recast back into GEOGRAPHY,
                # since ST_Simplify does not work on GEOGRAPHY type anyway
                sql = 'UPDATE %s SET track = the_geom::geography;' % (fulltable)
            else:
                # have to call ST_Simplify for GEOGRAPHY column
                sql = 'UPDATE %s SET track = ST_SimplifyPreserveTopology(track::geometry,%s)::geography;' % (fulltable, str(dx))
                                                
            cursor.execute(sql);

        conn.commit()
        id += 1

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
    parser = argparse.ArgumentParser(description='add multibeam sonar metadata and navigation to a PostGIS database')
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='verbose output')
    # always populate the GEOGRAPHY column, no need for this
    # parser.add_argument('-G', '--geography', dest='geog', action='store_true', help='store trackline as GEOGRAPHY type')
    parser.add_argument('-M', '--geometry', dest='geom', action='store_true', help='Store simplified trackline as GEOMETRY type in addition to the default GEOGRAPHY column.  This may be helpful in order to be recognized by some GIS software, including Qgis before version 1.6.  Note that data are in flat x, y fields that have no notion of a spherical (not to mention ellipsoidal) Earth.')
    parser.add_argument('-S', '--simplify', dest='simplify', action='store_true', help='simplify trackline (default will store full resolution trackline; simplification will use less storage and plot faster in GIS tools).  This applies only to the main GEOGRAPHY column; the GEOMETRY column will always be simplified (if populated by the -M argument).')
    parser.add_argument('-D', '--drop-table', dest='drop', action='store_true', help='drop table before inserting new data WARNING: This will delete all existing data in table!')
    parser.add_argument('-H', '--hostname', dest='hostname', default='localhost', help='postgreSQL server hostname (default "localhost")')
    parser.add_argument('-s', '--schema', dest='schema', default='multibeam', help='postgreSQL schema (default "multibeam")')
    parser.add_argument('-d', '--dbname', dest='dbname', default='gis_test', help='postgreSQL database name (default "gis_test")')
    parser.add_argument('-t', '--table', dest='table', default='datafiles', help='postgreSQL table name (default "datafiles")')
    parser.add_argument('-u', '--username', dest='username', default='gis', help='postgreSQL username (default "gis")')
    parser.add_argument('-U', '--unprocessed', dest='unproc', action='store_true', help='Don''t use processed files (default will use processed datafiles if available)')
    parser.add_argument('-c', '--cruiseid', dest='cruiseid', default='auto', help='Manually set cruiseid string for all files.  Default "auto" will use the lowest level subdirectory containing each datafile as the cruise id.  Setting to "none" will leave the cruise id empty.')
    parser.add_argument('-I', '--datalist', dest='datalist', default='datalist.mb-1', help='MB datalist file (default "datalist.mb-1")')
    args = parser.parse_args()
    main(args)
