#!/usr/bin/env python2.7
"""
load MB .fnv files from datalist and load into PostGIS database

Author: Kelsey Jordahl
Version: pre-alpha
Copyright: Kelsey Jordahl 2010
License: GPLv3
Time-stamp: <Thu Nov 11 16:05:55 EST 2010>

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
import psycopg2
from datetime import datetime, date, time

def main():
    # TODO: make these defaults and take command line options
    #datadir = "/Users/kels/DOTS/VANC04MV/"
    #datadir = "/Users/kels/MB-SystemExamples.5.1.0/cookbook_examples/other_data_sets/ew0204survey/"
    datadir = ""
    datalist = datadir + "datalist.mb-1"
    schema = "multibeam"
    shorttable = "test"
    # concatenate schema.table
    table = schema + "." + shorttable
    badnav = 0

    try:
        d = open(datalist,'r')
    except:
        exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
        sys.exit("File open failed!\n ->%s" % (exceptionValue))
    
    # connect to PostGIS database
    conn_string = "host='chipotle' dbname='gis_test' user='kels'"
    print "Connecting to database\n	->%s" % (conn_string)
    try:
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        print "Connected!\n"
    except:
        exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
        sys.exit("Database connection failed!\n ->%s" % (exceptionValue)) 

    # create the table
    #CREATE SCHEMA multibeam AUTHORIZATION kels;   # if schema doesn't exist
    cursor.execute("BEGIN;")
    sql = "DROP TABLE IF EXISTS " + table + ";"
    print sql
    cursor.execute(sql)
    # for GEOGRAPHY
    #sql = "CREATE TABLE " + table + " (file_id SERIAL PRIMARY KEY, track GEOGRAPHY);"
    # for GEOMETRY
    sql = "CREATE TABLE " + table + " (file_id SERIAL PRIMARY KEY);"
    cursor.execute(sql);
    print sql
    sql = "SELECT AddGeometryColumn('" + schema + "','" + shorttable + "','the_geom','4326','GEOMETRY',2);"
    print sql
    cursor.execute(sql);

    id = 1;
    for line in d:
        # can the type be set in split?
        #    (year,month,day,hour,minute,second,x,lon,lat,q,w,y)=line.split();
        fields = line.split();
        file = datadir + fields[0];
        infofile = file + ".inf"
        navfile = file + ".fnv"
        print navfile
        sql = parse_fnv(navfile,table,id)

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
        print "%d bad nav points ignored" % badnav

    except:
        print datetime.combine(d, t), float(lon), float(lat)
        exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
        sys.exit("Barf!\n ->%s" % (exceptionValue)) 

# end main()

def parse_fnv(navfile,table,id):
    """ Parse a .fnv fast nav file and return a line of SQL code to
    INSERT into database.  Currently the database name is a global
    variable, that should change.
    """
    # open the .fbt file
    try:
         f = open(navfile,'r')
    except:
         exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
         sys.exit("File open failed!\n ->%s" % (exceptionValue))

    sql = "INSERT INTO " + table + " (file_id, the_geom)"
    sql = sql + " VALUES (" + str(id) + ",ST_GeomFromText('LINESTRING("
    #    sql = sql + " VALUES (" + str(id) + ",ST_GeomFromText('POINT("
    linecount = 0;

    # parse the .fnv file
    for line in f:
        (lat, lon, t) = get_navpoint(line);
        if lon < 1 and (abs(lat) < 1 or lat < -89):
            print "Bad nav point"
            BADNAV = BADNAV + 1;        # increment GLOBAL bad nav count
        else:
            point = "%s %s" % (lon, lat)

        if point:
            if linecount == 0:
                sql = sql + point
            else:
                sql = sql + "," + point
            linecount = linecount + 1
    sql = sql + ")',4326));"
    if linecount > 1:
        return sql
        id = id + 1
    else:
        print "only %d line read - not included in database" % linecount
        return ""

# end parse_fnv
        
def get_navpoint(line):
    """ Parse a line of .fbt file to return longitude, latitude, Python time

    """

    fields=line.split();
    if len(fields)>9:               # minimal error checking
        year=int(fields[0])
        month=int(fields[1])
        day=int(fields[2])
        hour=int(fields[3])
        minute=int(fields[4])
        second=float(fields[5])

        d = date(year, month, day)
        t = time(hour, minute, int(second)) # second is float, round it
        lon=float(fields[7])
        if lon < 0:                   # wrap eastern hemisphere
            lon = lon + 360
        lat=float(fields[8])
        return (lat, lon, t)
    else:
        print len(fields), fields
        return ()

# end get_navpoint()

if __name__ == '__main__':
    main()
