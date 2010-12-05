#!/usr/bin/env python2.7
"""
mb.py: module for interacting with MB-System objects,
       including in a PostGIS database.

Requirements: working PostgreSQL installation with PostGIS enabled
              python2.7 (may work on previous 2.x versions, but untested)
              psycopg2 for calling postgreSQL from within Python
              MB-System <http://www.ldeo.columbia.edu/res/pi/MB-System>

Author: Kelsey Jordahl
Version: alpha
Copyright: Kelsey Jordahl 2010
License: GPLv3
Time-stamp: <Sun Dec  5 09:58:40 EST 2010>

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
import re
import argparse
import psycopg2
from datetime import datetime, date, time
import tempfile

class Datafile(object):
    """Class for MB-System datafile.

    May include metadata, processed and unprocessed datafile names,
    and ancillary files such as .fnv, .fbt and .inf files.
    """

    badnav = 0
    
    def __init__(self,filename):
        """By default a new instance of Datafile will contain the
        filename passed, and it will check for ancillary files
        associated with it.  In particular, it will check for a .inf
        file and read it entirely into self.info"""

        self.filename = os.path.basename(filename)
        self.dirname = os.path.dirname(filename)
        self.procfile = None
        self.pars = None
        self.cruiseid = None
        self.badsql = False             # flag for SQL error
        if os.path.exists(os.path.join(self.dirname,self.filename) + '.par'):
            self.parfile = self.filename + '.par'
        else:
            self.parfile = None
        if os.path.exists(os.path.join(self.dirname,self.filename) + '.inf'):
            self.inffile = self.filename + '.inf'
            f = open(os.path.join(self.dirname,self.inffile),'r')
            self.info = f.read()
            f.close()
        else:
            self.inffile = None
            self.info = None
        if os.path.exists(os.path.join(self.dirname,self.filename) + '.fbt'):
            self.fbtfile = self.filename + '.fbt'
        else:
            self.fbtfile = None
        if os.path.exists(os.path.join(self.dirname,self.filename) + '.fnv'):
            self.fnvfile = self.filename + '.fnv'
        else:
            self.fnvfile = None
            
    @property
    def records(self):
        """Return number of records in datafile"""
        if not self.info:
            print "no self.info"
            return None
            # TODO: parse output of mbinfo command
        else:
            match = re.search(r'Number of Records:\s+(\d+)',self.info)
            return int(match.group(1))

    @property
    def starttime(self):
        """Return timestamp of start of data"""
        if not self.info:
            print "no self.info"
            return None
            # TODO: parse output of mbinfo command
        else:
            match = re.search(r'Start of Data:\s*\nTime:\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+):(\d+):(\d+)\.(\d+)',self.info)
            try:
                d = date(int(match.group(3)),int(match.group(1)),int(match.group(2)))
                t = time(int(match.group(4)),int(match.group(5)),int(match.group(6)),int(match.group(7)))
                return datetime.combine(d, t)
            except:
                print "Time/Date failed!"
                return None

    @property
    def endtime(self):
        """Return timestamp of end of data"""
        if not self.info:
            print "no self.info"
            return None
            # TODO: parse output of mbinfo command
        else:
            match = re.search(r'End of Data:\s*\nTime:\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+):(\d+):(\d+)\.(\d+)',self.info)
            try:
                d = date(int(match.group(3)),int(match.group(1)),int(match.group(2)))
                t = time(int(match.group(4)),int(match.group(5)),int(match.group(6)),int(match.group(7)))
                return datetime.combine(d, t)
            except:
                print "Time/Date failed!"
                return None

    def setformat(self,format):
        self.format = int(format)

    def badnavpoint(self):
        Datafile.badnav += 1

    def showbadnav(self):
        return Datafile.badnav

    def useproc(self):
        """Use processed datafiles for future operations if available.
        Similar to $PROCESSED directive in an MB datalist
        """
        # this could be in __init__, but would be slower if not used
        if self.parfile:
            self.procfile = self.parameter('OUTFILE')
            if self.procfile:
                # set ancillary files to processed versions
                if os.path.exists(self.procfile + '.inf'):
                    self.inffile = self.procfile + '.inf'
                    f = open(self.inffile,'r')
                    self.info = f.read()
                    f.close()
                if os.path.exists(self.procfile + '.fbt'):
                    self.fbtfile = self.procfile + '.fbt'
                if os.path.exists(self.procfile + '.fnv'):
                    self.fnvfile = self.procfile + '.fnv'

    def parameter(self,param):
        """Read an arbitrary parameter from the processing parameters
        file if it exists, None otherwise.  Will always return value
        as a string, there is no testing for numerical values.  User
        will need to cast as a float or int as appropriate.
        """
        if self.parfile:
            if not self.pars:
                f = open(os.path.join(self.dirname,self.parfile),'r')
                self.pars = f.read()
                f.close()
            param = param + '\s+(.+)'
            match = re.search(param,self.pars)
            if match:
                 return match.group(1)
            else: return None
        else:
            return None
        
    def sql(self,args,cur):
        """Add navigation and metadata for a multibeam datafile to a
        PostGIS database.

        Cursor cur must already have been opened by psycopg2 calls.
        """

        fulltable = args.schema + "." + args.table # multibeam datalist table
        temptable = 'tempfnv1234'  # name for temporary table storing nav points

        if self.procfile:
            datafile = self.procfile         # use processed file if available
        else:
            datafile = self.filename

        # copy navigation to a temporary table
        npoints = self.copy_nav(args,temptable,cur)
        # conn.commit()
        if npoints > 1:             # make a line
            sql = "INSERT INTO %s (filename, directory, mbformat, cruise_id, records, start_time, end_time, track)" % (fulltable)
            sql = sql + ' VALUES (%s,%s,%s,%s,%s,%s,%s,(SELECT ST_MakeLine(tmp_point)::geography FROM ' + args.schema + '.' + temptable + '));'
        elif npoints == 1:          # only one nav point
            sql = "INSERT INTO %s (filename, directory, mbformat, cruise_id, records, start_time, end_time, track)" % (fulltable)
            sql = sql + ' VALUES (%s,%s,%s,%s,%s,%s,%s,(SELECT tmp_point::geography FROM ' + args.schema + '.' + temptable + '));'
        elif npoints == 0:          # no navigation - insert metadata only
            sql = "INSERT INTO %s (filename, directory, mbformat, cruise_id, records, start_time, end_time)" % (fulltable)
            sql = sql + ' VALUES (%s,%s,%s,%s,%s,%s,%s);'
        if args.verbose:
            print sql
        try:
            cur.execute(sql,(self.filename,self.dirname,self.format,self.cruiseid,self.records,self.starttime,self.endtime))
        except:
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            print 'SQL Error: %s' % exceptionValue
            print '%s not written to database' % self.filename
            self.badsql = True

    def copy_nav(self,args,temptable,cur):
        """Copy the contents of the .fnv file associated with a
        multibeam data file to a temporary PostGIS table for import as
        a LINESTRING.  Use of the copy_from() function is much faster
        than reading it into an INSERT statement as text.

        Temporary table will contain the following columns:
            lon FLOAT,
            lat FLOAT,
            tmp_point POINT
        """
        fulltable = args.schema + '.' + temptable
        try:
            f = open(os.path.join(self.dirname,self.fnvfile),'r')
        except:
            # no need to crash - just warn
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            print "WARNING: File open failed!\n ->%s" % (exceptionValue)
            f = []                          # make empty list to iterate over
        t = tempfile.TemporaryFile()
        for line in f:
            fields=line.split();
            t.write('%s %s\n' % (fields[7],fields[8]))
        t.seek(0)
        sql = "DROP TABLE IF EXISTS %s;" % (fulltable)
        if args.verbose:
            print sql
        cur.execute(sql);
        sql = 'CREATE TABLE %s (id SERIAL PRIMARY KEY, lon FLOAT, lat FLOAT);' %  (fulltable)
        if args.verbose:
            print sql
        cur.execute(sql);
        sql = "SELECT AddGeometryColumn(%s,%s,'tmp_point','4326','GEOMETRY',2);"
        if args.verbose:
            print sql
        cur.execute(sql,(args.schema,temptable));
        cur.copy_from(t, fulltable, sep= ' ', columns=('lon','lat'))
        sql = 'UPDATE %s SET tmp_point = ST_SetSRID(ST_MakePoint(lon,lat),4326);' %  (fulltable)
        if args.verbose:
            print sql
        cur.execute(sql);
        t.seek(0)
        return len(t.readlines())
        f.close()

# point parsing as a function, not a method.  Should there be a point class?
def get_navpoint(line):
    """ Parse a line of .fnv file to return longitude, latitude

    (currently does not return timestamp for each point, but it could)

    """
    
    t = 0                               # no timestamp (faster parsing)
    fields=line.split();
    if len(fields)>9:               # minimal error checking
        if t:                       # get timestamp
            year=int(fields[0])
            month=int(fields[1])
            day=int(fields[2])
            hour=int(fields[3])
            minute=int(fields[4])
            second=int(fields[5].floor)
            microsecond=(float(fields[5])-second)*1e6
            d = date(year, month, day)
            t = time(hour, minute, second, microsecond)
            t = datetime.combine(d, t)

        # TODO?: could switch to just sending string lat/lon for efficiency,
        #        but would lose ability to filter valid nav points
        #        and would be a potential security issue in SQL query
        lon=float(fields[7])
        if lon > 180:                   # wrap hemisphere
            lon = lon - 360
        lat=float(fields[8])
        return (lat, lon, t)
    else:
        print len(fields), fields
        return ()

# main() for testing purposes only
def main(args):
    print "hostname:", args.hostname
    print "schema:", args.schema
    print "table:", args.table
    print "datalist:", args.datalist

if __name__ == '__main__':
    # parse command line args
    parser = argparse.ArgumentParser(description='MB-System python tools')
#    parser.add_argument('-o', '--output')
    parser.add_argument('-v', dest='verbose', action='store_true')
    parser.add_argument('-H', '--hostname', dest='hostname', default='localhost', help='postgreSQL server hostname (default "localhost")')
    parser.add_argument('-s', '--schema', dest='schema', default='multibeam', help='postgreSQL schema (default "multibeam")')
    parser.add_argument('-t', '--table', dest='table', default='datafiles', help='postgreSQL table name (default "datafiles")')
    parser.add_argument('-I', '--datalist', dest='datalist', default='datalist.mb-1', help='MB datalist file (default "datalist.mb-1")')
    args = parser.parse_args()
    main(args)
