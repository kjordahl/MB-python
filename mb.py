#!/usr/bin/env python2.7
"""
mb.py: module for interacting with MB-System objects,
       including in a PostGIS database.

Requirements: working PostgreSQL installation with PostGIS enabled
              python2.7 (may work on previous 2.x versions, but untested)
              psycopg2 for calling postgreSQL from within Python
              MB-System <http://www.ldeo.columbia.edu/res/pi/MB-System>

Author: Kelsey Jordahl
Version: pre-alpha
Copyright: Kelsey Jordahl 2010
License: GPLv3
Time-stamp: <Thu Dec  2 17:38:16 EST 2010>

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
        
    def sql(self,table):
        """return an SQL string containing navigation for a cruise

        Parse a .fnv fast nav file and return a line of SQL code to
        INSERT into database.  Currently the database name is a global
        variable, that should change.
        """
        
        if self.procfile:
            datafile = self.procfile         # use processed file if available
        else:
            datafile = self.filename

        try:
            f = open(os.path.join(self.dirname,self.fnvfile),'r')
        except:
            # no need to crash - just warn
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            print "WARNING: File open failed!\n ->%s" % (exceptionValue)
            f = []                          # make empty list to iterate over
            #         sys.exit("File open failed!\n ->%s" % (exceptionValue))

        sql = "INSERT INTO " + table + " (filename, directory, mbformat, cruiseid, track)"
        # TODO fix security issues with preformatting string
        # cursor.copy_from() would probably be better
        sql = sql + " VALUES (%s,%s,%s,%s,ST_GeomFromText('LINESTRING("
        linecount = 0;
        point = ""

        # parse the .fnv file
        try:
            for line in f:
                (lat, lon, t) = get_navpoint(line);
                # simple filtering
                # TODO: what if data actually approach lat=lon=0?
                if (abs(lon) < 1 and abs(lat) < 1) or lat < -89:
                    self.badnavpoint()
                else:
                    point = "%s %s" % (lon, lat)
                    
                if point:
                    if linecount == 0:
                        sql = sql + point
                    else:
                        sql = sql + "," + point
                    linecount = linecount + 1
        finally:
            if isinstance(f,file):
                f.close()
        sql = sql + ")',4326));"
        if linecount > 1:
            return sql
            id = id + 1
        else:
            print "%d line(s) read - not included in database" % linecount
            return ""

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
