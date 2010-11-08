#!/usr/bin/env python2.7
#
# load MB .fnv file and load into PostGIS database
#

# Kelsey Jordahl
# Time-stamp: <Mon Nov  8 17:50:16 EST 2010>

import sys
import psycopg2
from datetime import datetime, date, time

# testing on specific file
#file = "/Volumes/Data/multibeam/surveys/LDEO/EW9602/hs_ew9602_120_bcenb.mb24"
file = "/Users/kels/DOTS/VANC04MV/SIOGDC_VANC04MV_20070727125814012_20070727125814012_SBdespike.2002nov11.truep.mb32"
infofile = file + ".inf"
navfile = file + ".fnv"
print navfile
schema = "multibeam"
shorttable = "test"
# concatenate schema.table
table = schema + "." + shorttable


# open the .inf file
try:
    f = open(navfile,'r')

except:
    exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
    sys.exit("File open failed!\n ->%s" % (exceptionValue))

# connect to PostGIS database
#conn_string = "host='chipotle' dbname='gis_test' user='kels'"
# connect local
conn_string = "host='localhost' dbname='gis_test' user='kels'"
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
cursor.execute(sql)
# for GEOGRAPHY
#sql = "CREATE TABLE " + table + " (file_id SERIAL PRIMARY KEY, track GEOGRAPHY);"
# for GEOMETRY
sql = "CREATE TABLE " + table + " (file_id SERIAL PRIMARY KEY);"
cursor.execute(sql);
sql = "SELECT AddGeometryColumn('" + schema + "','" + shorttable + "','the_geom','4326','GEOMETRY',2);"
cursor.execute(sql);
sql = "INSERT INTO " + table + " (file_id, the_geom)"
#cursor.execute("VALUES (1,ST_Geometry('LINESTRING(")

sql = sql + " VALUES (1,ST_GeomFromText('LINESTRING("
first = 1;

# parse the file
for line in f:
    # can the type be set in split?
#    (year,month,day,hour,minute,second,x,lon,lat,q,w,y)=line.split();
    fields=line.split();
    if len(fields)>9:
        year=int(fields[0])
        month=int(fields[1])
        day=int(fields[2])
        hour=int(fields[3])
        minute=int(fields[4])
        second=float(fields[5])
        
        d = date(year, month, day)
        t = time(hour, minute, int(second)) # second is float, round it
        lon=float(fields[7])
        lat=float(fields[8])

        #    print(hour, minute, second)
        #    print datetime.combine(d, t), float(lat), float(lon)
        #    print line,
        if first:
            first=0
        else:
            sql = sql + ","
        sql = sql + str(lon) + " " + str(lat)
    else:
        print len(fields), fields

try:
    sql = sql + ")',4326));"
    cursor.execute(sql);
    conn.commit()
    cursor.close()
    conn.close()

except:
    #    print sql
    print datetime.combine(d, t), float(lon), float(lat)
    
    exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
    sys.exit("Barf!\n ->%s" % (exceptionValue)) 
