#!/usr/bin/env python2.7
#
# load MB .fnv files from datalist and load into PostGIS database
#

# Kelsey Jordahl
# Time-stamp: <Wed Nov 10 16:16:27 EST 2010>

import sys
import psycopg2
from datetime import datetime, date, time

#datadir = "/Users/kels/DOTS/VANC04MV/"
#datadir = "/Users/kels/MB-SystemExamples.5.1.0/cookbook_examples/other_data_sets/ew0204survey/"
datadir = ""
datalist = datadir + "datalist.mb-1"
schema = "multibeam"
shorttable = "test"
# concatenate schema.table
table = schema + "." + shorttable

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
    # testing on specific file
    #file = "/Volumes/Data/multibeam/surveys/LDEO/EW9602/hs_ew9602_120_bcenb.mb24"
    #file = "/Users/kels/DOTS/VANC04MV/SIOGDC_VANC04MV_20070727125814012_20070727125814012_SBdespike.2002nov11.truep.mb32"
    infofile = file + ".inf"
    navfile = file + ".fnv"
    print navfile

# break here for testing
# quit()

    # open the .fbt file
    try:
        f = open(navfile,'r')

    except:
        exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
        sys.exit("File open failed!\n ->%s" % (exceptionValue))

    sql = "INSERT INTO " + table + " (file_id, the_geom)"

    sql = sql + " VALUES (" + str(id) + ",ST_GeomFromText('LINESTRING("
#    sql = sql + " VALUES (" + str(id) + ",ST_GeomFromText('POINT("
    first = 1;

    # parse the .fnv file
    for line in f:
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
            if lon<0:                   # wrap eastern hemisphere
                lon = lon + 360
            lat=fields[8]

            # for testing
            #    print(hour, minute, second)
            #    print datetime.combine(d, t), float(lat), float(lon)
            #    print line,
            if first:
                first=0
                print "%0.6f %s" % (lon, lat)
#                sql = sql + "%f %f" % (lon, lat)
            else:
                sql = sql + ","
            # as floats
            #            sql = sql + "%f %f" % (lon, lat)
            # as strings
            sql = sql + "%s %s" % (lon, lat)
        else:
            print len(fields), fields

    sql = sql + ")',4326));"
    cursor.execute(sql);
    id = id + 1

try:
    conn.commit()
    cursor.close()
    conn.close()

except:
    print datetime.combine(d, t), float(lon), float(lat)
    
    exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
    sys.exit("Barf!\n ->%s" % (exceptionValue)) 
