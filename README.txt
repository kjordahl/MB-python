                        MB utilities in python
                        ======================

Author: Kelsey Jordahl
Date: 2011-05-12 11:42:42 EDT


Introduction 
-------------
This is an ALPHA version of MB utilities in python.  These utilities
are written in Python to interact with multibeam bathymetry data and
metadata used with [MB-System] by David W. Caress and Dale N. Chayes.
Use of this program is only recommended at the present time for those
very familiar with both MB-System and PostGIS.  Please see security
WARNING below!

Currently, the program ~mbnav2sql.py~ is the most functional.
This program is intended to add files from an MB datalist (which may
be recursive, containing other datalists) to a PostGIS database in
order to complement the capabilities of MB-System programs such as
mbdatalist.  This allows the powerful spatial queries in PostGIS to
apply to multibeam navigation.  For example, one might query to get a
list of files within 50 km of the coast of the island of Oahu.  It
also allows browsing of navigation files with a GIS program that can
interact with a PostGIS database.  I have tested with Qgis, which
works well (but can be slow for many large navigation files).

There is currently limited functionality, but testing has included
added over 25,000 files to a PostGIS database.  Please let me know if
you find this program useful, if you find bugs or have suggestions.

The PostGIS database column structure is as follows (though still
subject to change):


  file_id SERIAL PRIMARY KEY,
  filename VARCHAR(100),
  directory VARCHAR(200),
  mbformat INT,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  records INT,
  cruise_id VARCHAR(30),
  track GEOGRAPHY
  the_geom GEOMETRY

Use "~mbnav2sql.py -h~" for help and command line options.

See the ~EXAMPLES~ file for some usage possibilities.

An example of reading metadata for entire cruises from a public
repository can be seen in the program ~mbcruisemeta.py~.

Author: Kelsey Jordahl ~kjordahl@alum.mit.edu~ 
Time-stamp: <Thu May 12 11:42:42 EDT 2011>


[MB-System]: http://www.ldeo.columbia.edu/res/pi/MB-System

WARNING 
--------

Although some checking is done on input, PostgreSQL communication is
currently not done in an entirely secure manner.  DO NOT USE with a
production database or with any important data unless you are SURE
exactly what you are doing.  For more information on the potential
dangers of SQL injection attacks, please see [http://xkcd.com/327].

TODO  
------

- Improve security of SQL cursor commands
- Better error checking
- Filter egregiously bad navigation points.  More thorough
  renavigation should be done with mbnavedit or other MB-System tools,
  but database could be used to find navigation that needs to be
  edited.
- Qgis 1.5 doesn't recognize ~GEOGRAPHY~ columns.  Qgis 1.6, in my
  initial testing,sometimes fails to read ~GEOGRAPHY~ columns.
  Database structure more friendly for common tools would be good.
  Qgis 1.5 works well on the simplified ~GEOMETRY~ columns.
- Represent actual data coverage (full swath) in addition to trackline
- Tools for accessing multibeam data in the database.  Currently the
  only way to interact with the database is directly through SQL
  commands.
- Link cruiseid to metadata for each cruise, ideally with automatic retrieval of
  metadata from archives such as [MGDS], [NGDC] or [Rolling Deck to Repository]
  (an example of a start for this can be seen in ~mbcruisemeta.py~)
- and plenty more....

These programs are released under the Gnu Public License (version 3).
They are free to use, imply no warantee whatsoever, and may be
redistributed under the terms of the GPLv3.  Please see the file
COPYING or [http://www.gnu.org/licenses] for details.


[MGDS]: http://www.marine-geo.org/tools/web_services.php
[NGDC]: http://www.ngdc.noaa.gov/metadata/published/NGDC_-_MGG_-_Multibeam_Survey/list
[Rolling Deck to Repository]: http://www.rvdata.us

REQUIREMENTS 
-------------

- [python2.7] (may work on previous 2.x versions, but untested)
- [psycopg2] for calling postgreSQL from within Python
- [MB-System] (probably version 5.1 or greater)
- [PostgreSQL] installed and running
- [PostGIS] enabled
- (optional) PostGIS-enabled GIS program such as [Qgis]


[python2.7]: http://www.python.org/download/releases/2.7/
[psycopg2]: http://initd.org/psycopg
[MB-System]: http://www.ldeo.columbia.edu/res/pi/MB-System
[PostgreSQL]: http://www.postgresql.org 
[PostGIS]: http://postgis.refractions.net
[Qgis]: http://www.qgis.org 

FILES 
------
~README.org~: This information file (formatted for [org-mode] and
                  GitHub markup)
~README.txt~: ASCII version exported from ~README.org~
~COPYING~: GPL v3 license
~EXAMPLES~: Usage examples
~mb.py~: Python module containing classes for MB data structures
~mbnav2sql.py~: Load MB ~.fnv~ files from datalist and load into PostGIS database
~mbcruisemeta.py~: Print some metadata about cruises in a PostGIS database

[org-mode]: http://orgmode.org/

