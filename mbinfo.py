#!/usr/bin/env python
#
#
#

# Kelsey Jordahl
# Time-stamp: <Mon Nov  8 14:28:27 EST 2010>

import sys

# testing on specific file
file = "/Volumes/Data/multibeam/surveys/LDEO/EW9602/hs_ew9602_120_bcenb.mb24"
infofile = file + ".inf"
infofile = "kdfhgkjdshfgkj"
print infofile

# open and read the .inf file
try:
    f = open(infofile,'r')

except:
    # Get the most recent exception
    exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
    # Exit the script and print an error telling what happened.
    sys.exit("File open failed!\n ->%s" % (exceptionValue))

for line in f:
    print line,
