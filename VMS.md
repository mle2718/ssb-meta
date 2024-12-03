
# Overview

VMS data is maintained by [NMFS OLE](http://www.nmfs.noaa.gov/ole/index.html).  It contains a vessel id, a timestamp, and location.

As of the end of 2018, the contact at NMFS OLE was Kelly Spalding .

Tables: Not sure

Location: Not sure

Schema: Not sure. It had been stored on VMS on NEFSCB1, but was removed by December 2024.  

# Current Collection Methods
# Changes to collection methods


# Tips and Tricks

## General Caveats
VMS data from 1997-May 2008 is stored in 1 table.

VMS data from 2008 on is stored in annual tables. 

# Sample Code

Here is a SQL query to pull out data from the 2008 table that corresponds roughly to the Northeast region.
```
select VESSEL_PERMIT as permit, LAT_GIS, LON_GIS, to_char(POS_SENT_DATE,'YYYY MON DD HH24:MI:SS') as POS_SENT_DATE, PREV_LAT_GIS, PREV_LON_GIS, AVG_COURSE, AVG_SPEED,to_char(PREV_POS_SENT_DATE,'YYYY MON DD HH24:MI:SS') as PREV_POS_SENT_DATE from VMS.VMS2008
WHERE LAT_GIS BETWEEN 30 and 50 AND LON_GIS BETWEEN -80 and -60"
```

And here is a SQL query to extract the older data (1996-2007).

```
"select EXTRACT(YEAR FROM POS_SENT_DATE) as YEAR, VESSEL_PERMIT as permit, LAT_GIS, LON_GIS, to_char(POS_SENT_DATE,'YYYY MON DD HH24:MI:SS') as POS_SENT_DATE, PREV_LAT_GIS, PREV_LON_GIS,AVERAGE_COURSE AS AVG_COURSE, AVERAGE_SPEED AS AVG_SPEED,to_char(PREV_POS_SENT_DATE,'YYYY MON DD HH24:MI:SS') as PREV_POS_SENT_DATE from VMS.OLD_VMS_1997_TO_MAY_2008 WHERE EXTRACT(YEAR FROM POS_SENT_DATE) BETWEEN 1996 and 2007 AND LAT_GIS BETWEEN 30 and 50 AND LON_GIS BETWEEN -80 and -60"
```

Some [sample code](code_fragments/import_VMS.R) to extract the old data, loop over the annual data and save only a subset of permits. 



# Update Frequency and Completeness 

# Other Metadata sources

+ Preceded by: 
+ Succeeded by:

# Related Tables 

# Support Tables 



