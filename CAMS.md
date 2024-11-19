# Overview
CAMS is slated to replace both the CFDBS_AA tables and the DMIS tables.

Location: NEFSC_USERS

Schema: CAMS_GARFO

Metadata can be found here:

https://www.greateratlantic.fisheries.noaa.gov/ro/fso/reports/cams/index.html

and here

http://nerswind/cams/cams_documentation/index.html

To get access, ask for it in the [CAMS Jira board](https://apps-st.fisheries.noaa.gov/jira/projects/CAMSNR/issues/CAMSNR-764?filter=allopenissues)
# Current Collection Methods

# Changes to Collections Methods

# Tips and Tricks.

See the [README](https://github.com/NEFSC/READ-SSB-Lee-metadata/) for a note about CAMS and Transportable Table Spaces (TTS).

# General Caveats.

* CAMS currently fills in VALUE=NULL with a zero. This might be a problem if you are calculating average prices by summing the landings and value for all rows.  
  
```
select * from cams_land where value=0 and lndlb>0 and dlr_disp<>101 and itis_tsn='167687' and  status not in ('VTR_NOT_SOLD', 'VTR_DISCARD', 'DLR_ORPHAN_SPECIES') 
order by lndlb desc;
 
select * from nefsc_garfo.cfders_all_years where DEALER_RPT_ID in ('99187778','91840653','97278482','99187792','90695887');
```
You may want to do this instead:

```
select sum(value) as value, sum(lndlb) as landings, sum(value)/sum(lndlb) as price from cams_land where value>0 and lndlb>0 and dlr_disp<>101 and itis_tsn='167687' and  status not in ('VTR_NOT_SOLD', 'VTR_DISCARD', 'DLR_ORPHAN_SPECIES')
group by itis_tsn;

```


# Sample Code

Some sample code can be found here: 

https://www.greateratlantic.fisheries.noaa.gov/ro/fso/reports/cams/articles/common_query_examples.html


+ select * from CAMS_GARFO.CAMS_LANDINGS; 


Here is stata code that uses ODBC get landings for cod and haddock from particular stat areas from VTR.
```
global commercial_grab_start 2019
global commercial_grab_end 2021

forvalues yr=$commercial_grab_start(1)$commercial_grab_end {;
/* and here is the odbc load command */
	clear;
	tempfile new;
	local files `"`files'"`new'" "';
	odbc load,  exec("select g.carea, s.gearid, s.tripid, s.sppcode, s.qtykept, s.datesold from vtr.veslog`yr's s, vtr.veslog`yr'g g where g.gearid=s.gearid and s.sppcode in ('COD', 'HADD') and g.carea between 511 and 515;
") $myNEFSC_USERS_conn ;
	gen year=`yr';
	quietly save `new';
};
```

### From CAMS.

```
	odbc load,  exec("select sum(nvl(lndlb,0)) as landings,  sum(livlb) as livelnd, year, month, itis_tsn from cams_land cl where 
		cl.area between 511 and 515 and 
		cl.year between $commercial_grab_start and $commercial_grab_end and
		itis_tsn in ('164712','164744')
		group by year, month, itis_tsn;") $myNEFSC_USERS_conn ;	
```

And discards
```
odbc load,  exec("select year, extract(month from date_trip) as month, itis_tsn, sum(nvl(cams_discard,0)) as discard from cams_discard_all_years cl where 
		cl.area between 511 and 515 and 
		year between $commercial_grab_start and $commercial_grab_end and
		itis_tsn in (164712,164744)
		group by year, extract(month from date_trip), itis_tsn;") $myNEFSC_USERS_conn ;		
		
```


### From VTR
```
forvalues yr=$commercial_grab_start(1)$commercial_grab_end {;
/* and here is the odbc load command */
	clear;
	tempfile new;
	local files `"`files'"`new'" "';
	odbc load,  exec("select g.carea, s.gearid, s.tripid, s.sppcode, s.qtykept, s.datesold from vtr.veslog`yr's s, vtr.veslog`yr'g g where g.gearid=s.gearid and s.sppcode in ('COD', 'HADD') and g.carea between 511 and 515;
") $oracle_cxn ;
	gen year=`yr';
	quietly save `new';
};
```


## Summer Flounder example

### All permits that landed summer flounder in 2014
```
SELECT distinct PERMIT
  from cams_land where 
  ITIS_TSN=172735 and YEAR in ('2014')
```

### Subtrip level info for permits that landed summer flounder in 2014

```
select * FROM cams_subtrip s 
    where s.YEAR in ('2014') and s.PERMIT in (SELECT distinct PERMIT
    from cams_garfo.cams_land where 
    ITIS_TSN=172735 and YEAR in ('2014'));
```



### Catch level info for those trips

This query adds landings level information for those subtrips, retains just some subtrip-level columns, and does some ordering.

```
SELECT t.CAMSID, t.DOCID, t.VTRSERNO, t.PERMIT, t.ITIS_TSN, t.DLRID, t.DLR_DATE, t.STATE, t.PORT, t.DLR_MKT, t.DLR_GRADE, t.LNDLB, t.VALUE, t.NEGEAR, t.WEEK, s.VTR_CREW, s.RECORD_SAIL, s.RECORD_LAND, s.VTR_TRIPCATG, s.subtrip, s.YEAR 
  FROM cams_land t
LEFT OUTER JOIN 
    (select CAMSID, VTR_CREW, RECORD_SAIL, RECORD_LAND,
    VTR_TRIPCATG, SUBTRIP, YEAR, permit FROM cams_subtrip) s 
    on t.SUBTRIP=s.SUBTRIP AND
    t.CAMSID=s.CAMSID
    where t.YEAR in ('2014') and 
      t.PERMIT in (SELECT distinct PERMIT
           from cams_land 
              where ITIS_TSN=172735 and YEAR in ('2014'))
    order by t.permit, t.camsid, itis_tsn, dlr_mkt, dlr_grade;
```

You may want to filter out the PERMIT=000000, add discards, or add VTR orphans depending the your project.

### State Landings and Revenue

Note that the above query (Catch level info for those trips) will only capture activity that occurs by vessels with a federal permit at the time the trip was taken.  Vessel owners may not always renew all of their federal permits at the begininng of the year.   If interested in total catches at the vessel-level, hullid should be used rather than permit. Trips by vessels without a federal permit will have ``PERMIT=000000``.  Many Council managed species have minor state landings, however there are exceptions (e.g. summer flounder, scup, black sea bass).

The variable ``PERMIT_STATE_FED`` in the CAMS_LAND table indicates whether landings are associated with state (PERMIT=000000) or federal fishing activity. Unknown vessels (``PERMIT=190998, 390998, 490998``) are classified as Federal  in the construction of ``PERMIT_STATE_FED''

### Classifying Limited Access, General Category and Access Area trips 

[Stata code to extract and classify scallop trips](/code_fragments/SCALLOP_CAMS_GC_LA.do)




# Update Frequency and Completeness 

# Other Metadata sources

+ Preceded by: DMIS, CFDERS _AA
+ Succeeded by: n/a

# Related Tables 

# Support Tables 

