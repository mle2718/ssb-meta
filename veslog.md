# Warning
As of at least January 6, 2023 this table is no longer being supported by ITD. This means the table is not being updated and users should switch to GARFO VTR tables (TRIP_REPORTS_CATCH, TRIP_REPORTS_DOCUMENT, TRIP_REPORTS_IMAGES tables in the NEFSC_GARFO schema) and ultimately CAMS when available.

As of April 2024, the VTR.VLGEAR table no longer contains all possible gears and codes. No substitute or authoritative table currently exists.

# Overview
The veslog data contains everything collected through the Vessel Trip Report System.   These data are primarily generated through mandatory reporting by federally-permitted fishing vessels.

Tables: 
+ VESLOGyyyyT - contains trip level information
+ VESLOGyyyyG - contains gear information  
+ VESLOGyyyyS - contains species (Catch) information  

Location: Sole

Schema: VTR

+ 1994 to present
+ one table per year
        
        
# Current Collection Methods
These data are the result of mandatory federal vessel reporting.  Federally permitted vessels are required to submit one VTR report per "gear-mesh-area" fished.  Both commericial and for-hire (charter or party) recreational vessels are required to file VTRs.  50CFR648.7(b)(1).  

The [VTR instructions](./external/vtrinstructions.pdf)  can be useful to understand these data.

# Changes to Collections Methods
+ The VTR form has changed slightly over time.

+ Electronic VTRs start in 2011.

+ Some fisheries allow for (or require) reporting through a different system.

# Tips and Tricks.
+ A dealer-veslog link can be made reasonbly well starting in 2005.  To make this link, match the VTRSERNO in CFDBS to SERIAL_NUM in the VTR.  Chances are that you care about Trip-level outcomes: be careful, because a vessel may have more than one SERIAL_NUM per TRIPID in the VESLOG tables.
+ Vessels may declare out of fishing.  The NOT_FISHED column in VESLOG_T can be used to filter these out.   
+ The TRIPID is unique to a "fishing trip." (PRIMARY KEY)
    * A TRIPID to should match to at least one GEARID.
    * A TRIPID to should match to at least one CATCH_ID (if a vessel caught any fish)
    * The NRPAGES and NSUBTRIP Columns will be something other than "1" if there are more than one GEARID corresponding to a particular TRIPID. 
+ The GEARID is unique to the "gear-mesh-area" fished. There should be a one-to-one correspondence between GEARID and SERIAL_NUM.  
    * A GEARID to should match to exactly 1 TRIPID
    * A GEARID to could match to 0 CATCH_ID if no fish was caught.
    * A GEARID is far more likely to match to 1 or more CATCH_IDs though.
+ The CATCH_ID is unique to the "SPPCODE- DEALNUM-dealer-gearcode-mesh-area" fished.
    * A CATCH_ID to should match to at exactly 1 GEARID. And therefore exactly 1 TRIPID).
+ DATESAIL, DATELND1, DATELND2 include clock time.  You can sometimes get negative trip durations due to reporting or data entry errors.

## General Caveats.
* 1994-1995 are kind of sketchy

* Electronic VTRs have very long SERIAL_NUM, TRIPID, GEARID, and CATCH_ID.  Some software doesn't like this (Excel, stata) -- you might want to do this:

```select to_char(g.serial_num) from vtr.veslog2014g g```

To quickly pull the EVTRs, you can do this:
```
 select * from vtr.veslog2020G b where substr(b.SERIAL_NUM,9,16) is not null;
```
or
```
select * from nefsc_garfo.trip_reports_images where substr(SERIAL_NUM,9,16) is not null;
```

* The column ``SERIAL_NUM'' in the vtr data has 16 characters. It is the DOCID concatenated with 2 extra digits to identify subtrips. If you need to match to to dealer data, you must trim off the last 2 characters. As long as non-paper VTRS are 14 digits or fewer, the following is safe:

```
select substr(SERIAL_NUM,1,14) as SERIAL_NUM from nefsc_garfo.trip_reports_images ;
```


    
* Some of the older numbers (from 1994-1995) are non-numeric. 
    
* All quanties are "Hail Weights," which are the operator's best estimate of catch.
* SPPCODES will not match well to dealer's  NESPP4 codes. For example, VTR cod is all 0818 (unclassifed round). Almost all Cod will eventually be classified when sold; there is very little 0818 in dealer data.

* The following species are sketchy:

    + Surfclam and Ocean Quahog dealer reports are contained in the SFCLAM schema (separate from VESLOG).  This might create inaccuracies for trips that land SF/OQ and other species (two un-linkable logbooks).
    
    + Lobster coverage is not full.  All lobster landings by vessels holding other federal permits are supposed to be in VTR.  

    + Giant Bluefin Tuna dealer are supposed to be reported individually and should be in a different schema.  Giant Bluefin Tuna in CFDERS are either misreporting or duplication [George Silva, NMFS HMS].

* PORTLND1 and PORTLND2 are inconsistently encoded over time. 
    + The names corresponding to the port codes may or may not match to Census “units.”  The 2 digit state code does not correspond to FIPS codes.  
    + The PORTLND1 and PORTLND2 fields are data entered and error-corrected on the fly[Lee, Dentremont]  This means that data entry of ```NEW ROCHELL, NY``` is autocorrected to ```NEW ROCHELLE, NY``` and coded as ```PORT=350739```
    + This might provide more insight:```select * from vtr.vlportsyn order by doc desc;```
    + In 2019, the PORT code and PORTLND1 variable were been frequently NULL.  This appears to have been due to a problem with mapping what people write in the PORTLND1 field on a paper VTR to a port code, but has been fixed.  Here is some sample code to get a handle on trips with missing ports:

```
/* count up the number of trips with a populated PORTLND1 but a missing PORT code */ 
select count(tripid), portlnd1, state1, port from vtr.veslog2019t where portlnd1 IS NOT NULL and PORT IS NULL group by portlnd1, state1, port;
select * from port where stateabb='NY' order by portnm;
/* there are no rows with missing PORTLND1 but a populated PORT code */
select count(tripid), portlnd1, state1, port from vtr.veslog2019t where portlnd1 IS NULL and PORT IS NOT NULL group by portlnd1, state1, port;

/* count the number of trips with a missing PORT code, aggregated by tripcatg*/
select count(tripid), tripcatg, PORT_MISS from (            
select tripid, tripcatg, CASE  WHEN PORT IS NULL THEN 'Missing' ELSE 'Exists' END PORT_MISS from  vtr.veslog2022t t ) group by tripcatg, PORT_MISS order by tripcatg, PORT_MISS;

/* count the number of trips with a missing PORTLND1 code, aggregated by tripcatg*/
select count(tripid), tripcatg, PORTLND1_MISS from (            
select tripid, tripcatg, CASE  WHEN PORTLND1 IS NULL THEN 'Missing' ELSE 'Exists' END PORT_MISS from  vtr.veslog2022t t ) group by tripcatg, PORTLND1_MISS order by tripcatg, PORTLND1_MISS;


/* examine missing PORTLND1 for eVTRs*/
select count(a.TRIPID) from vtr.veslog2019T a, vtr.veslog2019G b where a.tripid = b.tripid and substr(b.SERIAL_NUM,9,16) is not null and a.portlnd1 is null;

/* examine missing PORTLND1 for paper VTRs */
select count(a.TRIPID) from vtr.veslog2020T a, vtr.veslog2020G b where a.tripid = b.tripid and substr(b.SERIAL_NUM,9,16) is null and a.portlnd1 is null;
```   

> When the (e-VTR) app was programmed the intent was to replicate the paper VTR but landed port was left off the fields to be entered. I'm not sure when/if this will be corrected and this was not well advertised as APSD did not know about this until July. The sail port was included so to the extent to which sail port and landed port may be the same would be an alternative work around. For recreational evtr's this is very likely since passengers need to get back to where their cars are parked.  --Eric Thunberg, October 1, 2020.

 There are more VTR records with missing PORTLND after 2020, but it is not a massive increase.
    
* CAREA, CNEMAREA, CLATDEG, CLATMIN, CLATSEC, CLONDEG, CLONMIN, CLONSEC, CERRNO, AREA_IND, TENMSQ
    + The "C" stands for calculated.  The calculation is pretty complicated. [Lee, H. McBride]
    + The "AREA" refers to the Northeast region statistical areas.
        + AREA in the 300s and 400s are in Canada
        + AREAS under 200 are Inshore. They shouldn't show up, but occasionally do.
    + "NEMAREA" includes "inshore areas"
        + The latitude and longitude points reported on a VTR are first binned into a Ten Minute Square, then checked against a lookup table that converts Ten Minutes Squares into statistical areas [LOC2AREAS]. 
        + If there is a match, then the reported latitude and longitude points are accepted.
        + If there is no match and the reported lat-lon is in an AREA adjacent to the reported AREA, then the lat-lon points are accepted and the reported statistical area is replaced by the AREA corresponding to the lat-lon.
        + If there is no match and the reported lat-lon is *not* in an AREA adjacent to the reported AREA, then the lat-lon points are rejected and converted to "NULL" values. The reported statistical area used as the CAREA.  
    + Technically, only degrees and minutes are required to reported.
    + Some vessels report LORAN readings. These are converted to lat-lon prior to that entire process.
    + This QAQC step is performed differently in the GARFO CATCH, IMAGES, DOCUMENT data.

* Some dealer numbers (DNUM in VESLOG_S) indicate that catch was not sold to a federally permitted dealer (DNUM<=8 or DNUM=99998)
* Recreational trips report numbers of fish, not pounds.
* PORTLND1 and PORTLND2 are recorded. Sailing port is not recorded
    + Use landing location from previous trip?
    + Assume round trip to same port?
* The code for WOLFFISH is CAT.  The code for BLUE CATFISH and NS Catfish are CATB and CATNS respectively. This is hilarious.  If you need to pick WOLF apart from CATFISH, wolffish is not caught inshore.
* The  code for WHITE HAKE is WHAK. White HAKE is not Whiting. Sometimes operators write "WHAK" for "WHITING - HAKE."  They should be writing SHAK, HAKOS, or WHB.   You can separate based on mesh size if necessary. [Thunberg]

* There are some errors on OPERNUM and OPERATOR. This happens mostly because of transcription mistakes
    + The captain writes his name, but not the number and that name is incorrectly linked to the OPERNUM.
    + The captain's writes his number, but it doesn't agree with the NAME
    This type of error seems to happen infrequently, but often enough to be a problem.

* Hullnum in the VESLOG T tables are NULL starting from 2018

# Sample Code

If you need to extract many years of VESLOG data, do it in a loop. 
*  Here is a bit of sample code in [R](https://github.com/NEFSC/READ-SSB-Lee-project-template/blob/main/R_code/data_extraction_processing/extraction/r_oracle_connection.R) and in [stata](https://github.com/NEFSC/READ-SSB-Lee-project-template/blob/main/stata_code/data_extraction_processing/extraction/extract_from_sole.do)


Here is some code that extracts data using VESLOG  

```
quietly forvalues yr=$firstyr/$lastyr{ ;
	tempfile new;
	local NEWfiles `"`NEWfiles'"`new'" "'  ;
	clear;
	odbc load, exec("select sum(s.qtykept) as qtykept, s.sppcode, s. dealnum, t.state1, t.portlnd1, t. permit, t.port, t.tripid, trunc(nvl(s.datesold, t.datelnd1)) as datesell from vtr.veslog`yr's s, vtr.veslog`yr't t 
		where t.tripid= s.tripid and (t.tripcatg=1 or t.tripcatg=4)
			and s.dealnum not in ('99998', '1', '2', '5', '7', '8')  and s.qtykept>=1 and s.qtykept is not null
			and sppcode not in ('WHAK','HAKNS','RHAK','WHAK','SHAK','HAKOS','WHB','CAT','RED')
			group by s.sppcode, t.state1, t.portlnd1, s.dealnum, t. permit, t.port, t.tripid, trunc(nvl(s.datesold, t.datelnd1));")  $oracle_cxn;                    
	gen dbyear= `yr';
	quietly save `new', emptyok;
};
	clear;
	append using `NEWfiles';
```

And here is a port of that code that extracts similar data the Catch, Images, and Document tables in the NEFSC_GARFO schema.

```
	odbc load, exec("select sum(s.kept) as qtykept, s.SPECIES_ID as sppcode, s.dealer_num as dealnum, t.state1, t.port1 as portlnd1, 
	t.VESSEL_PERMIT_NUM as permit, t.PORT1_NUMBER as port, t.docid as tripid, trunc(nvl(s.date_sold, t.date_land)) as datesell, 
	EXTRACT(YEAR from t.DATE_LAND) as dbyear
	from  NEFSC_GARFO.TRIP_REPORTS_CATCH s, NEFSC_GARFO.TRIP_REPORTS_DOCUMENT t, NEFSC_GARFO.TRIP_REPORTS_IMAGES g 
	where t.docid= g.docid and g.imgid=s.imgid and (t.tripcatg=1 or t.tripcatg=4)
			and s.dealer_num not in ('99998', '1', '2', '5', '7', '8')  and s.kept>=1 and s.kept is not null
			and s.SPECIES_ID not in ('WHAK','HAKNS','RHAK','WHAK','SHAK','HAKOS','WHB','CAT','RED')
			group by s.SPECIES_ID, t.state1, t.port1, s.dealer_num, t.VESSEL_PERMIT_NUM, t.PORT1_NUMBER, t.docid, trunc(nvl(s.date_sold, t.date_land)), EXTRACT(YEAR from t.DATE_LAND) ;")   $myNEFSC_USERS_conn; 
```

You should not expect these to match exactly, because ther is different underlying data processing.




# Sample Projects 

# Update Frequency and Completeness 
+ Nightly updates. Expect approximately 300 changes or additions to the current and previous year of data per day.
+ Data is complete 6-9 months after the end of the calendar year; however, small changes are always occurring.

# Other Metadata sources
+ INPORT.  https://inport.nmfs.noaa.gov/inport/item/1423
+ NEFSC's Data Dictionary  http://nova.nefsc.noaa.gov/datadict/

+ Preceded by: "none"
+ Succeeded by: n/a

# Related Tables very incomplete.
+ TRIP_REPORTS_CATCH, TRIP_REPORTS_IMAGES, TRIP_REPORTS_DOCUMENT - these are NEFSC views of the  "GARFO" tables  NOAA.CATCH, NOAA.IMAGES, and NOAA.DOCUMENT respectively.

The following SQL code stitches together vtr records from these views 
```
select d.*, i.*, c.*
  from NEFSC_GARFO.TRIP_REPORTS_DOCUMENT d,  -- vessel permit, sail, land, crew, trip category (commercial, P/C, rec)
       NEFSC_GARFO.TRIP_REPORTS_IMAGES i,    -- Subtrip information, gear, effort, area
       NEFSC_GARFO.TRIP_REPORTS_CATCH c      -- Species, kept, discarded, who sold to, ddate_sold
 where i.docid = d.docid
   and c.imgid = i.imgid;
```

# Support Tables very incomplete.
+ VLSPPTBL decodes SPPCODES into names and NESPP4 codes. So does VLSPPSYN_94_95, which looks deprecated.    
+ TENMINSQ, LOC,LOC2AREAS
+ PORTSYN, VLPORTSYN
+ VLGEAR - decodes gear codes into english


# Cool Stuff

##  VESLOG data is the only source for these kinds of information

|	Column	|Location | 	Description
|:---------------		|:---------- |:----------------------------------
CAREA	|		G		|
CATCH_ID	|			S	|
CLATDEG	|		G		|
CLATMIN	|		G		|
CLATSEC	|		G		|
CLONDEG	|		G		|
CLONMIN	|		G		|
CLONSEC	|		G		|
CNEMAREA	|		G		|
CREW	|	T			|
DATE_SIGNED	|	T			|
DATELND1	|	T			| This includes  time
DATELND2	|	T			| This includes  time
DATESAIL	|	T			| This includes  time
DATESOLD	|			S	| 
DEALNAME	|			S	|
DEALNUM	|			S	|
DEPTH	|		G		|
FZONE	|		G		|
GEARCODE	|		G		|
GEARID	|		G	S	|
GEARQTY	|		G		|
GEARSIZE	|		G		|
HULLNUM	|	T			|
MESH	|		G		|
NANGLERS	|	T			|
NHAUL	|		G		|
NOT_FISHED	|	T			|
NRPAGES	|	T			|
NSUBTRIP	|	T			|
OPERATOR	|	T			|
OPERNUM	|	T			|
PAGENO	|		G		|
PERMIT	|	T			|
PORT	|	T			|
PORTLND1	|	T			|
PORTLND2	|	T			|
QDSQ	|		G		|
QTYDISC	|			S	|
QTYKEPT	|			S	|
SERIAL_NUM	|		G		|
SOAKHRS	|		G		|
SOAKMIN	|		G		|
SPPCODE	|			S	|
STATE1	|	T			|
STATE2	|	T			|
SUBTRIP	|		G		|
TENMSQ	|		G		|
TIMELND1	|	T			|
TIMELND2	|	T			|
TIMESAIL	|	T			|
TRIP_ACTIVITY_TYPE	|	T			|
TRIPCATG	|	T			|
TRIPID	|	T	G	S	|

##  VESLOG data should probably be considered the primary source for these kinds of information

|	Column	|Location | 	Description
|:---------------		|:---------- |:----------------------------------
DATELND1	|	T			|
DATELND2	|	T			|
DATESAIL	|	T			|
DATESOLD	|			S	|
DEALNAME	|			S	|
DEALNUM	|			S	|
HULLNUM	|	T			|
TIMELND1	|	T			|
TIMELND2	|	T			|
TIMESAIL	|	T			|
PERMIT	|	T			|
PORT	|	T			|
PORTLND1	|	T			|
PORTLND2	|	T			|
SERIAL_NUM	|		G		|

##  VESLOG data should probably be considered a secondary source for these kinds of information. These fields might be more accurate somewhere else.
|	Column	|Location | 	Description
|:---------------		|:---------- |:----------------------------------
AREA_IND	|		G		| I have no idea what this is.

## These are the QA/QC fields 

|	Column	|	Description
|:---------------	|:--------------------------------------------------------
BATCHID	|	T		S	|
DATE_RECV	|		G		|
DC	|	T	G	S	|
DE	|	T	G	S	|
UC	|	T	G	S	|
UE	|	T	G	S	|
FILENAME	|		G		|
IMG_DATE	|		G		|
IMGTYPE	|		G		|
SIDEID	|		G		|

