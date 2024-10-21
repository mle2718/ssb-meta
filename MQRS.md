# Overview
Tables: MQRS_MORT_ELIG_CRITERIA
MQRS_ALLOCATION_TRANSFER -- Permanent transfer of share
Location: NEFSCDB1

Schema: NEFSC_GARFO
The MQRS system is fun. It contains information on "eligibilities."  The sector rosters are also stored here.  It is used in conjunction with other tables, but not cross-referenced against them for logical consistency.

MQRS_ALLOCATION_TRANSFER contains permanent transfer of shares, in percentages, and the price.  This table is maintained in double-book accounting, so there is 1 row for the buyer and one row for the seller.

The SECTOR schema still exists on SOLE.
        
# Current Collection Methods
Someone at GARFO puts data into the MQRS databases.


# GARFO Documentation

1.  [MQRS Presentation](/external/MQRS%20talk.ppt).  PPT circulated by Tammy Murphy on Feb 21, 2017.  Probably by Jay Hermsen, last edited June 10, 2014.


# Changes to Collections Methods

# Tips and Tricks

## General Caveats


  *  At GARFO, MQRS.MORT_ELIG_CRITERIA is not archived and is a current production table. I strongly suggest using a view to interpret it, because there are some pitfalls in the table structure that aren't obvious.(torey.adler on 15/Dec/23 11:45 AM)

  * There is an APP_NUM. This is different from the AP_NUM in the PERMIT system.

  * There are business names. They do not necessarily match the business names from the permit system. 

  * History Retention and CPH are essentially synonyms. 
  
  * When an owner puts a right into CPH, the last known PER_NUM associated with that RIGHT_ID is stored.  

> we don't change the relationship between the CPH and the permit if a new owner or the same owner places a new set of permits on the vessel.  The CPH records remain as is until they are moved to another vessel.  We place the permits in CPH based on the last known or permitted vessel. [Ted Hawes @ GARFO, April 18, 2018]  

This can create PER_NUMs with more than 1 RIGHT_ID. Ted Hawes suggests adding
```
auth_type not in ('CPH','HISTORY RETENTION')
```
and then 
```
auth_type  in ('CPH','HISTORY RETENTION')
```
And stitching results together with a union.


  * Sometimes an entry gets put into the MQRS database where the DATE_ELIGIBLE is equal to the DATE_CANCELLED. This may cause mis-merges.  You also may want to exclude these completely from your query.
  
  ```
select * from NEFSC_GARFO.mqrs_mort_elig_criteria
  where date_eligible<>date_cancelled or date_cancelled is null;
```

  * There was a "cleanup" in 2009.  Perhaps in March?  [Ted Hawes]

  * Auth_id is really only used internally.  auth_id is a unique field for each transaction, and from the original creators of this system back in the early 1990s, the auth_id authorizes the issuance of a permit to the vessel or it authorizes allocation to be issued to the vessel.  The idea was that the auth_id would be used to connect the permit system with mort_elig_critieria when the permit staff processes an application for a limited access permit.  MQRS will authorize the issuance of a limited access using the auth_id. [Ted Hawes]
  
  * The auth_id and right_id can be the same when a new limited access permit is created. Alternatively, when auth_id=right_id, the right has never been transferred.[Ted Hawes]
  
  
  * There are at least a handful of cases in which a different system (DAS, DAS2 mostly) links to an incorrect right id. For example: 
  ```
select * from DAS.DAS_ALLOCATION where right_to_days_id not in (select distinct right_id from NEFSC_GARFO.mqrs_mort_elig_criteria where fishery='MULTISPECIES') and das_category='A';  
```
will extract all the entries in the DAS.DAS_ALLOCATION table that have an MRI that is not in the NEFSC_GARFO.mqrs_mort_elig_criteria.  This shouldn't be possible, because an entity needs an MRI 

similarly:
```
select distinct right_id  from das2.allocation where plan='MUL' and 
right_id not in (select distinct right_id from NEFSC_GARFO.mqrs_mort_elig_criteria where fishery='MULTISPECIES')
```

Here is a partial list of the MRI's affected by the cleanup. This came from Ted Hawes. But you could look at the "remark" field and see if there is a reference to a cleanup.   

```
select PER_NUM AS PERMIT,
		AUTH_ID AS MRI,
		FISHERY,
		DATE_ELIGIBLE,
		DATE_CANCELLED
		from NEFSC_GARFO.mqrs_mort_elig_criteria where auth_id in(1179,1183,1187,1196,1219,1255,1261,1293,1296,1362,1374,2423,1174, 1184, 1176, 1209,1219,1298,1358,1372,2423);  
```
The date_eligible and date_cancelled fields were broken during the cleanup. I'm not sure the best way to deal with it.

## Confirmation of Permit History
A vessel owner can put a vessel's history into the Confirmation of Permit History program. This allows a vessel owner to get the benefits of a vessels history when a vessel does not exist. Perhaps a vessels was scrapped, sunk, or sold without it's history. For some fisheries (groundfish), this means that the owner can still recieve an annual allocation.  The owner databases are linked to vessels in the CPH program. 

A vessel's history must be moved out of the CPH program to a vessel owned by the person that put the history into the CPH program before it is sold.  GARFO only will allow transfers of CPH when the sole permit holder is deceased and an estate or heir is taking possesion. An owner with a vessel in CPH is not required to fill out annual paperwork (like a permit renewal form). However, because the "transfer before sale" requirement, we know who owns the history even without a data collection effort like this.

# Sample Code

Here's some code to link right_ids to permits for just the multispecies fishery, courtesy of Dan Caless.:
```
SELECT PER_NUM AS PERMIT,
		RIGHT_ID AS MRI,
		FISHERY,
		DATE_ELIGIBLE,
		DATE_CANCELLED,
		AUTH_TYPE,
		ELIG_STATUS
	  FROM NEFSC_GARFO.mqrs_mort_elig_criteria 
	  WHERE FISHERY = 'MULTISPECIES'
		AND not ((TRUNC(DATE_ELIGIBLE) =  TRUNC(NVL(DATE_CANCELLED,SYSDATE+20000))) AND (CANCEL_REASON_CODE = 7 AND AUTH_TYPE = 'BASELINE'))
		AND DATE_ELIGIBLE IS NOT NULL
		AND (TRUNC(DATE_CANCELLED) >= '01-MAY-03' or DATE_CANCELLED IS NULL):
```

Here is some code to link the Permanent Share transfers to the yearly allocations (pounds). To filter out internal transfers or bartering arrangements with no compensation, include price>0. This code was confirmed to be correct by Ben Galuardi (October 21, 2024).
```
select * from NEFSC_GARFO.AMS_ALLOCATION_TX a, NEFSC_GARFO.MQRS_ALLOCATION_TRANSFER b
where a.CHARGE_MRI=b.ALLOCATION_NUMBER
and a.EFFECTIVE_DATE=b.TRANSFER_DATE
and a.ROOT_MRI=b.TO_FROM
and a.FMP='SCAG'
and b.price>0
order by transfer_number, allocation_number;
```

  
# Sample Projects

# Update Frequency and Completeness


# Other Metadata sources
+ INPORT.  https://inport.nmfs.noaa.gov/inport/item/12987
+ Missing from NEFSC's Data Dictionary  http://nova.nefsc.noaa.gov/datadict/


# Related Tables
+ BUS_OWN contains ownership data that is often linked to these PERMIT data. We'll put them in a separate section.
+ CPH
+ OPERATORS (captains) are permitted independently 

## Support Tables
  + VALID_FISHERY translates the PLAN and CAT into words.

