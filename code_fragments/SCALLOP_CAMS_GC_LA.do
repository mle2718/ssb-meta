***This file parses the ACTIVITY_CODE_1 field from CAMS_SUBTRIP and VTR_ORPHANS_SUBTRIP to classify all CAMS trips (including orphans) as Limited Access or General Category.
**** It uses the ACCESSAREA field from the same tables to classify trips as Access Area or not.

***see https://github.com/NEFSC/READ-SSB-metadata/blob/master/CAMS.md for more on CAMS 

 clear all

jdbc connect , jar("$jar")  driverclass("$classname")  url("$nefscdb1_url")  user("$myuid") password("$nefscdb1pwd")
 
jdbc load, exec ("select a.CAMSID, d.ACTIVITY_CODE_1, d.ACCESSAREA from CAMS_GARFO.CAMS_LAND a full outer join CAMS_GARFO.CAMS_subtrip d on a.CAMSID=d.CAMSID where a.ITIS_GROUP1 like '%SCALLOP%' and a.YEAR between $startyear and $endyear ") 
 
save "${my_datadir}/intermediate/CAMS_SCALLOP_TRIPS.dta", replace 
 
 
 ****load the orphans 
  clear all

jdbc connect , jar("$jar")  driverclass("$classname")  url("$nefscdb1_url")  user("$myuid") password("$nefscdb1pwd")
 
jdbc load, exec ("select CAMSID, ACTIVITY_CODE_1, ACCESSAREA from CAMS_GARFO.CAMS_VTR_ORPHANS_SUBTRIP where MAIN_SPP_GRP like '%SCA%' and YEAR between $startyear and $endyear ") 
 
 append using "${my_datadir}/intermediate/CAMS_SCALLOP_TRIPS.dta", replace 
 

 split ACTIVITY_CODE_1, p(-)
 
 gen activity_code = ACTIVITY_CODE_11 +"-"+ ACTIVITY_CODE_12
 
 **label all Limited access trips 
 gen LA= 1 if activity_code =="SES-SAA" |activity_code =="SES-SCA" 
 replace LA = 0 if missing(LA)
 
**label all general category trips 
 gen GC= 1 if activity_code =="SES-SCG" 
  replace GC = 0 if missing(GC)
  *** Northern Gulf of Maine trips by the Scallop General Category fleet will have the ACTIVITY_CODE_1 that starts with "SES-SCG-NG"
 *** rename access area
 
 rename ACCESSAREA access_area
 replace access_area = "1" if access_area=="AA"
  replace access_area = "0" if access_area!="1"
  destring access_area, replace 
  
keep CAMSID access_area LA GC  
 
**this is at the sub-trip level, so will have to make a call for whole trips. 
***decision rule, if sub-trip is AA do whole trip as AA
***decision rule if LA make whole trip LA

***AA first
sort CAMSID
by CAMSID: egen max_aa = max(access_area)

sort CAMSID
by CAMSID: egen max_LA = max(LA)

**drop duplicate trip_ids

sort CAMSID
quietly by CAMSID: gen dup = cond(_N==1, 0,_n)
drop if dup > 1

keep CAMSID max_LA max_aa GC

rename max_L LA  
 rename max_aa access_area
 replace GC= 0 if LA==1

 
 save "${my_datadir}/intermediate\LA_AA_SCALLOP_TRIPS.dta", replace 
