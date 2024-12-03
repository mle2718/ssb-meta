# Code to pull VMS data and retain records corresponding to the permits in permit_universe


library("here")
library("tibble")
library("dplyr")
library("ROracle")
library("readr")

here::i_am("KA_Scallop/R_code/import_VMS.R")

import_vintage<-"2023_11_30"

vintage_string<-Sys.Date()
vintage_string<-gsub("-","_",vintage_string)

oracle_server="nefsc_users"
oracle_username<-"mlee"
oracle_password<-novapw

# read in population of permit numbers.

permit_universe<-read_csv(here("KA_Scallop","data","data_intermediate", paste0("permit_population_",import_vintage,".csv",sep="") ), col_types=c("c") ) 
permit_universe <-permit_universe %>% rename(PERMIT=permit)

# 
# 
# con <- ROracle::dbConnect(
#   drv = ROracle::Oracle(),
#   username = oracle_username,
#   password = oracle_password,
#   dbname = oracle_server)

con <- ROracle::dbConnect(
  drv = ROracle::Oracle(),
  username = oracle_username,
  password = solepw,
  dbname = "nefsc_users")


START.YEAR = 2008
END.YEAR = 2022
#END.YEAR = 2009

RESULT.COMPILED<-list()
t<-1
for(i in START.YEAR:END.YEAR) {
  print(i)
  CURRENT.QUERY = paste("select VESSEL_PERMIT as permit, LAT_GIS, LON_GIS, to_char(POS_SENT_DATE,'YYYY MON DD HH24:MI:SS') as POS_SENT_DATE, PREV_LAT_GIS, PREV_LON_GIS, AVG_COURSE, AVG_SPEED,to_char(PREV_POS_SENT_DATE,'YYYY MON DD HH24:MI:SS') as PREV_POS_SENT_DATE from VMS.VMS",i,
		" WHERE LAT_GIS BETWEEN 30 and 50 AND LON_GIS BETWEEN -80 and -60", sep="")
  RESULT.COMPILED[[t]]<- tbl(con,sql(CURRENT.QUERY)) %>% 
    collect() %>%
    dplyr::inner_join(permit_universe, by=join_by(PERMIT)) %>% 
    group_by(PERMIT) %>%
    arrange(POS_SENT_DATE, .by_group=TRUE)
  
  t<-t+1
}    

VMS_NAME <-paste0("VMS1_",vintage_string,".Rds")
saveRDS(RESULT.COMPILED, file=here("KA_scallop","data","data_intermediate", VMS_NAME))



CURRENT.QUERY = "select EXTRACT(YEAR FROM POS_SENT_DATE) as YEAR, VESSEL_PERMIT as permit, LAT_GIS, LON_GIS, to_char(POS_SENT_DATE,'YYYY MON DD HH24:MI:SS') as POS_SENT_DATE, PREV_LAT_GIS, PREV_LON_GIS,AVERAGE_COURSE AS AVG_COURSE, AVERAGE_SPEED AS AVG_SPEED,to_char(PREV_POS_SENT_DATE,'YYYY MON DD HH24:MI:SS') as PREV_POS_SENT_DATE from VMS.OLD_VMS_1997_TO_MAY_2008 WHERE EXTRACT(YEAR FROM POS_SENT_DATE) BETWEEN 2001 and 2007 AND LAT_GIS BETWEEN 30 and 50 AND LON_GIS BETWEEN -80 and -60"

OLD_VMS<-tbl(con,sql(CURRENT.QUERY)) %>%
  collect() %>%
  dplyr::inner_join(permit_universe, by=join_by(PERMIT)) %>%
  group_by(PERMIT) %>%
  arrange(POS_SENT_DATE, .by_group=TRUE)

dbDisconnect(con)

VMS_NAME2 <-paste0("VMSold_",vintage_string,".Rds")

saveRDS(OLD_VMS, file=here("KA_scallop","data","data_intermediate", VMS_NAME2))

