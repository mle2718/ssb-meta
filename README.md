# Social Sciences Branch Metadata
This repository describes data in the NEFSC databases with a special eye towards fields that are used by social scientists.  It is intended as a supplement to the [data dictionary system](https://nova.nefsc.noaa.gov/datadict/). This repository is mostly oracle focused, but there is information on other data sources too.  Sometimes it contains sample code to extract data.

Please help make this a valuable up-to-date resource. This repository is public, but writing to the "master" branch is protected. To contribute, you should either:

1. Create a branch, make your changes, and issue a pull request. Make sure to request a review from someone in SSB that is on the "approving list."
2. Fork this repository to create your own version, make changes, and issue a pull request.

See the instructions in the [How to help](https://github.com/NEFSC/READ-SSB-Lee-WorkingEfficiently) section for more info.


# Basics and General Thoughts

1. Most data are stored in **Oracle databases**, which are located on the "NEFSC_USERS" **servers**.  "Nova" was shut down in 2022 and replaced with NEFSC_USERS on NEFSCDB1. Sole is planned to be shut down in January, 2024.

1. There are various **schema**. Schema are collections of related tables.

1. Quality Assurance and Quality Control (QA/QC) is often needed as the data are imperfect. 

1.  Many data tables are live, with nightly or continuous updates. For example, when working with the  dealer data, expect approximately 300 changes or additions to the current and previous year of data per day. Data is “complete” 6-9 months after the end of the calendar year; however, small changes are always occurring.
This has consequences for reproducibility if you do not store a copy of the data.

1.  Make sure the table that you're using is not "stale."  Some of the data is stored at GARFO and periodically copied from GARFO to NEFSC servers. Sometimes, they stop getting copied. Sometimes they are updated monthly. One way to check this is to get the maximum DE, DC, or some other date field.

1. Exploring the databases using SQLDeveloper is a good way to build some intuition
    +  In the Connections tab, connect to Sole
    +  Expand the "Other Users" tab.
    +  Expand a schema, like "CFDBS" and explore  "Tables," "Views," and "Materialized Views" corresponding to that schema.  If they are empty, it may mean that you do not have permissions to view any of those tables.
    

1.  There are tables and there are views.  Sometimes, the sql that generates a view can help you figure out why you're getting an unexplainable result of a query. For example, the following bit of code shows that SECTOR_PARTIPANTS_CPH is based, in part on permit.vps_owner, permit.vps_vessel, and mqrs.mort_elig_criteria.
![sql picture](/figures/sql.png)

1.  If you want to use ODBC with R or Stata to read data straight into your software, take a look [here](https://github.com/NEFSC/READ-SSB-Lee-project-template)

1.  It's usually good practice to include the schema when you query data. That is, write:
```
select * from nefsc_garfo.permit_vps_owner
```
instead of 
```
select * from permit_vps_owner
```

The second may work or it may fail. If there are multiple tables with the same name (in different schema),  it may fail invisibly.

However, an exception is the CAMS data, which uses transportable table spaces.  For CAMS schema, and any other tables that use TTS, you will want to use the public synonym for speed

```
select * from CAMS_LAND
```

will be much faster than 

```
select * from CAMS_GARFO.CAMS_LAND
```



1.  You may find it useful to extract the comments for the columns. Here is some sample code to do that for the CAMS_LAND table in CAMS_GARFO.

```  
select table_name, column_name, comments from all_col_comments where owner='CAMS_GARFO' and table_name='CAMS_LAND' order by column_name;
```

1.  ITD maintains an Inventory of databases [here](https://docs.google.com/spreadsheets/d/15FtGnNUgct7mTsRpPP9xX4BLkceY7SfMnZvdA_kjlxY/edit#gid=1754518543&fvid=668259322)



# The Goods (alphabetically)

[AMS and DAS ](AMS_DAS.md) : Allocation Management System (AMS); Days-at-Sea Management System

[CAMS](CAMS.md): Catch Accounting and Monitoring System . The next generation DMIS and _AA

[Client](Client.md) :The Client schema


[Cost Survey](Cost_survey.md) : Cost survey Data

[Dealer](dealer.md) Dealer or Commercial Fisheries Dealer Database (CFDBS)

[DMIS](DMIS.md): Data Matching and Imputation System

[MQRS](MQRS.md) : Moratorium Qualification something System

[observer](observer.md): Fishery Observers

[permit](permit.md): Vessel permitting

[sector](sector.md) : Sector Databases

[svdbs](svdbs.md) : Survey Databases

[Trip Costs](Trip_Costs.md) : Dataset containing predicted trip costs

[Veslog](veslog.md) : Vessel Trip Reports (VTRs)

[VMS](VMS.md) : Vessel monitoring system  (VMS)

[SSB survey and Data Efforts](SSB%20Survey%20and%20Data%20Efforts%20Tracking.md): SSB data collections that are not in oracle.


# NOAA Requirements
“This repository is a scientific product and is not official communication of the National Oceanic and Atmospheric Administration, or the United States Department of Commerce. All NOAA GitHub project code is provided on an ‘as is’ basis and the user assumes responsibility for its use. Any claims against the Department of Commerce or Department of Commerce bureaus stemming from the use of this GitHub project will be governed by all applicable Federal law. Any reference to specific commercial products, processes, or services by service mark, trademark, manufacturer, or otherwise, does not constitute or imply their endorsement, recommendation or favoring by the Department of Commerce. The Department of Commerce seal and logo, or the seal and logo of a DOC bureau, shall not be used in any manner to imply endorsement of any commercial product or activity by DOC or the United States Government.”


1. who worked on this project:  Min-Yang
1. when this project was created: Jan, 2021 
1. what the project does: Describes data in the oracle databases with a special eye towards fields that are used by social scientists
1. why the project is useful:  Describes data in the oracle databases with a special eye towards fields that are used by social scientists
1. how users can get started with the project: Just read the readme
1. where users can get help with your project:  Open an issue
1. who maintains and contributes to the project. Min-Yang

# License file
See here for the [license file](https://github.com/minyanglee/READ-SSB-Lee-metadata/blob/main/License.txt)
