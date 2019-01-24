*****/********************************************************************************************************************************
Identifying the household size of home health patients for analysis on unmet caregiver needs. Do lower income counties have larger 
households/multigenerational homes that can help to mitigate the problem of unmet caregiver needs. Part of the analysis on unmet
caregiver needs of home health patients. This projet in conjunection with Dr. Chen at UAMS. 

Author: Robert Schuldt
Email : rfschuldt@uams.edu

**************************************************************************************************************************************;

/* Creating the libraries for accesing my libraries */
libname house '\\FileSrv1\CMS_Caregiver\DATA\Caregiver Households';
libname cms '\\FileSrv1\CMS_Caregiver\DATA\Data Exploration';
libname ahrf 'X:\Data\AHRF\2017-2018';
libname ztca '\\FileSrv1\CMS_Caregiver\DATA\Xwalk';


options mprint symbolgen;
/* Call in the Master Beneficiary 2015 file*/
data mbsf;
	set cms.mbsf_abcd_summary;
		if state_cnty_fips_cd_01 = state_cnty_fips_cd_01 then fips_check = 1;
			else fips_check = 0;
		
	run;

proc freq data = mbsf;
title 'Check on discrepencies in the Jan to Dec Fips code';
title2 'By patient';
table fips_check;
run;

data mbsf_fips;
	set mbsf;

	/*** There is no change amongst the Fips County Code from Jan to December for all patients, so we can just use this. Do not need
	to merge in the SSA to FIPS crosswalk, because we already have it***/
	fips_merge = state_cnty_fips_cd_01;
	fips_cnty = put(state_cnty_fips_cd_01, 12.);
	** Age market **;

	if AGE_AT_END_REF_YR >= 65 then age_mark = 1;
		else age_mark = 0;
	 
	**Generating female variable**;
	female = 0;
	if SEX_IDENT_CD = '2' then female = 1;
	if SEX_IDENT_CD = '0' then female = .;
	
	white = '0';
	if rti_race_cd = '1' then white = 1;
	if rti_race_cd = '0' then white = .;

	black = '0';
	if rti_race_cd = '2' then black = 1;
	if rti_race_cd = '0' then black = .;

	hispanic = 0;
	if rti_race_cd = '5' then hispanic = 1;
	if rti_race_cd = '0' then hispanic = .;

	other_race = '0';
	if rti_race_cd = '3' then other_race = 1;
	if rti_race_cd = '4' then other_race = 1;
	if rti_race_cd = '6' then other_race = 1;
	if rti_race_cd = '0' then other_race = .;

	if DUAL_ELGBL_MONS = 12 then dual = 1;
		else dual = 0;
	if 1 <= DUAL_ELGBL_MONS <=12 then part_dual = 1;
		else part_dual = 0;

	if BENE_HMO_CVRAGE_TOT_MONS = 0 then ffs= 1;
		else ffs = 0;

		run;


/*Now I need to pull in my American Community Survey data*/

proc import datafile = "\\FileSrv1\CMS_Caregiver\DATA\Caregiver Households\zta.xlsx"
dbms = xlsx out = household replace;
run;

data numeric_vars;
	set household;
		keep fips Geography owner_household rental_household percent_owner percent_rental;

			owner_household = owner;
			rental_household  = rental;

			percent_owner = owner_oc/total_units;
			percent_rental = renter_oc/total_units;


	run;
/*Macro variable for calling my main variables*/
%let measure = owner_household rental_household;

/*Check the descriptive stats for household size*/
proc means;
var &measure;
run;

/*Now I need to fix the fips codes which do not match. These fips codes just need the leading zeros added on 
which is an easy solution*/
data fips_fix;
	set numeric_vars;
		drop fips;
		length fips_merge $ 5;
		fips_merge = put(fips, z5.);

		merge_check = 1;

	run;

/*Inserting my sorting macro into the file to sort data for merging*/
%macro sort(dataset, sorted);
proc sort data = &dataset;
by &sorted;
run;

%mend sort;

%sort(fips_fix, fips_merge)
%sort(mbsf_fips, fips_merge)

/*Merge the two together so we can see what the breakdown*/

data msbf_house;
	merge mbsf_fips (in = a) fips_fix (in = b);
	by fips_merge;
	if a;

run;

/*Checking those counties that did not match*/

data county_check;
	set msbf_house;
		if merge_check = . then merge_check = 0;
run;
%sort(county_check, merge_check)

title 'Number of patients who do not match to county';
proc freq;
table merge_check;
run;

/*We only have 7,000 out of 3 million + who do not match. Will kick these patients from sample. They all have
location missing for parts of the year*/

/*remerge with both a and b join*/
data msbf_house;
	merge mbsf_fips (in = a) fips_fix (in = b);
	by fips_merge;
	if a;
	if b;

run;

/*now I need bring the AHRF data*/

data ahrf;
	set ahrf.AHRF_2017_2018;
	keep fips_merge percent_poverty black_percent white_percent native_percent asian_percent hispanic_percent 
	median_income;
	fips_merge = f00002;
	percent_poverty = f1332115;
	black_percent = F1464010/f0453010; /*percent for all these variables using 2010 census data*/
	white_percent =  F1463910/f0453010;
	native_percent = F1465610/f0453010;
	asian_percent = F1345710/f0453010;
	hispanic_percent = F0454210/f0453010;
	median_income = f1322615;

	run;
%sort(msbf_house, fips_merge)
%sort(ahrf, fips_merge)
	data ahrf_mbsf_house;
		merge msbf_house (in = a) ahrf (in = b);
		by fips_merge;
		if a;
		if b;
	run;

	%sort(ahrf_mbsf_house, fips_merge)

	proc rank data = ahrf_mbsf_house
	out = ranked_data groups = 10;
	var percent_poverty;
	ranks poverty_rank;
	run;
%sort(ranked_data, poverty_rank)

proc means;
var &measure percent_poverty black_percent median_income;
by poverty_rank;
run;

data patient_weight;
	set ranked_data;

	count = 1;
		idl = lag(fips_merge);
			retain pat_count;
				if idl = fips_merge then pat_count = pat_count + 1;
					else pat_count = 0;
	run;


/*de duping for faster processing*/
proc sort data = patient_weight;
by fips_merge descending pat_count;
run;

proc sort nodupkey;
by fips_merge;
run;


%let p = percent_poverty;

data map_image;
	set patient_weight;
	keep &measure percent_poverty black_percent median_income poverty_rank fips_merge;
run;
