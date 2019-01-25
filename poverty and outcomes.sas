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


options mprint symbolgen;

/*Now I need to pull in my American Community Survey data*/

proc import datafile = "\\FileSrv1\CMS_Caregiver\DATA\Caregiver Households\zta.xlsx"
dbms = xlsx out = household replace;
run;

data numeric_vars;
	set household;
		drop Geography;
		
%macro numeric(a);

	if &a = "-" then &a = "." ;

	&a.num = input(&a, 12.);

%mend;
%numeric(rental_size)
%numeric(percent_owner)
%numeric(owner_size)
		zta = put(Geography, z5.);
	run;
proc import datafile = "\\FileSrv1\CMS_Caregiver\DATA\Caregiver Households\income.xlsx"
dbms = xlsx out= income replace;

data median;
	set income;
	drop Geography;
		zta = put(Geography, z5.);
		if median_income = '-' or median_income = '(X)' then median_income = '.';
		if median_income = '250,000+' then median_income = '250000';
		if median_income = '2,500-' then median_income = '2500';
		income_median = inputn(median_income, 12.);

	run;


/*Macro variable for calling my main variables*/
%let measure = owner_housenum rental_housenum;

/*Check the descriptive stats for household size*/
proc means;
var &measure;
run;

/*Now I need to merge with the HUD file for matching ZTA to ZIP code so I can properly merge with CC*/
proc import datafile = '\\FileSrv1\CMS_Caregiver\DATA\Xwalk\part 1 crosswalk'
dbms = xlsx out = zta1 replace;
run;
proc import datafile = '\\FileSrv1\CMS_Caregiver\DATA\Xwalk\part 2 crosswalk'
dbms = xlsx out = zta2 replace;
run;

data merge_zta;
	set zta1
	zta2;
	drop ZCTA2KX;
	length zta $5.;
	zta = ZCTA2KX;

run;

/*Inserting my sorting macro into the file to sort data for merging*/
%macro sort(dataset, sorted);
proc sort data = &dataset;
by &sorted;
run;

%mend sort;
%sort(median, zta)
%sort(merge_zta, zta)
%sort(numeric_vars, zta)
/*Combine the survey data*/

data house_income;
	merge numeric_vars (in = a) median (in = b);
	by zta;
	if a;
	if b;
run;


/*Merge the two together so we can see what the breakdown*/
data house_zta;
merge house_income (in = a) merge_zta (in = b);
by zta;
if a;
if b;
run;

/*Now I add in the FIPS code so I can identify by state*/

proc import datafile = '\\FileSrv1\CMS_Caregiver\DATA\Caregiver Households\zcta_county_rel_10.txt'
dbms = dlm out = state_code replace;
delimiter = ',';
getnames = yes;
run;

data state;
	set state_code;
	keep zta state county state_name;
	zta = put(ZCTA5, z5.);

	state_name = fipstate(state);

run;

/*Merge with the household zta file*/
%sort(house_zta, zta)
%sort(state, zta)
data state_house;
	merge house_zta (in = a) state (in = b);
	by zta;
	if a;
	if b;
run;

proc sort nodupkey;
by zip;
run;


%sort(state_house, state_name)
proc univariate;
class zip;
var income_median;
by state_name;
run;


/****************************************************************************************************************
this code is for previous version of project that was focused on County Level Data. Keeping for reference for how
county can be used with the AHRF, but this is mothballed for this project.


*Checking those counties that did not match*

data county_check;
	set msbf_house;
		if merge_check = . then merge_check = 0;
run;
%sort(county_check, merge_check)

title 'Number of patients who do not match to county';
proc freq;
table merge_check;
run;

*We only have 7,000 out of 3 million + who do not match. Will kick these patients from sample. They all have
location missing for parts of the year*

*remerge with both a and b join*
data msbf_house;
	merge mbsf_fips (in = a) fips_fix (in = b);
	by fips_merge;
	if a;
	if b;

run;
*now I need bring the AHRF data*

data ahrf;
	set ahrf.AHRF_2017_2018;
	keep fips_merge percent_poverty black_percent white_percent native_percent asian_percent hispanic_percent 
	median_income;
	fips_merge = f00002;
	percent_poverty = f1332115;
	black_percent = F1464010/f0453010; *percent for all these variables using 2010 census data*
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


*de duping for faster processing*
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
