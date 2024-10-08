# Customer Master ETL 

## Table of contents
1. [Introduction](#introduction)
2. [How To Run](#paragraph1)
3. [How it Works](#introduction)
4. [Issues Outstanding](#paragraph2)
    1. [Sub paragraph](#subparagraph2)

## Project Introduction <a name="introduction"></a>
This is an ETL that will grab customer infomation, including, customer number, customer name, customer emails, and possibly other fields if needed.  This is a living project.

The way this project is intended to work was as a customer contact (emails) ETL; however, this provides more than just that.  since there isn't a clearly defined database for customer information and this project to create that source of truth.  This way, wether or not you need emails or to tie out our customer data with CAT, or use these data points for reporting.  This should suffice all those needs.  The idea is to create a single source of truth, such that, everyone is running off the same information and we stay in sync.
## How To Run <a name="paragraph1"></a>
Ideally this project is able to kick off an ETL process that will either take in a table, and whole data source or all defined data sources. 
1) This project uses python poetry (which is a project management & library dependency tool)
    - In the toml file on the root level of the poject to shows all scripts from poetry.
    - This includes a command called "grab-data".  to call it in cmd use "poetry run grab-data (with any datasource or data source with a table).
    - It would look something like this, "poetry run grab-data Cloudlink prospects".
    -
2) At which point, it will run the scirpt from main and get the data from that source and insert it into the db.
3) This lands in a bronze or raw table, there are stored procedures that can be called to trickle in new data to silver and gold tables.  They use delta loads to grab only new information.  Doesn't matter if you reload a datasource again, it should only gran new data.  It is also time stamped.

## How it Works <a name="paragraph1"></a>
Most of the code is straightforward and easy to follow.  There are folders that house the data sources, both for excels and sql databses (currently).  There is a modules folder that has the bulk of the code in it.  each is handling a specifc type of action.  Usually containing helper functions with along with the doc strings and comments.  Again, everything is really straghtforward for the most part.  

There is one execption, which is the fuzzy matching logic.  The file is called "FuzzyMatching.py".  Its piggy backing on a library, which is free and open source, called "rapidfuzz".  rapidfuzz is a python wrapper for C++ code which effectively takes in a list of customer names and compares it to the one in question. It looks at the entire dataset, which can be computationally expensive, keep in mind the bulk of the fuzzy matching is whent his project first kicks off.  After that first run we want to get this portion of the code running is .  It is also using a library called "joblib" which just allows for various cores to run the code in parallel, resulting in faster times for fuzzy matching.  Which again would be optimal for more powerful computers, but in any rate its an performance gain.  

## Issues Outstanding<a name="paragraph2"></a>
Now while the code runs, I think something to consider is updating information in a more efficient manner.  Like this would be applicable for both tables and excel spreadsheets or other data sources yet defined, the apporach should be after the inital lanuch of this to only run tables from a date from last ran date.  Not sure how doable this is as in each seperate data source you'd have to define a date last ran and a column to pick.  Excels would be most likely candidates, because we can store the date last ran and 
