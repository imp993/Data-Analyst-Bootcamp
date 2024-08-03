
-- Source of the Dataset: https://www.kaggle.com/datasets/swaptr/layoffs-2022/data?select=layoffs.csv

-- View Dataset

SELECT *
FROM world_layoffs.layoffs;

#### Step 1: Create Staging table. This is the one I will work in and clean the data because 
-- We want a keep the table with the raw data in case something happens

CREATE TABLE world_layoffs.layoffs_staging
LIKE world_layoffs.layoffs; -- should have the same structure as the existing table layoffs

-- insert all data from layoffs table to layoffs_staging

INSERT world_layoffs.layoffs_staging
SELECT *
FROM world_layoffs.layoffs;

SELECT *
FROM world_layoffs.layoffs_staging;


####### Data Cleaning #######

##### Step 2: Remove Duplicates

-- First let's check for duplicates
-- Here I'm using OVER and ROW NUMBER statements because this table does not have unique id column. 
-- So if i get row number 2 or more then those records will be duplicates

SELECT COUNT(*)
FROM world_layoffs.layoffs_staging;

-- Before remove the duplicates 3309 rows found

SELECT *
FROM world_layoffs.layoffs_staging;

SELECT *
FROM (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised,
    ROW_NUMBER() OVER(
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised) AS row_num
	FROM world_layoffs.layoffs_staging
) AS duplicates
WHERE row_num > 1;


-- I found 2 duplicates records in this dataset
-- Trying to delete duplicates rows using CTE 

WITH delete_cte AS
(
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised,
    ROW_NUMBER() OVER(
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised) AS row_num
	FROM world_layoffs.layoffs_staging
) AS duplicates
WHERE row_num > 1
)
DELETE 
FROM delete_cte;

-- Error Code: 1288. The target table delete_cte of the DELETE is not updatable

-- Trying to create another staging table and delete the duplicate rows

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` int default null,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised` int default null,
`row_num` INT
);

SELECT *
FROM world_layoffs.layoffs_staging2;

INSERT INTO world_layoffs.layoffs_staging2
SELECT *, ROW_NUMBER() OVER(
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised)
FROM world_layoffs.layoffs_staging;
    
-- Error Code: 1366. Incorrect integer value: '' for column 'total_laid_off' at row 1
-- Then I modified my query to convert empty strings to NULL before inserting them into the target table like this

INSERT INTO world_layoffs.layoffs_staging2
SELECT 
    company, 
    location, 
    industry, 
    CASE 
        WHEN total_laid_off = '' THEN NULL 
        ELSE total_laid_off 
    END AS total_laid_off, 
    percentage_laid_off, 
    `date`, 
    stage, 
    country, 
    funds_raised, 
    ROW_NUMBER() OVER(
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised
    ) AS row_num
FROM world_layoffs.layoffs_staging;

SELECT *
FROM world_layoffs.layoffs_staging2;
    
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1
;

-- delete the 2 duplicates records

DELETE
FROM world_layoffs.layoffs_staging2
WHERE row_num > 1
;

SELECT COUNT(*)
FROM world_layoffs.layoffs_staging2
;

-- now we have the 3307 records 


#### Step 3: Standardizing Data

-- TRIM the text columns

SELECT TRIM(company), TRIM(location), TRIM(industry), TRIM(stage), TRIM(country)
FROM world_layoffs.layoffs_staging2;

UPDATE world_layoffs.layoffs_staging2
SET company = TRIM(company);

UPDATE world_layoffs.layoffs_staging2
SET location = TRIM(location);

UPDATE world_layoffs.layoffs_staging2
SET industry = TRIM(industry);

UPDATE world_layoffs.layoffs_staging2
SET stage = TRIM(stage);

UPDATE world_layoffs.layoffs_staging2
SET country = TRIM(country);

SELECT *
FROM world_layoffs.layoffs_staging2;

SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry = "https://www.calcalistech.com/ctechnews/article/rysmrkfua";

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company = "eBay";

-- In industry column i found website link as industry, so I'm going to change it to retail considering other data

UPDATE world_layoffs.layoffs_staging2
SET industry = "Retail"
WHERE industry = "https://www.calcalistech.com/ctechnews/article/rysmrkfua";

SELECT DISTINCT location
FROM world_layoffs.layoffs_staging2
ORDER BY 1
; -- No amenment required

SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY 1
; -- No amenment required

SELECT DISTINCT stage
FROM world_layoffs.layoffs_staging2
ORDER BY 1
; -- No amenment required

-- noted that date column data type show as text format, so i going to change to date format

SELECT `date`, STR_TO_DATE(`date`, '%Y-%m-%d')
FROM world_layoffs.layoffs_staging2;

UPDATE world_layoffs.layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%Y-%m-%d');

ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;


#### Step 4: Remove NULL and BLANK values

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL
OR industry = ' '
;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Appsmi%'; 

-- In industry column found a blank value but cannot replace because no idea what is industry for that particular company
-- Hence i deleted that record 

DELETE 
FROM world_layoffs.layoffs_staging2
WHERE company = 'Appsmith';

SELECT *
FROM world_layoffs.layoffs_staging2;

-- noted that there are records both total_laid_off and percentage_laid_off valus were missing. Let's check

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off = ''
;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off  = ''
AND percentage_laid_off IS NULL
;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off = ''
AND percentage_laid_off = ''
;

-- 528 records were total_laid_off was null and percentage_laid_off was blank
-- since i coudn't retrive the values for both columns and without total_laid_off and percentage_laid_off data i delete the those records 

DELETE
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off = ''
;


#### Step 5: Remove unwanted column, here row_num coulumn no needed for analysis

ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num;


SELECT *
FROM world_layoffs.layoffs_staging2;

SELECT COUNT(*)
FROM world_layoffs.layoffs_staging2;

-- we have the 2778 records for exploratory analysis

##################################################


