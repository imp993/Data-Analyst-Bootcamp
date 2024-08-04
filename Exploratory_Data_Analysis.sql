SELECT *
FROM world_layoffs.layoffs_staging2;

-- check the date range for this dataset: 2020-03-11 to 2024-07-19

SELECT MAX(date), MIN(date)
FROM world_layoffs.layoffs_staging2;

-- check the maximum laid off and minimum laid for a day: 14000	and 3

SELECT MAX(total_laid_off), MIN(total_laid_off)
FROM world_layoffs.layoffs_staging2;

-- check the 100% laid off records with maximum laid off

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC
;

-- what are the top companies which have higher laid off: Amazon, Meta, Tesla

SELECT company, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY total_laid_off DESC;

-- what are the top industries which have higher laid off: Retail, Consumer, Transportation

SELECT industry, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY total_laid_off DESC;

-- what are the industries which have less laid off: AI, Legal, Manufacturing

SELECT industry, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY total_laid_off;

-- what are the top countries which have high (United States, India, Germany) and low laid off (Vietnam, Pakistan, Luxembourg)

SELECT *
FROM world_layoffs.layoffs_staging2;

SELECT country, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY total_laid_off DESC;

SELECT country, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY total_laid_off;

-- total laid off by year. 2023 was the year that have most laid off

SELECT YEAR(`date`) AS year_data, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY year_data
ORDER BY 1;

-- total laid off by stage wise: POST IPO has the highest laid off

SELECT stage, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- rolling sum of laid off by month with year

SELECT SUBSTRING(date, 1, 7) AS date_2, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY date_2
ORDER BY 1;

-- output as 2020-03 - 8981, 2020-04 - 25271 ..

WITH rolling_sum AS
(
SELECT SUBSTRING(date, 1, 7) AS date_2, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY date_2
ORDER BY 1

)
SELECT date_2, total_laid_off, SUM(total_laid_off) OVER(ORDER BY date_2) AS rolling_total
FROM rolling_sum;

-- 2020-03	8981	8981
-- 2020-04	25271	34252
-- 2020-05	22699	56951

-- output the data by company, year and laid off with raning by descending order

SELECT company, YEAR(`date`) AS year_data, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY company, year_data
ORDER BY 3 DESC
;

-- top 3 companies which had the highest laid off for each year

WITH ranking_laid_off (company, year_data, total_laid_off) AS 
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company, YEAR(`date`)
), company_year_rank AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY year_data ORDER BY total_laid_off DESC) AS ranking
FROM ranking_laid_off
WHERE total_laid_off IS NOT NULL
)
SELECT *
FROM company_year_rank
WHERE ranking <= 3
;

/* Output
Uber		2020	7525	1
Groupon		2020	2800	2
Swiggy		2020	2250	3
Bytedance	2021	3600	1
Katerra		2021	2434	2
Zillow		2021	2000	3
...
*/

################################################################