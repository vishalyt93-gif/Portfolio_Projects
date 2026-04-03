
SELECT * FROM layoffs_staging2;

-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways


-- Check if there are any duplicates
SELECT * FROM
			(SELECT *, ROW_NUMBER() 
			OVER(PARTITION BY company, location, industry, total_laid_off, 
            percentage_laid_off, `date`, stage, country) 
			as row_num
			FROM layoffs_staging) TEMP
WHERE row_num >1;

-- We will create a copy of this table and add a Unique identifier column 
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `layoffs_staging2` 
SELECT *,  ROW_NUMBER() 
			OVER(PARTITION BY company, location, industry, total_laid_off, 
            percentage_laid_off, `date`, stage, country) 
			as row_num
FROM layoffs_staging;

-- NOW WE CAN REMOVE
DELETE FROM layoffs_staging2
WHERE row_num >1;

-- Check 
SELECT * FROM layoffs_staging2
WHERE row_num >1;



-- Standardize
SELECT distinct(company) FROM layoffs_staging2;

-- Use Trim() to remove whitespaces 
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Look at industry 
SELECT distinct(industry) FROM layoffs_staging2
ORDER BY 1;

/* We find NUll and blank values and 
"CryptoCurrency", "Crypto Currency" and "Crypto" all same industry. We must fix it */

-- First we fix Crypto
SELECT * FROM layoffs_staging2
WHERE industry LIKE 'Cry%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Cry%';

--  null and empty rows, Let's fix it
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb%';

-- airbnb is a travel, but this one just isn't populated.
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values

-- step 1
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- step 2. now we need to populate those nulls if possible
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- and if we check it looks like Bally's was the only one without a populated row. we can remove this row
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Investigate country column
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;

-- we have some "United States" and some "United States." with a period at the end. Must fix that.
SELECT DISTINCT(TRIM(TRAILING '.' FROM country)) 
FROM layoffs_staging2
WHERE country LIKE 'United St%';

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);



-- fix the date columns:
SELECT * FROM world_layoffs.layoffs_staging2;

-- Date is in string format 
-- we can use str to date to update this field

UPDATE layoffs_staging2
SET date = CASE
    WHEN date LIKE '%/%' THEN STR_TO_DATE(date, '%m/%d/%Y')
    WHEN date LIKE '%-%' THEN STR_TO_DATE(date, '%Y-%m-%d')
    ELSE NULL
END;

-- now we can convert the data type properly
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;




--  remove any columns and rows we dont want. 
-- we dont want total_laid_off and percentages_laid_off that are NULl

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- We dont want the row_num column either
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- ---------------------------   EDA   --------------------------------------------------------------------------

--  HIGHEST LAYOFFS AT A SINGLE TIME
SELECT * FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC 
limit 10; -- done not needed

-- HIGHEST FUNDS RAISED COMPANIES WHO LAYEDOFF ENTIRELY  layoffs_staging2
SELECT * FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;  --  no but easy  dont think its needed


-- COMPANIES WITH THE HIGHEST LAYOFFS
SELECT company, SUM(total_laid_off) AS TOTAL_OFF 
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY TOTAL_OFF DESC;  -- Top 5 are tech companies done

-- INDUSTRIES WITH THE HIGHEST LAYOFFS
SELECT industry, SUM(total_laid_off) AS TOTAL_OFF 
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY TOTAL_OFF DESC; -- done with dashboard

-- COUNTRY WITH THE HIGHEST LAYOFFS
SELECT country, SUM(total_laid_off) AS TOTAL_OFF 
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY TOTAL_OFF DESC;

-- YEARS IN WITH THE HIGHEST LAYOFFS
SELECT YEAR(`date`) AS LAYOFF_YEAR, SUM(total_laid_off) AS TOTAL_OFF 
FROM world_layoffs.layoffs_staging2
GROUP BY  YEAR(`date`)
HAVING LAYOFF_YEAR IS NOT NULL
ORDER BY TOTAL_OFF DESC ;

-- STAGES OF COMPANIES WITH HIGHEST LAYOFFS
SELECT stage, SUM(total_laid_off) AS TOTAL_OFF 
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- Running total of layoffs from the starting month till the end
WITH temp AS (
		SELECT substring(`date`, 1,7) AS month_, SUM(total_laid_off) AS total_off
		FROM world_layoffs.layoffs_staging2
		WHERE substring(`date`, 1,7) IS NOT NULL
		GROUP BY month_
		ORDER BY 1 ASC
)
SELECT month_ ,total_off,SUM(total_off) OVER(ORDER BY month_)AS running_total
FROM temp;


-- Top 5 companies each year with highest layoffs
WITH temp (company, years, total_laid_off) as (
			SELECT 		
			company, year(`date`),
			SUM(total_laid_off) 
            FROM world_layoffs.layoffs_staging2
			GROUP BY company, year(`date`)
			order by 3 desc),
            
 company_year_rank AS (
			SELECT *, 
			DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
			FROM temp
			WHERE years IS NOT NULL)
SELECT * FROM company_year_rank
WHERE ranking <=5;






