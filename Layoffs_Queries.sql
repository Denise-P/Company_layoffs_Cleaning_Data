use layoffs;

# 1. Remove Duplicates
# 2. Standardize the Data
# 3. Null Values or blank values
# 4. Remove any columns


#Step 1: Remove Duplicates

select *
from layoffs;

create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select * 
from layoffs;

select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging;

with duplicate_cte as
(
select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

#Check if Oda rows match in each column (Nope :p)
select *
from layoffs_staging
where company = 'Oda';

#Check if Casper rows match in each column (Yup)
select *
from layoffs_staging
where company = 'Casper';

#Now create a real table to remove duplicates

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions
)  as row_num
from layoffs_staging;

select * 
from layoffs_staging2
where row_num > 1;

delete 
from layoffs_staging2
where row_num > 1;

select * 
from layoffs_staging2;

#Stnadardize data

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2
order by 1;

select *
from  layoffs_staging2
where industry like "Crypto%";

update layoffs_staging2
set industry = 'Crypto'
where industry like "Crypto%";

select distinct location
from layoffs_staging2
order by 1;

select distinct country
from layoffs_staging2
order by 1;

select country, trim(trailing '.' from country) #specify what we are removing
from layoffs_staging2
where country like 'United States%'
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select `date`
from layoffs_staging2;

alter table layoffs_staging2
modify column `date` date;

select *
from layoffs_staging2;

#NUlls blank values

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2
where industry is null or industry = '';

select *
from layoffs_staging2
where company = 'Airbnb';

#Join Testing

select t1.industry, t2.industry 
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2
set industry = null
where industry = '';

select *
from layoffs_staging2
where industry is null;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select * 
from layoffs_staging2
where company = 'Airbnb';

#Checking for any issues with industry
select *
from layoffs_staging2
where industry is null;

#Bally is still NULL. Maybe fixable
select *
from layoffs_staging2
where company like 'Bally%';

#Not enough data to fill in more null cells so move on

#Remove columns

#How to decide wheater to remove a column or not
	#Do you need to use these columns to find data through query or needed for the assignment?
    #Is the column relevent to the data?
    #Is there to many blanks or null cells to justify keeping it?
  
#First we delete rows that cuases issues in the column and see if it fixes the column
delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;