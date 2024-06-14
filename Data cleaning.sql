--- data cleaning 


select * from layoffs;

--- Remove Duplicates
--- standardize data 
--- null or blank values 
--- remove any columns and columns

create table layoff_staging
like layoffs;

insert layoff_staging
select * from  layoffs;

select * from layoff_staging;

select * ,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, 'date') as row_num
from layoff_staging;

with duplicate_cte As 
(
select * ,
row_number() over(
partition by company, location,
 industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoff_staging
)
 select * 
 from duplicate_cte
 where row_num > 1;
 
 select * from 
 layoff_staging
 where company = 'Casper';

CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  row_num int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select *
from layoff_staging2;

insert into layoff_staging2
select * ,
row_number() over(
partition by company, location,
 industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoff_staging;

select *
from layoff_staging2 
where row_num > 1 ;

delete 
from layoff_staging2 
where row_num > 1 ;

select *
from layoff_staging2;

--- standardizing data 

select company, trim(company)
from layoff_staging2;

update layoff_staging2
set company = trim(company);

select *
from layoff_staging2
where industry like 'Crypto%'; 

update layoff_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct industry 
from layoff_staging2;

select distinct location
from layoff_staging2
order by 1;

select distinct country , trim(trailing '.' from country)
from layoff_staging2
order by 1;

update layoff_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

select date
from layoff_staging2;

select date,
str_to_date(date, '%m/%d/%Y')
from layoff_staging2;

update layoff_staging2
set date = str_to_date(date, '%m/%d/%Y');

ALTER TABLE  layoff_staging2
modify column date Date;

select * 
from layoff_staging2
where total_laid_off is null
and percentage_laid_off is null;

update layoff_staging2
set industry = null
where industry ='';



select * 
from layoff_staging2
where industry is null
or industry = '';

select * 
from layoff_staging2 t1
join layoff_staging2 t2
on t1.company = t1.company 
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

select t1.industry , t2.industry
from layoff_staging2 t1
join layoff_staging2 t2
on t1.company = t1.company 
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoff_staging2 t1
join layoff_staging2 t2
on t1.company = t1.company 
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select* 
from layoff_staging2
where company = 'Airbnb';

select* 
from layoff_staging2
where company  like 'Bally%';

select* 
from layoff_staging2;

delete
from layoff_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoff_staging2;

alter table layoff_staging2
drop column row_num;

--- Explorartory data analysis 

Select max(total_laid_off), max(percentage_laid_off)
from layoff_staging2;

select *
from layoff_staging2
where percentage_laid_off = 1
order by total_laid_off desc;

select company, sum(total_laid_off)
from layoff_staging2
group by company 
order by 2 desc;

select industry, sum(total_laid_off)
from layoff_staging2
group by industry 
order by 2 desc;

select country, sum(total_laid_off)
from layoff_staging2
group by country 
order by 2 desc;

select year(date), sum(total_laid_off)
from layoff_staging2
group by year(date) 
order by 1 desc;

select stage, sum(total_laid_off)
from layoff_staging2
group by stage 
order by 2 desc;

select substring(date,6,2) as month
from layoff_staging2;

select substring(date,6,2) as month, SUM(total_laid_off)
from layoff_staging2
group by month;


select substring(date,1,7) as month, SUM(total_laid_off)
from layoff_staging2
where substring(date,1,7) is not null
group by month
order by 1 asc;

with rolling_total as 
(
select substring(date,1,7) as month, SUM(total_laid_off) as total_off
from layoff_staging2
where substring(date,1,7) is not null
group by month
order by 1 asc
)
select month, sum(total_off) over(order by month) as total_off
from rolling_total;

select company , year(date), sum(total_laid_off)
from layoff_staging2
group by company, year(date)
order by company asc;

select company , year(date), sum(total_laid_off)
from layoff_staging2
group by company, year(date)
order by 3 desc;

with company_year(company , year, total_laid_off) as 
(
 select company , year(date), sum(total_laid_off)
from layoff_staging2
group by company, year(date)
), company_year_rank as
(select *, dense_rank() over (partition by year order by total_laid_off desc) as ranking
from company_year
where year is not null
)
select * 
from company_year_rank
where ranking <= 5;





