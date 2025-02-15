/*
COVID-19 Data Exploration 

Skills used: Joins, CTEs, Temp Tables, Window Functions, Aggregate Functions, Creating Views, Data Type Conversions
*/

SELECT *
FROM CovidDataAnalysis..CovidDeaths
WHERE continent IS NOT NULL
ORDER BY 3,4

SELECT *
FROM CovidDataAnalysis..CovidVaccinations
ORDER BY 3,4

-- select data that we are going to be using
SELECT location, date, total_cases, new_cases, total_deaths, population 
FROM CovidDataAnalysis..CovidDeaths
WHERE continent IS NOT NULL
ORDER BY 1,2

-- looking at total cases vs total deaths
SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 AS DeathPercentage
FROM CovidDataAnalysis..CovidDeaths
ORDER BY 1,2

-- Improvements over the first query:
-- 1. Uses ISNULL(total_deaths, 0) to handle NULL values in total_deaths
-- 2. Uses NULLIF(total_cases, 0) to prevent division by zero errors
-- 3. Converts total_deaths to FLOAT to ensure accurate decimal division
-- this query shows likelihood of dying if you contract covid in your country
SELECT location, date, total_cases, total_deaths, 
       (CAST(ISNULL(total_deaths, 0) AS FLOAT) / NULLIF(total_cases, 0)) * 100 AS DeathPercentage
FROM CovidDataAnalysis..CovidDeaths
WHERE location LIKE '%states%'
and continent IS NOT NULL
ORDER BY 1,2

-- looking at total cases vs population
-- shows what percentage of population got covid
SELECT location, date, population, total_cases, 
       (CAST(ISNULL(total_cases, 0) AS FLOAT) / NULLIF(population, 0)) * 100 AS PercentPopulationInfected
FROM CovidDataAnalysis..CovidDeaths
-- WHERE location LIKE '%states%'
ORDER BY 1,2

-- looking at countries with highest infection rate compared to population
SELECT location, population, MAX(total_cases) AS HighestInfectionCount,
       MAX((CAST(ISNULL(total_cases, 0) AS FLOAT) / NULLIF(population, 0))) * 100 AS PercentPopulationInfected
FROM CovidDataAnalysis..CovidDeaths
-- WHERE location LIKE '%states%'
GROUP BY location, population
ORDER BY PercentPopulationInfected DESC

-- showing the countries with the highest death count per population
SELECT location, MAX(cast(total_deaths AS int)) AS TotalDeathCount
FROM CovidDataAnalysis..CovidDeaths
-- WHERE location LIKE '%states%'
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY TotalDeathCount DESC

-- LET'S BREAK THINGS DOWN BY CONTINENT
-- showing the continents with highest death count per population
SELECT continent, MAX(cast(total_deaths AS int)) AS TotalDeathCount
FROM CovidDataAnalysis..CovidDeaths
-- WHERE location LIKE '%states%'
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY TotalDeathCount DESC

-- GLOBAL NUMBERS
SELECT date, SUM(new_cases) AS total_cases, SUM(cast(new_deaths as int)) AS total_deaths,
(SUM(CAST(new_deaths AS FLOAT)) / NULLIF(SUM(new_cases), 0)) * 100 AS DeathPercentage
FROM CovidDataAnalysis..CovidDeaths
-- WHERE location LIKE '%states%'
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY 1,2

-- death percentage overall across the world
SELECT SUM(new_cases) AS total_cases, SUM(cast(new_deaths as int)) AS total_deaths,
(SUM(CAST(new_deaths AS FLOAT)) / NULLIF(SUM(new_cases), 0)) * 100 AS DeathPercentage
FROM CovidDataAnalysis..CovidDeaths
-- WHERE location LIKE '%states%'
WHERE continent IS NOT NULL
-- GROUP BY date
ORDER BY 1,2

-- joining the two tables covid deaths and covid vaccinations by location and date
SELECT *
FROM CovidDataAnalysis..CovidDeaths dea
JOIN CovidDataAnalysis..CovidVaccinations vac
on dea.location = vac.location
and dea.date = vac.date

-- looking at total population vs vaccination
-- shows percentage of population that has recieved at least one covid vaccine
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(CONVERT(int,vac.new_vaccinations)) OVER (Partition by cast(dea.location AS NVARCHAR(255)) ORDER BY cast(dea.location AS NVARCHAR(255)), dea.date) AS RollingPeopleVaccinated
FROM CovidDataAnalysis..CovidDeaths dea
JOIN CovidDataAnalysis..CovidVaccinations vac
on dea.location = vac.location
and dea.date = vac.date
WHERE dea.continent IS NOT NULL
ORDER BY 2,3

-- use CTE (common table expression) to perform calculation on Partition by in previous query
WITH PopvsVac (continent, location, date, population, new_vaccinations, RollingPeopleVaccinated)
AS
(
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(CONVERT(int, ISNULL(vac.new_vaccinations, 0))) OVER (Partition by cast(dea.location AS NVARCHAR(255)) ORDER BY cast(dea.location AS NVARCHAR(255)), dea.date) AS RollingPeopleVaccinated
FROM CovidDataAnalysis..CovidDeaths dea
LEFT JOIN CovidDataAnalysis..CovidVaccinations vac
on dea.location = vac.location
and dea.date = vac.date
WHERE dea.continent IS NOT NULL
-- ORDER BY 2,3
)
SELECT *, (RollingPeopleVaccinated/population)*100
FROM PopvsVac

-- using Temp Table to perform calculations on Partition By in previous query
DROP TABLE if EXISTS #PercentPopulationVaccianted
CREATE TABLE #PercentPopulationVaccianted
(
    continent NVARCHAR(255),
    location NVARCHAR(255),
    date datetime,
    population numeric,
    new_vaccinations numeric,
    RollingPeopleVaccinated numeric
)
INSERT into #PercentPopulationVaccianted
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(CONVERT(int, ISNULL(vac.new_vaccinations, 0))) OVER (Partition by cast(dea.location AS NVARCHAR(255)) ORDER BY cast(dea.location AS NVARCHAR(255)), dea.date) AS RollingPeopleVaccinated
FROM CovidDataAnalysis..CovidDeaths dea
LEFT JOIN CovidDataAnalysis..CovidVaccinations vac
on dea.location = vac.location
and dea.date = vac.date
-- WHERE dea.continent IS NOT NULL
-- ORDER BY 2,3

SELECT *, (RollingPeopleVaccinated/population)*100
from #PercentPopulationVaccianted

-- creating view to store data for later visualizations
CREATE VIEW PercentPopulationVaccianted AS 
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(CONVERT(int, ISNULL(vac.new_vaccinations, 0))) OVER (Partition by cast(dea.location AS NVARCHAR(255)) ORDER BY cast(dea.location AS NVARCHAR(255)), dea.date) AS RollingPeopleVaccinated
FROM CovidDataAnalysis..CovidDeaths dea
LEFT JOIN CovidDataAnalysis..CovidVaccinations vac
on dea.location = vac.location
and dea.date = vac.date
WHERE dea.continent IS NOT NULL
-- ORDER BY 2,3

SELECT *
from PercentPopulationVaccianted















