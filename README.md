# Clean Econ Dev

This repository hosts R scripts and data for analyzing clean energy and economic development trends. It was originally organized for use in RStudio but can be run in any R environment once dependencies and paths are configured.

## Repository Layout

```
/Clean_Econ_Dev
├── CRED R Setup Script.R        - Environment setup, API keys, working dir
├── State_Fact_Base.R            - Build state indicator fact base
├── Econ Dev Scripts/            - Sector analyses & dashboards
│   ├── All_Geos_Combined.R      - Merge county, state, and metro data
│   ├── All_Geos_Combined_Dashboard  - Shiny app for interactive plots
│   ├── Electricity.R, Industry.R, Transport.R, ...
│   └── ... (other thematic scripts)
├── V2 Columbia Critical Minerals Dashboard Scrape  - Web scraper
├── Data/
│   └── county_vars.csv          - Sample county-level dataset
└── Clean_Econ_Dev.Rproj         - RStudio project file
```

## Getting Started

1. **Install R packages** – See the library calls in [`CRED R Setup Script.R`](CRED%20R%20Setup%20Script.R) for the list of packages. You will need packages such as `tidyverse`, `sf`, `censusapi`, `shiny`, etc.
2. **Set your working directory** – The setup script expects paths like `OneDrive - RMI/...`. Update `setwd()` and any file paths to match your environment.
3. **Provide API keys** – Several scripts rely on services like the U.S. Census API. Define keys in `.Renviron` or edit the scripts directly, e.g. `Sys.setenv(CENSUS_KEY='...')` in [`All_Geos_Combined.R`](Econ%20Dev%20Scripts/All_Geos_Combined.R#L17-L19).

## Data Pipelines

- **State Fact Base** – [`State_Fact_Base.R`](State_Fact_Base.R) downloads and merges data such as EIA emissions, tax rates, right‑to‑work status, and energy prices. Example code:
  ```r
  url <- 'https://www.eia.gov/environment/emissions/state/excel/table1.xlsx'
  GET(url = url, write_disk(tempfile(fileext = ".xlsx"), overwrite = TRUE))
  state_ems <- read_excel(temp_file, sheet = 1, skip = 4)
  ```
  【F:State_Fact_Base.R†L40-L52】
- **Geographic Aggregation** – [`All_Geos_Combined.R`](Econ%20Dev%20Scripts/All_Geos_Combined.R) combines county, congressional district, and other geographies. It creates a unified table joined to policy and investment metrics. Example snippet:
  ```r
  geo <- county_2020 %>%
    mutate(state.fips = as.numeric(substr(GEOID,1,2))) %>%
    left_join(states_simple %>% select(fips, abbr, full), by = c("state.fips" = "fips"))
  ```
  【F:Econ Dev Scripts/All_Geos_Combined.R†L56-L64】

## Dashboard

The `All_Geos_Combined_Dashboard` script launches a Shiny app with advanced filtering and plotting. It auto‑detects the dataset path and displays correlation metrics. Launch the app by sourcing the script in R:
```r
source('Econ Dev Scripts/All_Geos_Combined_Dashboard')
```
Key features include auto colour scales and axis units:
```r
unit_choices <- c("Number"="num","Thousands (K)"="k","Millions (M)"="m","Billions (B)"="b")
shinyApp(ui, server)
```
【F:Econ Dev Scripts/All_Geos_Combined_Dashboard†L56-L77】【F:Econ Dev Scripts/All_Geos_Combined_Dashboard†L333-L339】

## Web Scraping

`V2 Columbia Critical Minerals Dashboard Scrape` is a standalone script that performs a BFS crawl, builds API endpoints, and parses results into CSV and RDS files. Part of the fetch loop:
```r
results_list <- map(all_urls_to_fetch, function(u) {
  response <- fetch_url(u)
  raw_file_path <- save_raw_response(response, u)
  parsed_result <- parse_response(response)
  list(url = u, status_code = status_code(response), parsed_data = parsed_result$data)
})
```
【F:V2 Columbia Critical Minerals Dashboard Scrape†L288-L312】

## Example Dataset

`Data/county_vars.csv` holds county‑level variables such as industry codes, policy scores, tax metrics, and investment totals. The header looks like:
```
county,county_name,industry_code,industry_desc,rca,density,pci,eci, ...
```
【F:Data/county_vars.csv†L1】

## Next Steps

- Create parameterized configuration for file paths and API keys.
- Modularize repeated code into functions for easier maintenance.
- Add unit tests or data validation checks.

---
This README provides a high‑level overview of the project structure and how to run the main scripts. Explore each R file for detailed workflows and customize the paths to your environment.
