# ============================================================================
# RMI economic development footprint — county-level CSV export
# ----------------------------------------------------------------------------
# Produces one row per US county (cb = TRUE, year = 2024) with the columns:
#   county_geoid    e.g. "42003"
#   county_name     e.g. "Allegheny"
#   state_name      e.g. "Pennsylvania"
#   classification  one of: "County-Level", "Multi-County",
#                           "State-Level", "Multi-State", NA
#
# Classification priority (most specific wins):
#   1. County-Level  -- single county explicitly named (e.g. Allegheny County PA)
#   2. Multi-County  -- county is part of a sub-state regional engagement
#                       (Greater Cleveland, GNO, Twin Cities, GFDA, etc.)
#   3. State-Level   -- state is in state_level_fips and the county isn't
#                       already tagged by a more specific tier
#   4. Multi-State   -- state is part of a multi-state regional engagement
#                       (currently: Great Lakes states)
#   5. NA / blank    -- everything else
#
# Dependencies: tigris, sf, dplyr, readr
# ============================================================================

library(tigris)
library(sf)
library(dplyr)
library(readr)

options(tigris_use_cache = TRUE)

# ============================================================================
# 1. Engagement definitions
# ============================================================================

# ---- Multi-state regional engagements --------------------------------------
multistate_regions <- list(
  
  Great_Lakes = c(
    "17",  # Illinois
    "18",  # Indiana
    "26",  # Michigan
    "27",  # Minnesota
    "36",  # New York
    "39",  # Ohio
    "42",  # Pennsylvania
    "55"   # Wisconsin
  )
  
  # ---- To re-introduce the original Southeast / South Central SW
  # ---- multi-state engagements, uncomment the blocks below:
  #
  # ,
  # Southeast = c(
  #   "01",  # Alabama
  #   "13",  # Georgia
  #   "21",  # Kentucky
  #   "37",  # North Carolina   (also in state_level_fips; State-Level wins)
  #   "45",  # South Carolina   (also in state_level_fips; State-Level wins)
  #   "47",  # Tennessee
  #   "51"   # Virginia
  # ),
  # South_Central_SW = c(
  #   "04",  # Arizona
  #   "05",  # Arkansas
  #   "08",  # Colorado
  #   "22",  # Louisiana
  #   "35",  # New Mexico       (also in state_level_fips; State-Level wins)
  #   "40",  # Oklahoma
  #   "48"   # Texas
  # )
)

multistate_fips <- unlist(multistate_regions, use.names = FALSE)

# ---- State-level engagements ------------------------------------------------
# NOTE: Great Lakes states (IL, IN, MI, MN, OH, PA, WI) previously listed here
# have been demoted to Multi-State (Great Lakes regional engagement).
state_level_fips <- c(
  "35",  # New Mexico
  "37",  # North Carolina
  "45",  # South Carolina   (https://rmi.org/insight/toward-a-cleantech-led-economic-development-strategy-for-south-carolina/)
  "41"   # Oregon            (https://cdn.orbusinesscouncil.org/docs/policy/Oregon_Clean_Technology_Task_Force_Report.pdf)
)

# ---- Sub-state regional ("Multi-County") engagements -----------------------
substate_regions <- list(
  
  GFDA = c(
    "30005",  # Blaine
    "30007",  # Broadwater
    "30015",  # Chouteau
    "30027",  # Fergus
    "30035",  # Glacier
    "30041",  # Hill
    "30045",  # Judith Basin
    "30049",  # Lewis and Clark
    "30051",  # Liberty
    "30069",  # Petroleum
    "30071",  # Phillips
    "30073",  # Pondera
    "30099",  # Teton
    "30101"   # Toole
  ),
  
  Greater_Cleveland = c(
    "39007",  # Ashtabula
    "39035",  # Cuyahoga
    "39055",  # Geauga
    "39085",  # Lake
    "39093",  # Lorain
    "39103"   # Medina
  ),
  
  Allegheny_Conference = c(
    "42003",  # Allegheny       (NOTE: also in county_level_fips; County-Level wins)
    "42005",  # Armstrong
    "42007",  # Beaver
    "42019",  # Butler
    "42051",  # Fayette
    "42059",  # Greene
    "42063",  # Indiana
    "42073",  # Lawrence
    "42125",  # Washington
    "42129"   # Westmoreland
  ),
  
  Miami_MSA = c(
    "12011",  # Broward
    "12086"   # Miami-Dade
  ),
  
  GNO = c(
    "22051",  # Jefferson
    "22071",  # Orleans
    "22075",  # Plaquemines
    "22087",  # St. Bernard
    "22089",  # St. Charles
    "22093",  # St. James
    "22095",  # St. John the Baptist
    "22103",  # St. Tammany
    "22105",  # Tangipahoa
    "22117"   # Washington
  ),
  
  Twin_Cities = c(
    "27003",  # Anoka
    "27019",  # Carver
    "27037",  # Dakota
    "27053",  # Hennepin
    "27123",  # Ramsey
    "27139",  # Scott
    "27163"   # Washington
  ),
  
  Huntsville_MSA = c(
    "01083",  # Madison
    "01089"   # Limestone
  ),
  
  Greater_Des_Moines = c(
    "19001",  # Adair
    "19039",  # Clarke
    "19049",  # Dallas
    "19077",  # Guthrie
    "19099",  # Jasper
    "19121",  # Madison
    "19123",  # Mahaska
    "19125",  # Marion
    "19127",  # Marshall
    "19153",  # Polk
    "19157",  # Poweshiek
    "19171",  # Tama
    "19181"   # Warren
  )
)

substate_fips <- unlist(substate_regions, use.names = FALSE)

# ---- County-level engagements ----------------------------------------------
county_level_regions <- list(
  Allegheny_County = c(
    "42003"   # Allegheny County, PA
  )
)

county_level_fips <- unlist(county_level_regions, use.names = FALSE)

# ---- Optional: 18-county Northeast Ohio footprint (not used in output) -----
gcp_neo18_fips <- c(
  "39005",  # Ashland
  "39007",  # Ashtabula
  "39019",  # Carroll
  "39029",  # Columbiana
  "39035",  # Cuyahoga
  "39043",  # Erie
  "39055",  # Geauga
  "39077",  # Huron
  "39085",  # Lake
  "39093",  # Lorain
  "39099",  # Mahoning
  "39103",  # Medina
  "39133",  # Portage
  "39139",  # Richland
  "39151",  # Stark
  "39153",  # Summit
  "39155",  # Trumbull
  "39169"   # Wayne
)

# ============================================================================
# 2. Pull counties + states from tigris (cb, 2024)
# ============================================================================

all_counties <- counties(cb = TRUE, resolution = "20m", year = 2024)
all_states   <- states(cb = TRUE, resolution = "20m", year = 2024)

# Drop the five territories we don't classify.
drop_territories <- c("60", "66", "69", "72", "78")

# Lookup for state name by STATEFP — robust to tigris version differences in
# whether STATE_NAME is already attached to the counties frame.
state_lookup <- all_states |>
  st_drop_geometry() |>
  filter(!STATEFP %in% drop_territories) |>
  select(STATEFP, state_name = NAME)

# ============================================================================
# 3. Build the classification table
# ============================================================================

engagement_by_county <- all_counties |>
  st_drop_geometry() |>
  filter(!STATEFP %in% drop_territories) |>
  left_join(state_lookup, by = "STATEFP") |>
  mutate(
    classification = case_when(
      GEOID    %in% county_level_fips ~ "County-Level",
      GEOID    %in% substate_fips     ~ "Multi-County",
      STATEFP  %in% state_level_fips  ~ "State-Level",
      STATEFP  %in% multistate_fips   ~ "Multi-State",
      TRUE                            ~ NA_character_
    )
  ) |>
  transmute(
    county_geoid   = GEOID,
    county_name    = NAME,
    state_name     = state_name,
    classification = classification
  ) |>
  arrange(state_name, county_name)

# ============================================================================
# 4. Write CSV + quick sanity summary
# ============================================================================

write_csv(
  engagement_by_county,
  file = "rmi_engagement_by_county.csv",
  na   = ""
)

cat("Wrote", nrow(engagement_by_county), "rows to rmi_engagement_by_county.csv\n\n")
cat("Counts by classification:\n")
print(
  engagement_by_county |>
    count(classification, name = "n_counties") |>
    mutate(classification = ifelse(is.na(classification), "(blank)", classification))
)
