# ============================================================================
# RMI economic development footprint map
# ----------------------------------------------------------------------------
# Produces a US Albers map showing five engagement tiers:
#   1. Multi-state regional engagement   (light blue fill)
#   2. State-level engagement            (medium blue fill)
#   3. Sub-state regional engagement     (Energy teal fill)
#   4. County-level engagement           (dark teal fill)
#   5. Bootcamp cities                   (orange dots with labels)
#
# Color hierarchy = engagement intensity. Light blue (broadest reach) →
# medium blue (full state) → Energy teal (regional partnership) →
# dark teal (single county) → orange (bootcamp city). Where geographies
# overlap, the more specific tier renders on top.
#
# Dependencies: tigris, sf, dplyr, ggplot2, ggrepel, tibble
# ============================================================================

library(tigris)
library(sf)
library(dplyr)
library(ggplot2)
library(ggrepel)
library(tibble)

options(tigris_use_cache = TRUE)
sf_use_s2(FALSE)  # planar geometry — avoids spherical edge cases on small polys

# ---- RMI brand colors -------------------------------------------------------
rmi_blue_spruce  <- "#003B63"
rmi_energy       <- "#45CFCC"
rmi_basalt       <- "#58595B"
multistate_fill  <- "#C9E3F0"
state_fill       <- "#5887AE"
substate_fill    <- rmi_energy
county_fill      <- "#007B7B"
bootcamp_orange  <- "#F58228"
neutral_fill     <- "#EDEDED"

# ============================================================================
# 1. Define engagement geographies via FIPS codes
# ============================================================================

# ---- Multi-state regional engagements --------------------------------------
multistate_regions <- list(
  Southeast = c(
    "01",  # Alabama
    "13",  # Georgia
    "21",  # Kentucky
    "37",  # North Carolina
    "45",  # South Carolina
    "47",  # Tennessee
    "51"   # Virginia
  ),
  
  South_Central_SW = c(
    "04",  # Arizona
    "05",  # Arkansas
    "08",  # Colorado
    "22",  # Louisiana
    "35",  # New Mexico
    "40",  # Oklahoma
    "48"   # Texas
  )
)

multistate_fips <- unlist(multistate_regions, use.names = FALSE)

# ---- State-level engagements ------------------------------------------------
state_level_fips <- c(
  "42",  # Pennsylvania
  "39",  # Ohio
  "18",  # Indiana
  "26",  # Michigan
  "17",  # Illinois
  "55",  # Wisconsin
  "27",  # Minnesota
  "35",  # New Mexico
  "45",  # South Carolina #NOTE: Refers to: https://rmi.org/insight/toward-a-cleantech-led-economic-development-strategy-for-south-carolina/ 
  "41"   # Oregon #NOTE: Oregon refers to engaging on the manufacturing task force report: https://cdn.orbusinesscouncil.org/docs/policy/Oregon_Clean_Technology_Task_Force_Report.pdf  
)

# ---- Sub-state regional engagements ----------------------------------------
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
    "42003",  # Allegheny
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

# ---- Optional: 18-county Northeast Ohio footprint ---------------------------
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

# ---- Bootcamps --------------------------------------------------------------
bootcamps <- tibble::tribble(
  ~name,            ~lon,        ~lat,
  "Pittsburgh",      -79.9959,   40.4406,
  "Milwaukee",       -87.9065,   43.0389,
  "Salt Lake City", -111.8910,   40.7608,
  "Twin Cities",     -93.2650,   44.9778,
  "Washington DC",   -77.0369,   38.9072,
  "Durham",          -78.8986,   35.9940,
  "Boulder",        -105.2705,   40.0150
)

# ============================================================================
# 2. Pull and transform geometries
# ----------------------------------------------------------------------------
# Important fix:
#   shift_geometry() should be run BEFORE filtering out AK, HI, and PR.
#   Otherwise tigris warns that there are no AK/HI/PR features to shift.
# ============================================================================

all_states <- states(cb = TRUE, resolution = "20m", year = 2022)
all_counties <- counties(cb = TRUE, resolution = "20m", year = 2022)

all_states_shifted <- shift_geometry(all_states)
all_counties_shifted <- shift_geometry(all_counties)

bootcamps_sf <- st_as_sf(
  bootcamps,
  coords = c("lon", "lat"),
  crs = 4326
) |>
  shift_geometry()

# Drop territories after shift_geometry().
drop_territories <- c("60", "66", "69", "72", "78")

us_states <- all_states_shifted |>
  filter(!STATEFP %in% drop_territories)

us_counties <- all_counties_shifted |>
  filter(!STATEFP %in% drop_territories)

# Tag state engagement type. State-level wins ties with multi-state.
us_states <- us_states |>
  mutate(
    engagement = case_when(
      STATEFP %in% state_level_fips ~ "state",
      STATEFP %in% multistate_fips  ~ "multistate",
      TRUE                          ~ "none"
    )
  )

substate_counties <- us_counties |>
  filter(GEOID %in% substate_fips)

county_level_counties <- us_counties |>
  filter(GEOID %in% county_level_fips)

us_outline <- us_states |>
  summarise(geometry = st_union(geometry), .groups = "drop")

# ============================================================================
# 3. Build the map
# ============================================================================

p <- ggplot() +
  geom_sf(
    data      = us_states,
    fill      = neutral_fill,
    color     = "white",
    linewidth = 0.25
  ) +
  geom_sf(
    data      = us_states |> filter(engagement == "multistate"),
    aes(fill  = "Multi-state engagement"),
    color     = "white",
    linewidth = 0.25
  ) +
  geom_sf(
    data      = us_states |> filter(engagement == "state"),
    aes(fill  = "State-level engagement"),
    color     = "white",
    linewidth = 0.25
  ) +
  geom_sf(
    data      = substate_counties,
    aes(fill  = "Sub-state engagement"),
    color     = "white",
    linewidth = 0.15
  ) +
  geom_sf(
    data      = county_level_counties,
    aes(fill  = "County-level engagement"),
    color     = "white",
    linewidth = 0.15
  ) +
  geom_sf(
    data      = us_outline,
    fill      = NA,
    color     = rmi_blue_spruce,
    linewidth = 0.4
  ) +
  geom_sf(
    data    = bootcamps_sf,
    aes(fill = "Bootcamps"),
    shape   = 21,
    color   = rmi_blue_spruce,
    size    = 3.5,
    stroke  = 0.6
  ) +
  geom_text_repel(
    data               = bootcamps_sf,
    aes(label = name, geometry = geometry),
    stat               = "sf_coordinates",
    family             = "sans",
    fontface           = "bold",
    size               = 3.2,
    color              = rmi_blue_spruce,
    bg.color           = "white",
    bg.r               = 0.18,
    box.padding        = 0.45,
    point.padding      = 0.55,
    min.segment.length = 0.2,
    segment.color      = rmi_basalt,
    segment.size       = 0.3
  ) +
  scale_fill_manual(
    name = NULL,
    breaks = c(
      "Multi-state engagement",
      "State-level engagement",
      "Sub-state engagement",
      "County-level engagement",
      "Bootcamps"
    ),
    values = c(
      "Multi-state engagement"  = multistate_fill,
      "State-level engagement"  = state_fill,
      "Sub-state engagement"    = substate_fill,
      "County-level engagement" = county_fill,
      "Bootcamps"               = bootcamp_orange
    )
  ) +
  guides(
    fill = guide_legend(
      nrow = 1,
      override.aes = list(
        shape    = c(22, 22, 22, 22, 21),
        size     = c(5, 5, 5, 5, 3.5),
        color    = c("white", "white", "white", "white", rmi_blue_spruce),
        stroke   = c(0.4, 0.4, 0.4, 0.4, 0.6),
        linetype = 0
      )
    )
  ) +
  labs(
    title    = "Our economic development footprint",
    subtitle = "Multi-state, state-level, sub-state, county-level, and bootcamp engagements"
  ) +
  theme_void(base_family = "sans") +
  theme(
    plot.title = element_text(
      color  = rmi_blue_spruce,
      face   = "bold",
      size   = 16,
      margin = margin(b = 4)
    ),
    plot.subtitle = element_text(
      color  = rmi_basalt,
      size   = 11,
      margin = margin(b = 12)
    ),
    legend.position  = "bottom",
    legend.text      = element_text(color = rmi_blue_spruce, size = 10),
    legend.key       = element_blank(),
    legend.spacing.x = unit(0.4, "cm"),
    plot.background  = element_rect(fill = "white", color = NA),
    plot.margin      = margin(16, 16, 16, 16)
  )

# ============================================================================
# 4. Save and print
# ============================================================================

ggsave(
  filename = "rmi_footprint_map.png",
  plot     = p,
  width    = 12,
  height   = 7.5,
  dpi      = 300,
  bg       = "white"
)

ggsave(
  filename = "rmi_footprint_map.pdf",
  plot     = p,
  width    = 12,
  height   = 7.5,
  bg       = "white"
)

print(p)
