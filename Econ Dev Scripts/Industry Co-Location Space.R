# ============================================================================
# RMI Industry Co-location & Shiny Dashboard
# Robust, end-to-end script with RMI branding, state/sector/county filters,
# and a 12-county (Greater Des Moines) interactive.
#
# Key improvements vs. original:
# - Stronger error handling, input validation, and reproducibility
# - Faster sparse math (no nested loops), memory-aware ops
# - Cleaner API-like functions with unit-like checks
# - Consistent RMI branding for UI + plots (logo + colors + typography)
# - Single Shiny app with filters: State, NAICS 2-digit Sector, County (multi)
# - National, State, and 12-County (Greater Des Moines) views in one place
# - Sector coloring by NAICS 2-digit; accessible palette & legends
# - Download buttons, logging, and debug toggles
#
# Prereqs:
# install.packages(c(
#   "tidyverse","data.table","Matrix","igraph","plotly","visNetwork","htmlwidgets",
#   "shiny","bslib","shinycssloaders","shinyWidgets","DT","ggrepel","scales","glue",
#   "uwot"
# ))
#
# Data expectation:
# - A CSV with the columns shown in the user's glimpse() of CGT_COUNTY_DATA
# - Path is configurable via DATA_PATH below
#
# ============================================================================

suppressPackageStartupMessages({
  library(tidyverse)
  library(data.table)
  library(Matrix)
  library(igraph)
  library(plotly)
  library(visNetwork)
  library(htmlwidgets)
  library(shiny)
  library(bslib)
  library(shinycssloaders)
  library(shinyWidgets)
  library(DT)
  library(ggrepel)
  library(scales)
  library(glue)
  library(uwot)     # robust, fast 2D projection (UMAP)
})

# ------------------------------
# CONFIG
# ------------------------------
DEBUG            <- TRUE           # set FALSE to reduce console noise
SEED             <- 42             # reproducibility
options(warn=1)                    # show warnings as they occur
set.seed(SEED)

# Cross-platform OneDrive-aware path builder (base R only)
onedrive_root <- function(org = "RMI") {
  home <- path.expand("~")
  sys  <- Sys.info()[["sysname"]]
  
  # OneDrive often exposes these env vars on Windows
  envs <- c("OneDriveCommercial", "OneDriveConsumer", "OneDrive")
  env_paths <- unique(Filter(nzchar, Sys.getenv(envs)))
  
  # Start with generic candidates (works for many setups)
  candidates <- c(
    env_paths,
    file.path(home, "OneDrive"),
    file.path(home, paste0("OneDrive - ", org)),
    file.path(home, "Library", "CloudStorage", paste0("OneDrive-", org)),  # macOS biz
    file.path(home, "Library", "CloudStorage", "OneDrive"),                # macOS personal
    file.path(home, "Library", "CloudStorage", "OneDrive-Personal"),
    file.path(home, "Library", "CloudStorage", "OneDrive-Shared")
  )
  
  # Add Windows-specific fallbacks using the true user profile (not ~/Documents)
  if (identical(sys, "Windows")) {
    up <- Sys.getenv("USERPROFILE")
    if (nzchar(up)) {
      candidates <- c(
        candidates,
        file.path(up, paste0("OneDrive - ", org)),
        file.path(up, "OneDrive")
      )
    }
  }
  
  hit <- candidates[file.exists(candidates) | dir.exists(candidates)]
  if (length(hit) == 0) {
    stop("Couldn't locate your OneDrive folder automatically. ",
         "Please set it manually or adjust the org name.")
  }
  normalizePath(hit[[1]], winslash = "/", mustWork = FALSE)
}

# Build your data path (works on both macOS & Windows)
DATA_PATH <- file.path(
  onedrive_root("RMI"),
  "US Program - Documents",
  "6_Projects",
  "Clean Regional Economic Development",
  "ACRE",
  "Data",
  "CGT_county_data",
  "modified_CGT_files",
  "cgt_county_data_sep_29_2024.csv"
)

# RMI branding (from style guide)
RMI_COLORS <- list(
  blue_spruce = "#003B63",
  energy      = "#45CFCC",
  white       = "#FFFFFF",
  # Select a subset of secondary palette for sectors; the set below is curated for contrast and accessibility
  sector      = c(
    "#0064AC", "#20A0DA", "#56A846", "#A2B539", "#F58228", "#FFCA07", "#C4151C",
    "#6A0A0B", "#3F3969", "#7C77AF", "#1189B1", "#A78563", "#D1BDA1", "#DBDCDE", "#58595B"
  )
)

# RMI logo (provided link)
RMI_LOGO_URL <- "https://rmi.org/wp-content/uploads/2021/08/rmi_logo_horitzontal_no_tagline.svg"

# Greater Des Moines 12-county region (FIPS)
GDM_COUNTIES <- c(
  "19001","19039","19049","19077","19099","19121","19123","19125","19127","19153","19157","19181"
)

# [NEW] NAICS 2-digit code to Sector Description mapping
naics_sector_map <- tibble::tribble(
  ~naics2, ~sector_desc,
  "11", "Agriculture, Forestry, Fishing & Hunting",
  "21", "Mining, Quarrying, & Oil & Gas Extraction",
  "22", "Utilities",
  "23", "Construction",
  "31", "Manufacturing",
  "32", "Manufacturing",
  "33", "Manufacturing",
  "42", "Wholesale Trade",
  "44", "Retail Trade",
  "45", "Retail Trade",
  "48", "Transportation & Warehousing",
  "49", "Transportation & Warehousing",
  "51", "Information",
  "52", "Finance & Insurance",
  "53", "Real Estate & Rental & Leasing",
  "54", "Professional, Scientific, & Technical Services",
  "55", "Management of Companies & Enterprises",
  "56", "Administrative & Support & Waste Management",
  "61", "Educational Services",
  "62", "Health Care & Social Assistance",
  "71", "Arts, Entertainment, & Recreation",
  "72", "Accommodation & Food Services",
  "81", "Other Services (except Public Administration)",
  "90", "Government" # Using 90 as a placeholder per user request
) %>%
  mutate(sector_desc = factor(sector_desc)) # Use factor for consistent ordering

# ------------------------------
# UTILS: Logging & Guardrails
# ------------------------------
say <- function(fmt, ..., .debug = DEBUG) {
  if (.debug) {
    if (missing(...)) {
      cat(fmt, "\n", sep = "")
    } else {
      cat(sprintf(fmt, ...), "\n")
    }
  }
}
stop_if <- function(cond, msg) if (isTRUE(cond)) stop(msg, call. = FALSE)
warn_if <- function(cond, msg) if (isTRUE(cond)) warning(msg, call. = FALSE)

validate_cols <- function(df, needed) {
  missing <- setdiff(needed, names(df))
  stop_if(length(missing) > 0, sprintf(
    "Input data is missing required columns: %s", paste(missing, collapse=", ")
  ))
  invisible(TRUE)
}

naics2 <- function(x) stringr::str_sub(as.character(x), 1, 2)

# [MODIFIED] A consistent sector color mapping based on descriptions
make_sector_palette <- function(sector_descriptions) {
  uniq <- sort(unique(as.character(sector_descriptions)))
  pal  <- rep(RMI_COLORS$sector, length.out = length(uniq))
  setNames(pal, uniq)
}

# ============================================================================
# CORE MATRIX FUNCTIONS (MODIFIED FOR ROBUSTNESS & MEMORY EFFICIENCY)
# ============================================================================
compute_phi <- function(M) {
  say("Computing U = t(M) %%*%% M (sparse crossprod)...")
  U_sparse <- crossprod(M)
  diagU <- diag(U_sparse)
  say("Building denominator matrix using outer()...")
  denom <- outer(diagU, diagU, pmax)
  denom[denom == 0] <- 1e-9
  say("Converting sparse U to dense for final division...")
  U_dense <- as.matrix(U_sparse)
  storage.mode(U_dense) <- "double"
  phi <- U_dense / denom
  diag(phi) <- 0
  phi[is.na(phi) | !is.finite(phi)] <- 0
  if (!isSymmetric(phi, tol = 1e-9)) {
    warn_if(TRUE, "phi is not perfectly symmetric; forcing symmetry by averaging.")
    phi <- (phi + t(phi)) / 2
    diag(phi) <- 0
  }
  if (any(phi < 0) || any(phi > 1 + 1e-9)) {
    warn_if(TRUE, "phi has values slightly outside [0,1]; clamping.")
    phi <- pmax(0, pmin(1, phi))
  }
  say("phi matrix computed. Dims: %d x %d", nrow(phi), ncol(phi))
  as.matrix(phi)
}

build_M <- function(dt) {
  say("Building sparse M (counties x industries) ...")
  counties   <- sort(unique(dt$county_geoid))
  industries <- sort(unique(dt$industry_code))
  i <- match(dt$county_geoid, counties)
  j <- match(dt$industry_code, industries)
  x <- rep(1L, nrow(dt))
  M <- sparseMatrix(i=i, j=j, x=x, dims = c(length(counties), length(industries)),
                    dimnames = list(counties, as.character(industries)))
  M
}

embed_2d <- function(sim_matrix, seed = SEED) {
  if (!is.matrix(sim_matrix)) {
    say("Warning: Input to embed_2d is not a matrix, attempting to coerce.")
    sim_matrix <- as.matrix(sim_matrix)
  }
  stop_if(nrow(sim_matrix) != ncol(sim_matrix), "Similarity matrix must be square for embedding.")
  dist_mat <- 1 - sim_matrix
  dist_mat[dist_mat < 0] <- 0
  set.seed(seed)
  tryCatch({
    say("Attempting UMAP embedding...")
    umap_res <- uwot::umap(dist_mat, n_neighbors = 15, min_dist = 0.1, metric = "precomputed", verbose = FALSE)
    colnames(umap_res) <- c("x","y")
    say("UMAP successful.")
    umap_res
  }, error = function(e) {
    say("UMAP failed with error: %s", e$message)
    say("Falling back to classical MDS (cmdscale) ...")
    dist_obj <- as.dist(dist_mat)
    coords <- tryCatch({
      cmdscale(dist_obj, k = 2, eig = FALSE)
    }, error = function(e2) {
      say("cmdscale failed with error: %s", e2$message)
      say("Returning random coordinates as a last resort.")
      matrix(rnorm(2 * nrow(dist_mat)), ncol = 2)
    })
    if (is.null(dim(coords))) {
      coords <- cbind(coords, rnorm(length(coords), sd=0.01))
    }
    colnames(coords) <- c("x","y")
    coords
  })
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
top_quantile <- function(mat, q = 0.95) {
  vals <- mat[upper.tri(mat)]
  vals <- vals[is.finite(vals) & vals > 0]
  if (length(vals) < 10) return(NA_real_)
  as.numeric(stats::quantile(vals, q, na.rm = TRUE))
}

edges_from_matrix <- function(mat, thr) {
  if (is.na(thr)) return(tibble(from=character(), to=character(), weight=numeric()))
  keep <- mat >= thr
  keep[lower.tri(keep, diag = TRUE)] <- FALSE
  if (!any(keep)) return(tibble(from=character(), to=character(), weight=numeric()))
  which_idx <- which(keep, arr.ind = TRUE)
  tibble(
    from   = colnames(mat)[which_idx[,1]],
    to     = colnames(mat)[which_idx[,2]],
    weight = mat[which_idx]
  )
}

make_network <- function(mat, nodes_meta, thr = NULL) {
  W <- mat
  if (!is.null(thr) && is.finite(thr)) {
    W[W < thr] <- 0
  }
  diag(W) <- 0
  g <- graph_from_adjacency_matrix(W, mode="undirected", weighted=TRUE, diag=FALSE)
  g <- delete_vertices(g, which(degree(g) == 0))
  if (vcount(g) == 0) return(g)
  codes <- as.numeric(names(V(g)))
  meta_subset <- nodes_meta[match(codes, nodes_meta$industry_code), ]
  V(g)$industry_code <- codes
  V(g)$label <- meta_subset$industry_desc
  V(g)$sector_desc <- as.character(meta_subset$sector_desc)
  g
}

write_csv_safely <- function(df, path) {
  tryCatch({
    readr::write_csv(df, path)
    say("Wrote file: %s", path)
    TRUE
  }, error = function(e) {
    warning(sprintf("Failed to write %s: %s", path, e$message))
    FALSE
  })
}

# ------------------------------
# LOAD DATA
# ------------------------------
say("\n=== Loading data ===")
say("Data path: %s", DATA_PATH)
stop_if(!file.exists(DATA_PATH), sprintf("Data file not found: %s", DATA_PATH))
cgt <- tryCatch({
  readr::read_csv(DATA_PATH, show_col_types = FALSE, progress = FALSE)
}, error = function(e){
  stop(sprintf("Could not read CSV at %s\n%s", DATA_PATH, e$message), call. = FALSE)
})
say("Initial rows: %s  Cols: %s", format(nrow(cgt), big.mark=","), ncol(cgt))

validate_cols(cgt, c("county_geoid","industry_code","industry_desc","M","aggregation_level","state_name"))

# Basic sanitation and [NEW] join with sector descriptions
cgt <- cgt %>%
  mutate(
    county_geoid   = as.character(county_geoid),
    industry_code  = as.numeric(industry_code),
    naics2         = naics2(industry_code)
  ) %>%
  left_join(naics_sector_map, by = "naics2")

# Subset to NAICS 6-digit + M==1 (Comparative Advantage)
ca_long <- cgt %>%
  filter(aggregation_level == 4, M == 1) %>%
  select(county_geoid, county_name, state_name, st, industry_code, industry_desc, naics2, sector_desc)

stop_if(nrow(ca_long) == 0, "No rows found where aggregation_level==4 and M==1. Check input data.")
say("Filtered to %s rows with comparative advantage (M=1)", format(nrow(ca_long), big.mark=","))

# ------------------------------
# NATIONAL MATRICES
# ------------------------------
say("\n=== Building national matrices ===")
M <- build_M(as.data.table(ca_long))
say("Sparse matrix M dims: %d counties x %d industries", nrow(M), ncol(M))

phi <- compute_phi(M)

# [MODIFIED] Lookup table now includes sector_desc
industry_lookup <- ca_long %>%
  select(industry_code, industry_desc, naics2, sector_desc) %>%
  distinct() %>%
  arrange(industry_code) %>%
  filter(!is.na(sector_desc)) # Ensure we only use industries with a mapped sector
say("Found %d unique industries with valid sector mappings.", nrow(industry_lookup))

say("\nComputing 2D embedding for national industry space ...")
coords_nat <- embed_2d(phi)
nat_scatter <- tibble(
  industry_code = as.numeric(rownames(phi)),
  x = as.numeric(coords_nat[,1]),
  y = as.numeric(coords_nat[,2])
) %>% left_join(industry_lookup, by="industry_code")

top5_nat <- top_quantile(phi, 0.95); say("National 95th percentile (φ): %s", signif(top5_nat, 4))
edges_nat_top <- edges_from_matrix(phi, top5_nat)

# ------------------------------
# IOWA SUBSET (for pre-computation example)
# ------------------------------
say("\n=== Pre-computing Iowa subset ===")
ia_codes <- ca_long %>% filter(state_name=="Iowa") %>% pull(industry_code) %>% unique() %>% as.character()
say("Found %d unique industries in Iowa.", length(ia_codes))
phi_ia <- phi[ia_codes, ia_codes, drop=FALSE]
coords_ia <- embed_2d(phi_ia)
ia_scatter <- tibble(
  industry_code = as.numeric(rownames(phi_ia)),
  x = as.numeric(coords_ia[,1]),
  y = as.numeric(coords_ia[,2])
) %>% left_join(industry_lookup, by="industry_code")
top5_ia <- top_quantile(phi_ia, 0.95); say("Iowa 95th percentile (φ): %s", signif(top5_ia, 4))

# ------------------------------
# GREATER DES MOINES (12 COUNTIES)
# ------------------------------
say("\n=== Pre-computing Greater Des Moines (12-county) subset ===")
gdm_codes <- ca_long %>% filter(county_geoid %in% GDM_COUNTIES) %>% pull(industry_code) %>% unique() %>% as.character()
say("Found %d unique industries in GDM.", length(gdm_codes))
phi_gdm <- phi[gdm_codes, gdm_codes, drop=FALSE]
coords_gdm <- embed_2d(phi_gdm)
gdm_scatter <- tibble(
  industry_code = as.numeric(rownames(phi_gdm)),
  x = as.numeric(coords_gdm[,1]),
  y = as.numeric(coords_gdm[,2])
) %>% left_join(industry_lookup, by="industry_code")
top5_gdm <- top_quantile(phi_gdm, 0.95); say("GDM 95th percentile (φ): %s", signif(top5_gdm, 4))
# [NEW] Create GDM top 5% edges for export
edges_gdm_top <- edges_from_matrix(phi_gdm, top5_gdm)

# ------------------------------
# SHARED PLOTTING
# ------------------------------
sector_palette <- make_sector_palette(industry_lookup$sector_desc)

# [MODIFIED] Plotting function now uses sector_desc
gg_scatter <- function(df, title) {
  validate(need(nrow(df) > 0, "No industries to plot."))
  ggplot(df, aes(x, y, color = sector_desc, label = paste0(industry_code, " - ", industry_desc))) +
    geom_point(size=2.5, alpha=0.9) +
    geom_text_repel(
      data = df %>% slice_sample(n = min(80, n())),
      size = 2.7, family = "sans", max.overlaps = 30, box.padding = 0.4, point.padding = 0.2
    ) +
    scale_color_manual(values = sector_palette, guide = guide_legend(title = "Sector")) +
    labs(title = title, x = "Dimension 1", y = "Dimension 2", caption = "Layout: UMAP on φ proximity (1−φ distance)") +
    theme_minimal(base_family = "sans") +
    theme(
      plot.title.position = "plot",
      plot.title = element_text(face = "bold", size = 16, color = RMI_COLORS$blue_spruce),
      plot.caption = element_text(size = 9, color = "grey30"),
      legend.position = "right",
      panel.grid.minor = element_blank()
    )
}

to_plotly <- function(gg) {
  ggplotly(gg, tooltip = c("label")) %>%
    layout(legend = list(title = list(text = "<b>Sector</b>")))
}

# ------------------------------
# SHINY APP
# ------------------------------
say("\n=== Launching Shiny app ===")

app_ui <- function() {
  bslib::page_navbar(
    title = NULL,
    theme = bslib::bs_theme(
      base_font = bslib::font_google("Roboto"),
      heading_font = bslib::font_google("Roboto Condensed"),
      "navbar-bg" = RMI_COLORS$blue_spruce,
      "navbar-brand-color" = RMI_COLORS$white,
      "navbar-dark-color" = RMI_COLORS$white,
      primary = RMI_COLORS$energy
    ),
    header = tags$div(
      class = "d-flex align-items-center justify-content-between px-3 py-2",
      tags$img(src = RMI_LOGO_URL, height = 36, alt = "RMI"),
      tags$div()
    ),
    sidebar = bslib::sidebar(
      title = "Filters",
      width = 350,
      open = TRUE,
      tags$h5("Scope"),
      radioGroupButtons(
        inputId = "scope",
        label = NULL,
        choices = c("National","State","12-County (Greater Des Moines)"),
        justified = TRUE, status = "primary"
      ),
      uiOutput("state_ui"),
      # [MODIFIED] Picker now uses sector descriptions
      pickerInput(
        inputId = "sector",
        label = "Sector(s)",
        choices = levels(naics_sector_map$sector_desc),
        options = list(`actions-box` = TRUE, `live-search` = TRUE),
        multiple = TRUE
      ),
      uiOutput("county_ui"),
      hr(),
      sliderInput("edge_thr", "Edge threshold (φ) for network", min = 0, max = 1, value = 0.05, step = 0.01),
      switchInput("use_top5", "Use Top 5% threshold", value = FALSE, onLabel = "Top 5%", offLabel = "Custom φ"),
      hr(),
      downloadButton("dl_scatter", "Download Scatter (CSV)"),
      downloadButton("dl_edges",   "Download Edges (CSV)"),
      br(), br(),
      helpText("Colors follow RMI brand palette. Tooltip shows industry code and description.")
    ),
    nav_panel(
      "Explore",
      layout_column_wrap(
        width = 1/2,
        card(full_screen = TRUE, card_header("Industry Space (scatter)"),
             withSpinner(plotlyOutput("scatter_pl"), color = RMI_COLORS$energy)),
        card(full_screen = TRUE, card_header("Co-location Network"),
             withSpinner(visNetworkOutput("net_pl", height = "650px"), color = RMI_COLORS$energy))
      )
    ),
    nav_panel(
      "Table",
      card(full_screen = TRUE, card_header("Industries in scope"),
           withSpinner(DTOutput("tbl_industries"), color = RMI_COLORS$energy))
    ),
    footer = tags$div(
      class = "text-center p-3",
      style = glue("background:{RMI_COLORS$blue_spruce}; color:{RMI_COLORS$white};"),
      HTML("&copy; RMI — Energy. Transformed.")
    )
  )
}

app_server <- function(input, output, session) {
  
  output$state_ui <- renderUI({
    if (input$scope == "State") {
      pickerInput("state", "State", choices = sort(unique(ca_long$state_name)),
                  selected = "Iowa", options = list(`live-search` = TRUE))
    }
  })
  
  output$county_ui <- renderUI({
    if (input$scope == "12-County (Greater Des Moines)") {
      gdm_names <- ca_long %>% filter(county_geoid %in% GDM_COUNTIES) %>%
        distinct(county_geoid, county_name) %>% arrange(county_name)
      pickerInput("counties", "Select counties (optional)",
                  choices = setNames(gdm_names$county_geoid, gdm_names$county_name),
                  multiple = TRUE, options = list(`actions-box`=TRUE, `live-search`=TRUE))
    } else if (input$scope == "State") {
      req(input$state)
      st_counties <- ca_long %>% filter(state_name %in% input$state) %>%
        distinct(county_geoid, county_name) %>% arrange(county_name)
      pickerInput("counties", "Select counties (optional)",
                  choices = setNames(st_counties$county_geoid, st_counties$county_name),
                  multiple = TRUE, options = list(`actions-box`=TRUE, `live-search`=TRUE))
    }
  })
  
  rv_scope <- reactive({
    validate(need(input$scope %in% c("National","State","12-County (Greater Des Moines)"), "Invalid scope"))
    scope <- input$scope
    if (scope == "National") {
      list(phi = phi, scatter = nat_scatter, top5 = top5_nat)
    } else if (scope == "State") {
      req(input$state)
      rows <- ca_long %>% filter(state_name %in% input$state)
      if (!is.null(input$counties) && length(input$counties) > 0) {
        rows <- rows %>% filter(county_geoid %in% input$counties)
      }
      codes <- sort(unique(rows$industry_code)) %>% as.character()
      if (length(codes) < 2) return(NULL)
      mat <- tryCatch(phi[codes, codes, drop=FALSE], error = function(e) NULL)
      emb <- embed_2d(mat)
      sc  <- tibble(industry_code = as.numeric(rownames(mat)), x = emb[,1], y = emb[,2]) %>%
        left_join(industry_lookup, by="industry_code")
      list(phi = mat, scatter = sc, top5 = top_quantile(mat, 0.95))
    } else { # GDM
      rows <- ca_long %>% filter(county_geoid %in% GDM_COUNTIES)
      if (!is.null(input$counties) && length(input$counties) > 0) {
        rows <- rows %>% filter(county_geoid %in% input$counties)
      }
      codes <- sort(unique(rows$industry_code)) %>% as.character()
      if (length(codes) < 2) return(NULL)
      mat <- tryCatch(phi[codes, codes, drop=FALSE], error = function(e) NULL)
      emb <- embed_2d(mat)
      sc  <- tibble(industry_code = as.numeric(rownames(mat)), x = emb[,1], y = emb[,2]) %>%
        left_join(industry_lookup, by="industry_code")
      list(phi = mat, scatter = sc, top5 = top_quantile(mat, 0.95))
    }
  })
  
  # [MODIFIED] Filter logic now uses sector_desc
  scope_filtered <- reactive({
    sc <- rv_scope()
    validate(need(!is.null(sc), "No data in current scope. Please broaden your selection."))
    df <- sc$scatter
    mat <- sc$phi
    if (!is.null(input$sector) && length(input$sector) > 0) {
      keep_codes <- df %>% filter(sector_desc %in% input$sector) %>% pull(industry_code) %>% unique()
      df  <- df  %>% filter(industry_code %in% keep_codes)
      if (length(keep_codes) >= 2) {
        rc  <- as.character(sort(keep_codes))
        mat <- mat[rc, rc, drop=FALSE]
      } else {
        mat <- matrix(0, nrow=0, ncol=0)
      }
    }
    list(scatter = df, phi = mat, top5 = sc$top5)
  })
  
  output$scatter_pl <- renderPlotly({
    sc <- scope_filtered()$scatter
    validate(need(nrow(sc) > 0, "No industries remain after applying filters."))
    p <- gg_scatter(sc, title = paste("Industry Space —", input$scope))
    to_plotly(p)
  })
  
  # [MODIFIED] Network now uses sector_desc
  output$net_pl <- renderVisNetwork({
    s <- scope_filtered()
    mat <- s$phi
    validate(need(nrow(mat) > 1, "Not enough industries to build a network."))
    thr <- if (isTRUE(input$use_top5)) s$top5 else input$edge_thr
    thr <- ifelse(is.na(thr), input$edge_thr, thr)
    g <- make_network(mat, industry_lookup, thr = thr)
    validate(need(vcount(g) > 0, "No edges exist at the current threshold. Try lowering it."))
    
    nodes <- get.data.frame(g, "vertices") %>%
      transmute(
        id = industry_code,
        label = label,
        sector_desc = sector_desc,
        color = sector_palette[sector_desc],
        title = paste0("<b>", label, "</b><br>NAICS 6: ", id, "<br>Sector: ", sector_desc)
      )
    
    edges <- as_data_frame(g, "edges") %>%
      transmute(from = from, to = to, value = pmax(1, weight * 100),
                title = paste0("φ = ", signif(weight, 4)))
    
    visNetwork(nodes, edges, main = paste0("Co-location Network — ", input$scope)) %>%
      visEdges(smooth = list(enabled=TRUE, type="continuous")) %>%
      visOptions(
        highlightNearest = list(enabled = TRUE, hover = TRUE, degree = 1),
        nodesIdSelection = list(enabled = TRUE),
        selectedBy = list(variable = "sector_desc", main = "Filter by Sector")
      ) %>%
      visLegend(useGroups = FALSE, addNodes = nodes %>%
                  distinct(sector_desc, .keep_all = TRUE) %>%
                  transmute(label = sector_desc, color = color) %>%
                  rename(title=label)) %>%
      visPhysics(solver = "forceAtlas2Based", stabilization = list(enabled=TRUE, iterations = 300)) %>%
      visLayout(randomSeed = SEED)
  })
  
  output$tbl_industries <- renderDT({
    sc <- scope_filtered()$scatter %>%
      arrange(sector_desc, industry_code) %>%
      mutate(x = round(x, 3), y = round(y, 3)) %>%
      select(industry_code, industry_desc, sector_desc, x, y)
    datatable(sc, rownames = FALSE, filter = "top", options = list(pageLength = 25))
  })
  
  output$dl_scatter <- downloadHandler(
    filename = function() paste0("industry_scatter_", gsub("\\s","_", tolower(input$scope)), ".csv"),
    content = function(file) {
      sc <- scope_filtered()$scatter %>% arrange(industry_code)
      write_csv(sc, file)
    }
  )
  
  output$dl_edges <- downloadHandler(
    filename = function() paste0("edges_", gsub("\\s","_", tolower(input$scope)), ".csv"),
    content = function(file) {
      s <- scope_filtered()
      mat <- s$phi
      if (nrow(mat) <= 1) {
        write_csv(tibble(from=character(), to=character(), weight=numeric()), file)
      } else {
        thr <- if (isTRUE(input$use_top5)) s$top5 else input$edge_thr
        df  <- edges_from_matrix(mat, thr)
        write_csv(df, file)
      }
    }
  )
}

# ------------------------------
# OPTIONAL: Write out CSVs for offline use
# ------------------------------
invisible({
  say("\n=== Writing pre-computed CSVs for offline use ===")
  write_csv_safely(nat_scatter, "national_scatter_plot_data.csv")
  if (!is.na(top5_nat)) {
    write_csv_safely(edges_nat_top, "national_colocation_pairs_top5percent.csv")
  }
  write_csv_safely(ia_scatter,   "iowa_scatter_plot_data.csv")
  write_csv_safely(gdm_scatter,  "greater_des_moines_scatter_plot_data.csv")
  # [NEW] Export GDM top 5% edges
  if (!is.na(top5_gdm)) {
    write_csv_safely(edges_gdm_top, "greater_des_moines_colocation_pairs_top5percent.csv")
  }
})

# ------------------------------
# RUN APP
# ------------------------------
if (interactive()) {
  shinyApp(ui = app_ui(), server = app_server)
} else {
  shinyApp(ui = app_ui(), server = app_server)
}
