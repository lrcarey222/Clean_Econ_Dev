# LIGHTCAST EMPLOYMENT ANALYSIS - DABOIN METHOD v2.2
# Economic complexity methodology from Daboin et al.

library(tidyverse); library(tigris); library(readr); library(stringr); library(sf)
library(censusapi); library(readxl); library(curl); library(igraph); library(ggraph)
library(Matrix); library(parallel); library(proxy); library(arrow); library(sfarrow)
library(httr); library(jsonlite); library(base64enc); library(data.table)
options(tigris_use_cache = FALSE)

# CONFIG
get_os <- function() {
  sysinf <- Sys.info()
  if (!is.null(sysinf)) {
    os <- sysinf["sysname"]
    if (os == "Darwin") return("mac")
    if (os == "Windows") return("windows")
    if (os == "Linux") return("linux")
  }
  if (.Platform$OS.type == "windows") return("windows")
  return("unix")
}

find_onedrive_path <- function() {
  paths <- if (get_os() == "windows") {
    up <- Sys.getenv("USERPROFILE")
    c(file.path(up, "OneDrive-RMI"), file.path(up, "OneDrive - RMI"))
  } else {
    hd <- Sys.getenv("HOME")
    c(file.path(hd, "Library", "CloudStorage", "OneDrive-RMI"),
      file.path(hd, "Library", "CloudStorage", "OneDrive - RMI"),
      file.path(hd, "OneDrive-RMI"), file.path(hd, "OneDrive - RMI"))
  }
  for (p in paths) if (dir.exists(p)) return(p)
  stop("OneDrive path not found")
}

onedrive_base <- find_onedrive_path()
data_folder <- file.path(onedrive_base, "US Program - Documents", "6_Projects",
                         "Clean Regional Economic Development", "ACRE", "Data", "Raw Data")
lightcast_file <- file.path(data_folder, "LIGHTCAST_2024_EMPLOYMENT_COUNTY_6DNAICS_GEOID.csv")

n_cores <- max(1, detectCores() - 1)
VERBOSITY <- 1

# Parallel processing setup - detect if safe to use mclapply
# mclapply doesn't work well in RStudio on macOS due to forking issues
is_rstudio <- Sys.getenv("RSTUDIO") == "1"
is_macos <- Sys.info()["sysname"] == "Darwin"
use_parallel <- n_cores > 1 && .Platform$OS.type != "windows" && !(is_rstudio && is_macos)

# Safe parallel lapply wrapper
parallel_lapply <- function(X, FUN, ...) {
  if (use_parallel) {
    mclapply(X, FUN, ..., mc.cores = min(n_cores, length(X)))
  } else {
    lapply(X, FUN, ...)
  }
}

log_msg <- function(..., level = 1) { if (VERBOSITY >= level) cat(...) }
log_step <- function(n, name) { if (VERBOSITY >= 1) cat("\n[Step ", n, "] ", name, "\n", sep = "") }
log_detail <- function(...) log_msg(..., level = 2)
log_debug <- function(...) log_msg(..., level = 3)
data_summary <- function(df, name = "Data", show_glimpse = FALSE) {
  if (VERBOSITY >= 1) cat("  ", name, ": ", format(nrow(df), big.mark = ","), " rows x ", ncol(df), " cols\n", sep = "")
  if (VERBOSITY >= 2 && show_glimpse) glimpse(df)
  invisible(df)
}

# Compact diagnostic printing utilities
print_divider <- function(char = "─", width = 70) cat(paste(rep(char, width), collapse = ""), "\n")
print_header <- function(title) { print_divider(); cat(" ", title, "\n"); print_divider() }

# Single-line key-value pairs
kv <- function(key, value, width = 20) {
  paste0(format(paste0(key, ":"), width = width), value)
}

# Compact table row
trow <- function(..., widths = NULL) {
  args <- list(...)
  if (is.null(widths)) widths <- rep(12, length(args))
  paste(mapply(function(x, w) format(as.character(x), width = w), args, widths), collapse = " ")
}

# Progress indicator for loops
progress_pct <- function(i, n, width = 20) {
  pct <- floor(100 * i / n)
  filled <- floor(width * i / n)
  bar <- paste0("[", paste(rep("█", filled), collapse = ""), paste(rep("░", width - filled), collapse = ""), "]")
  cat("\r  ", bar, " ", pct, "%", sep = "")
  if (i == n) cat("\n")
}

# Compact multi-column summary
print_summary_row <- function(label, values, fmt = "%.2f") {
  formatted <- paste(sapply(values, function(v) sprintf(fmt, v)), collapse = " | ")
  cat("  ", format(label, width = 25), formatted, "\n", sep = "")
}

cat("Configuration: Cores =", n_cores, "| Parallel =", if (use_parallel) "enabled" else "disabled (RStudio/macOS)", 
    "| Verbosity =", VERBOSITY, "\n")

# HELPER FUNCTIONS
stress_test <- function(test_name, condition, details = NULL, stop_on_fail = TRUE) {
  if (condition) { if (VERBOSITY >= 3) cat("  ✓", test_name, "\n"); return(invisible(TRUE)) }
  cat("  ✗ FAIL:", test_name, "\n")
  if (stop_on_fail) stop(paste("Stress test failed:", test_name))
  invisible(FALSE)
}
approx_equal <- function(a, b, tol = 1e-10) abs(a - b) < tol

compute_proximity_matrix_vectorized <- function(M_matrix) {
  U_matrix <- crossprod(M_matrix)
  diag_U <- diag(U_matrix)
  max_mat <- pmax(outer(diag_U, rep(1, length(diag_U))), outer(rep(1, length(diag_U)), diag_U))
  phi_matrix <- U_matrix / max_mat
  phi_matrix[is.nan(phi_matrix)] <- 0
  list(U = U_matrix, phi = phi_matrix)
}

create_edge_list_vectorized <- function(phi_matrix, ind_codes, top_pct = 0.05) {
  upper_tri_idx <- which(upper.tri(phi_matrix), arr.ind = TRUE)
  weights <- phi_matrix[upper_tri_idx]
  edge_list <- data.frame(from = ind_codes[upper_tri_idx[, 1]], to = ind_codes[upper_tri_idx[, 2]],
                          weight = weights, stringsAsFactors = FALSE)
  edge_list <- edge_list[edge_list$weight > 0, ]
  n_top <- ceiling(nrow(edge_list) * top_pct)
  threshold <- sort(edge_list$weight, decreasing = TRUE)[min(n_top, nrow(edge_list))]
  top_edges <- edge_list[edge_list$weight >= threshold, ]
  strongest_from <- edge_list %>% group_by(from) %>% slice_max(weight, n = 1, with_ties = FALSE) %>% ungroup()
  strongest_to <- edge_list %>% group_by(to) %>% slice_max(weight, n = 1, with_ties = FALSE) %>% ungroup()
  bind_rows(top_edges, strongest_from, strongest_to) %>% distinct(from, to, .keep_all = TRUE)
}

calculate_strategic_gain_vectorized <- function(M_matrix, phi_matrix, density_matrix, ici, phi_col_sums) {
  n_geos <- nrow(M_matrix); n_inds <- ncol(M_matrix)
  phi_norm <- t(t(phi_matrix) / phi_col_sums)
  absence_matrix <- 1 - M_matrix
  ici_mat <- matrix(ici, nrow = n_geos, ncol = n_inds, byrow = TRUE)
  weighted_absence <- absence_matrix * ici_mat
  first_term <- weighted_absence %*% t(phi_norm)
  second_term <- density_matrix * ici_mat
  first_term - second_term
}

estimate_memory_gb <- function(n_rows, n_cols, bytes = 8) (n_rows * n_cols * bytes) / (1024^3)
get_available_memory_gb <- function() {
  tryCatch({
    if (.Platform$OS.type == "unix") {
      mem_info <- system("free -b 2>/dev/null || vm_stat 2>/dev/null", intern = TRUE)
      if (length(mem_info) > 0 && grepl("free", mem_info[1], ignore.case = TRUE)) {
        return(as.numeric(strsplit(mem_info[2], "\\s+")[[1]][7]) / (1024^3))
      }
    }
    8
  }, error = function(e) 8)
}

calculate_chunk_size <- function(total_size, mem_per_elem_gb, max_frac = 0.5) {
  avail <- get_available_memory_gb() * max_frac
  max(100, min(floor(sqrt(avail / mem_per_elem_gb)), total_size))
}

compute_proximity_matrix_chunked <- function(M_matrix, chunk_size = NULL, verbose = TRUE) {
  n_inds <- ncol(M_matrix); n_geos <- nrow(M_matrix)
  if (is.null(chunk_size)) chunk_size <- calculate_chunk_size(n_inds, (8 * 3) / (1024^3), 0.3)
  if (n_inds <= chunk_size) return(compute_proximity_matrix_vectorized(M_matrix))
  
  U_matrix <- matrix(0, nrow = n_inds, ncol = n_inds)
  n_chunks <- ceiling(n_inds / chunk_size)
  for (i_chunk in 1:n_chunks) {
    i_start <- (i_chunk - 1) * chunk_size + 1; i_end <- min(i_chunk * chunk_size, n_inds)
    for (j_chunk in i_chunk:n_chunks) {
      j_start <- (j_chunk - 1) * chunk_size + 1; j_end <- min(j_chunk * chunk_size, n_inds)
      U_chunk <- crossprod(M_matrix[, i_start:i_end, drop = FALSE], M_matrix[, j_start:j_end, drop = FALSE])
      U_matrix[i_start:i_end, j_start:j_end] <- U_chunk
      if (i_chunk != j_chunk) U_matrix[j_start:j_end, i_start:i_end] <- t(U_chunk)
    }
  }
  diag_U <- diag(U_matrix)
  phi_matrix <- matrix(0, nrow = n_inds, ncol = n_inds)
  for (i_chunk in 1:n_chunks) {
    i_start <- (i_chunk - 1) * chunk_size + 1; i_end <- min(i_chunk * chunk_size, n_inds); i_range <- i_start:i_end
    for (j_chunk in i_chunk:n_chunks) {
      j_start <- (j_chunk - 1) * chunk_size + 1; j_end <- min(j_chunk * chunk_size, n_inds); j_range <- j_start:j_end
      max_chunk <- pmax(outer(diag_U[i_range], rep(1, length(j_range))), outer(rep(1, length(i_range)), diag_U[j_range]))
      phi_chunk <- U_matrix[i_range, j_range] / max_chunk; phi_chunk[is.nan(phi_chunk)] <- 0
      phi_matrix[i_range, j_range] <- phi_chunk
      if (i_chunk != j_chunk) phi_matrix[j_range, i_range] <- t(phi_chunk)
    }
  }
  list(U = U_matrix, phi = phi_matrix)
}

compute_M_tilde_chunked <- function(M_matrix, row_scale, col_scale, chunk_size = NULL, verbose = TRUE) {
  n_rows <- nrow(M_matrix); out_size <- n_rows
  if (is.null(chunk_size)) chunk_size <- calculate_chunk_size(out_size, (8 * 2) / (1024^3), 0.3)
  if (out_size <= chunk_size) {
    M_col_scaled <- t(t(M_matrix) / col_scale)
    return((M_col_scaled / row_scale) %*% t(M_matrix))
  }
  M_tilde <- matrix(0, nrow = out_size, ncol = out_size)
  n_chunks <- ceiling(out_size / chunk_size)
  for (i_chunk in 1:n_chunks) {
    i_start <- (i_chunk - 1) * chunk_size + 1; i_end <- min(i_chunk * chunk_size, out_size); i_range <- i_start:i_end
    M_chunk <- M_matrix[i_range, , drop = FALSE]
    M_col_scaled_chunk <- t(t(M_chunk) / col_scale)
    M_row_scaled_chunk <- M_col_scaled_chunk / row_scale[i_range]
    M_tilde[i_range, ] <- M_row_scaled_chunk %*% t(M_matrix)
  }
  M_tilde
}

create_edge_list_chunked <- function(phi_matrix, ind_codes, top_pct = 0.05, chunk_size = 5000, verbose = TRUE) {
  n_inds <- nrow(phi_matrix)
  if (n_inds <= chunk_size) return(create_edge_list_vectorized(phi_matrix, ind_codes, top_pct))
  edge_list_chunks <- list(); chunk_idx <- 1; n_chunks <- ceiling(n_inds / chunk_size)
  for (i_chunk in 1:n_chunks) {
    i_start <- (i_chunk - 1) * chunk_size + 1; i_end <- min(i_chunk * chunk_size, n_inds); i_range <- i_start:i_end
    for (j_chunk in i_chunk:n_chunks) {
      j_start <- (j_chunk - 1) * chunk_size + 1; j_end <- min(j_chunk * chunk_size, n_inds); j_range <- j_start:j_end
      phi_chunk <- phi_matrix[i_range, j_range, drop = FALSE]
      if (i_chunk == j_chunk) {
        tri_idx <- which(upper.tri(phi_chunk), arr.ind = TRUE)
        weights <- phi_chunk[tri_idx]; from_idx <- i_range[tri_idx[, 1]]; to_idx <- j_range[tri_idx[, 2]]
      } else {
        all_idx <- expand.grid(row = seq_along(i_range), col = seq_along(j_range))
        weights <- phi_chunk[as.matrix(all_idx)]; from_idx <- i_range[all_idx$row]; to_idx <- j_range[all_idx$col]
      }
      nonzero <- weights > 0
      if (any(nonzero)) {
        edge_list_chunks[[chunk_idx]] <- data.frame(from = ind_codes[from_idx[nonzero]], to = ind_codes[to_idx[nonzero]],
                                                    weight = weights[nonzero], stringsAsFactors = FALSE)
        chunk_idx <- chunk_idx + 1
      }
    }
  }
  edge_list <- bind_rows(edge_list_chunks)
  n_top <- ceiling(nrow(edge_list) * top_pct)
  threshold <- sort(edge_list$weight, decreasing = TRUE)[min(n_top, nrow(edge_list))]
  top_edges <- edge_list[edge_list$weight >= threshold, ]
  strongest_from <- edge_list %>% group_by(from) %>% slice_max(weight, n = 1, with_ties = FALSE) %>% ungroup()
  strongest_to <- edge_list %>% group_by(to) %>% slice_max(weight, n = 1, with_ties = FALSE) %>% ungroup()
  bind_rows(top_edges, strongest_from, strongest_to) %>% distinct(from, to, .keep_all = TRUE)
}

calculate_strategic_gain_chunked <- function(M_matrix, phi_matrix, density_matrix, ici, phi_col_sums, chunk_size = 500, verbose = TRUE) {
  n_geos <- nrow(M_matrix); n_inds <- ncol(M_matrix)
  if (n_geos <= chunk_size) return(calculate_strategic_gain_vectorized(M_matrix, phi_matrix, density_matrix, ici, phi_col_sums))
  phi_norm <- t(t(phi_matrix) / phi_col_sums)
  strategic_gain <- matrix(0, nrow = n_geos, ncol = n_inds)
  n_chunks <- ceiling(n_geos / chunk_size)
  for (i_chunk in 1:n_chunks) {
    i_start <- (i_chunk - 1) * chunk_size + 1; i_end <- min(i_chunk * chunk_size, n_geos); i_range <- i_start:i_end
    chunk_n <- length(i_range)
    M_chunk <- M_matrix[i_range, , drop = FALSE]; density_chunk <- density_matrix[i_range, , drop = FALSE]
    absence_chunk <- 1 - M_chunk
    ici_mat_chunk <- matrix(ici, nrow = chunk_n, ncol = n_inds, byrow = TRUE)
    first_term_chunk <- (absence_chunk * ici_mat_chunk) %*% t(phi_norm)
    strategic_gain[i_range, ] <- first_term_chunk - density_chunk * ici_mat_chunk
  }
  strategic_gain
}

auto_chunk_method <- function(method_name, n_elements, size_threshold = 5000, use_chunked = NULL, verbose = TRUE) {
  if (is.null(use_chunked)) {
    mem_est <- estimate_memory_gb(n_elements, n_elements, 8)
    use_chunked <- (n_elements > size_threshold) || (mem_est > get_available_memory_gb() * 0.3)
  }
  use_chunked
}

# COMPLEXITY CALCULATION
# Implements Daboin et al. "Economic Complexity and Technological Relatedness: Findings for American Cities"
# Key equations:
#   Eq 4-5: RCA_ci = (X_ci/X_c)/(X_i/X); M_ci = 1[RCA >= 1]
#   Eq 6-7: Diversity_c = sum_i(M_ci); Ubiquity_i = sum_c(M_ci)
#   Eq 8-9: ECI/ICI from second eigenvector of M_tilde matrices
#   Eq 15-16: U_{i,i'} = M^T * M; phi_{i,i'} = U_{i,i'} / max(U_{i,i}, U_{i',i'})
#   Eq 19: density_{c,i'} = sum_i(M_{c,i} * phi_{i,i'}) / sum_i(phi_{i,i'})
#   Eq 21: SI_c = sum_i(d_{c,i} * (1-M_{c,i}) * ICI_i)
#   Eq 22: SG_{c,i} = [sum_{i'}(phi_{i,i'}/colsum) * (1-M_{c,i'}) * ICI_{i'}] - d_{c,i}*ICI_i
calculate_complexity_metrics <- function(empshare_lq_data, geo_level_name = "geography",
                                         run_stress_tests = TRUE, create_industry_space = TRUE) {
  log_msg("\n[Complexity] ", toupper(geo_level_name), "\n")
  empshare_lq_data <- empshare_lq_data %>% mutate(industry_lq = replace_na(industry_lq, 0))
  n_geos_input <- n_distinct(empshare_lq_data$geoid); n_inds_input <- n_distinct(empshare_lq_data$industry_code)
  log_msg("  Input: ", n_geos_input, " geos x ", n_inds_input, " industries\n")
  
  if (run_stress_tests) {
    stress_test("No NA in LQ", sum(is.na(empshare_lq_data$industry_lq)) == 0)
    stress_test("LQ >= 0", all(empshare_lq_data$industry_lq >= 0))
    stress_test("Complete matrix", nrow(empshare_lq_data) == n_geos_input * n_inds_input)
  }
  
  M_data <- empshare_lq_data %>% mutate(M_ci = as.integer(industry_lq >= 1)) %>% select(geoid, industry_code, M_ci)  # Eq. 5: M_ci = 1[RCA >= 1]
  n_specializations <- sum(M_data$M_ci); fill_rate <- 100 * n_specializations / nrow(M_data)
  log_msg("  M_ci: ", round(fill_rate, 1), "% fill rate (", n_specializations, " specializations)\n")
  
  if (run_stress_tests) {
    stress_test("M_ci binary", all(M_data$M_ci %in% c(0, 1)))
    stress_test("Fill rate reasonable", fill_rate > 5 && fill_rate < 50)
  }
  
  diversity <- M_data %>% group_by(geoid) %>% summarise(diversity = sum(M_ci), .groups = "drop")
  ubiquity <- M_data %>% group_by(industry_code) %>% summarise(ubiquity = sum(M_ci), .groups = "drop")
  
  if (run_stress_tests) {
    stress_test("Diversity >= 0", all(diversity$diversity >= 0))
    stress_test("Sum(div)==Sum(ubiq)", sum(diversity$diversity) == sum(ubiquity$ubiquity))
  }
  
  valid_geos <- diversity %>% filter(diversity > 0) %>% pull(geoid)
  valid_inds <- ubiquity %>% filter(ubiquity > 0) %>% pull(industry_code)
  M_filtered <- M_data %>% filter(geoid %in% valid_geos, industry_code %in% valid_inds)
  
  M_wide <- M_filtered %>% pivot_wider(names_from = industry_code, values_from = M_ci, values_fill = 0)
  geo_ids <- M_wide$geoid; ind_codes <- colnames(M_wide)[-1]
  M_matrix <- as.matrix(M_wide %>% select(-geoid)); rownames(M_matrix) <- geo_ids; colnames(M_matrix) <- ind_codes
  n_geos <- nrow(M_matrix); n_inds <- ncol(M_matrix)
  log_msg("  M matrix: ", n_geos, " x ", n_inds, " (density: ", round(sum(M_matrix) / (n_geos * n_inds), 3), ")\n")
  
  diversity_vec <- diversity %>% filter(geoid %in% geo_ids) %>% arrange(match(geoid, geo_ids)) %>% pull(diversity)
  ubiquity_vec <- ubiquity %>% filter(industry_code %in% ind_codes) %>% arrange(match(industry_code, ind_codes)) %>% pull(ubiquity)
  
  if (run_stress_tests) {
    stress_test("Row sums = diversity", all(rowSums(M_matrix) == diversity_vec))
    stress_test("Col sums = ubiquity", all(colSums(M_matrix) == ubiquity_vec))
  }
  
  K_c1 <- (M_matrix %*% ubiquity_vec) / diversity_vec
  K_i1 <- (t(M_matrix) %*% diversity_vec) / ubiquity_vec
  
  # M_tilde_C and ECI
  use_chunked_MC <- auto_chunk_method("M_tilde_C", n_geos, 3000)
  if (use_chunked_MC) {
    M_tilde_C <- compute_M_tilde_chunked(M_matrix, diversity_vec, ubiquity_vec)
  } else {
    M_col_scaled <- t(t(M_matrix) / ubiquity_vec)
    M_tilde_C <- (M_col_scaled / diversity_vec) %*% t(M_matrix)
  }
  
  if (run_stress_tests) {
    stress_test("M̃^C rows sum to 1", all(approx_equal(rowSums(M_tilde_C), 1)))
    stress_test("M̃^C non-negative", all(M_tilde_C >= 0))
  }
  
  eigen_C <- eigen(M_tilde_C, symmetric = FALSE)
  vals_C <- Re(eigen_C$values); ord_C <- order(vals_C, decreasing = TRUE)
  first_eval_C <- vals_C[ord_C[1]]; second_eval_C <- vals_C[ord_C[2]]; third_eval_C <- vals_C[ord_C[3]]
  eci_raw <- Re(eigen_C$vectors[, ord_C[2]])
  
  if (run_stress_tests) stress_test("λ1 ≈ 1", approx_equal(first_eval_C, 1, tol = 1e-6))
  
  # M_tilde_I and ICI
  use_chunked_MI <- auto_chunk_method("M_tilde_I", n_inds, 2000)
  if (use_chunked_MI) {
    M_tilde_I <- compute_M_tilde_chunked(t(M_matrix), ubiquity_vec, diversity_vec)
  } else {
    M_row_scaled_for_I <- M_matrix / diversity_vec
    M_T_Dinv <- t(M_row_scaled_for_I)
    M_tilde_I <- (M_T_Dinv / ubiquity_vec) %*% M_matrix
  }
  
  if (run_stress_tests) stress_test("M̃^I rows sum to 1", all(approx_equal(rowSums(M_tilde_I), 1)))
  
  eigen_I <- eigen(M_tilde_I, symmetric = FALSE)
  vals_I <- Re(eigen_I$values); ord_I <- order(vals_I, decreasing = TRUE)
  first_eval_I <- vals_I[ord_I[1]]; second_eval_I <- vals_I[ord_I[2]]; third_eval_I <- vals_I[ord_I[3]]
  ici_raw <- Re(eigen_I$vectors[, ord_I[2]])
  
  if (run_stress_tests) stress_test("Shared λ2", approx_equal(second_eval_C, second_eval_I, tol = 1e-6))
  
  # Sign anchoring - per Daboin et al:
  # - ICI should be negatively correlated with ubiquity (complex industries are less ubiquitous)
  # - ECI should be positively correlated with diversity (complex places are more diverse)
  ici_ubiq_corr_raw <- cor(ici_raw, ubiquity_vec)
  ici_sign_flipped <- ici_ubiq_corr_raw > 0
  if (ici_sign_flipped) ici_raw <- -ici_raw
  
  eci_div_corr_raw <- cor(eci_raw, diversity_vec)
  eci_sign_flipped <- eci_div_corr_raw < 0
  if (eci_sign_flipped) eci_raw <- -eci_raw
  
  eci <- (eci_raw - mean(eci_raw)) / sd(eci_raw)
  ici <- (ici_raw - mean(ici_raw)) / sd(ici_raw)
  
  if (run_stress_tests) {
    stress_test("ICI neg corr with ubiquity", cor(ici, ubiquity_vec) < 0)
    stress_test("ECI pos corr with diversity", cor(eci, diversity_vec) > 0)
  }
  
  avg_ici_by_geo <- vapply(seq_len(n_geos), function(c_idx) {
    specs <- which(M_matrix[c_idx, ] == 1); if (length(specs) == 0) NA_real_ else mean(ici[specs])
  }, numeric(1))
  eci_avgici_corr <- cor(eci, avg_ici_by_geo, use = "complete.obs")
  if (run_stress_tests) stress_test("|cor(ECI, avgICI)| > 0.99", abs(eci_avgici_corr) > 0.99)
  
  # Proximity and industry space
  proximity_results <- NULL
  if (create_industry_space) {
    use_chunked <- auto_chunk_method("proximity matrix", n_inds, 2000)
    prox_result <- if (use_chunked) compute_proximity_matrix_chunked(M_matrix) else compute_proximity_matrix_vectorized(M_matrix)
    U_matrix <- prox_result$U; phi_matrix <- prox_result$phi
    rownames(phi_matrix) <- colnames(phi_matrix) <- rownames(U_matrix) <- colnames(U_matrix) <- ind_codes
    
    if (run_stress_tests) {
      stress_test("φ symmetric", isSymmetric(phi_matrix))
      stress_test("φ diag = 1", all(approx_equal(diag(phi_matrix), 1)))
    }
    
    phi_row_sums <- rowSums(phi_matrix); phi_total_sum <- sum(phi_matrix)
    centrality_implicit <- phi_row_sums / phi_total_sum
    phi_col_sums <- colSums(phi_matrix)
    density_matrix <- (M_matrix %*% phi_matrix) / matrix(phi_col_sums, nrow = n_geos, ncol = n_inds, byrow = TRUE)
    rownames(density_matrix) <- geo_ids; colnames(density_matrix) <- ind_codes
    
    if (run_stress_tests) stress_test("Density in [0,1]", all(density_matrix >= 0 & density_matrix <= 1))
    
    ici_matrix <- matrix(ici, nrow = n_geos, ncol = n_inds, byrow = TRUE)
    absence_matrix <- 1 - M_matrix
    strategic_index <- rowSums(density_matrix * absence_matrix * ici_matrix); names(strategic_index) <- geo_ids
    
    use_chunked_sg <- auto_chunk_method("strategic gain", n_geos, 3000)
    strategic_gain <- if (use_chunked_sg) {
      calculate_strategic_gain_chunked(M_matrix, phi_matrix, density_matrix, ici, phi_col_sums)
    } else {
      calculate_strategic_gain_vectorized(M_matrix, phi_matrix, density_matrix, ici, phi_col_sums)
    }
    rownames(strategic_gain) <- geo_ids; colnames(strategic_gain) <- ind_codes
    
    proximity_results <- list(co_occurrence = U_matrix, proximity = phi_matrix, centrality = centrality_implicit,
                              density = density_matrix, strategic_index = strategic_index, strategic_gain = strategic_gain)
  }
  
  final_ici_ubiq_corr <- cor(ici, ubiquity_vec); final_eci_div_corr <- cor(eci, diversity_vec)
  log_msg("  ECI range: [", round(min(eci), 2), ", ", round(max(eci), 2), "] | ICI range: [", round(min(ici), 2), ", ", round(max(ici), 2), "]\n")
  log_msg("  cor(ICI,ubiq)=", round(final_ici_ubiq_corr, 3), " cor(ECI,div)=", round(final_eci_div_corr, 3), 
          " cor(ECI,avgICI)=", round(eci_avgici_corr, 3), "\n")
  
  list(
    eci = tibble(geoid = geo_ids, economic_complexity_index = eci),
    ici = tibble(industry_code = ind_codes, industry_complexity_index = ici),
    diversity = tibble(geoid = geo_ids, diversity = diversity_vec),
    ubiquity = tibble(industry_code = ind_codes, ubiquity = ubiquity_vec),
    K_c1 = tibble(geoid = geo_ids, avg_ubiquity = as.vector(K_c1)),
    K_i1 = tibble(industry_code = ind_codes, avg_diversity = as.vector(K_i1)),
    avg_ici = tibble(geoid = geo_ids, avg_ici = avg_ici_by_geo),
    M_matrix = M_matrix, proximity = proximity_results,
    diagnostics = list(ici_ubiquity_correlation = final_ici_ubiq_corr, eci_diversity_correlation = final_eci_div_corr,
                       eci_avgici_correlation = eci_avgici_corr, first_eigenvalue_C = first_eval_C,
                       second_eigenvalue_C = second_eval_C, third_eigenvalue_C = third_eval_C,
                       first_eigenvalue_I = first_eval_I, second_eigenvalue_I = second_eval_I,
                       third_eigenvalue_I = third_eval_I, ici_sign_flipped = ici_sign_flipped,
                       eci_sign_flipped = eci_sign_flipped, n_geos = n_geos, n_inds = n_inds, fill_rate = fill_rate)
  )
}

# INDUSTRY SPACE VISUALIZATION
create_industry_space_plot <- function(complexity_results, industry_names, geo_level_name, top_pct = 0.05) {
  if (is.null(complexity_results$proximity)) return(NULL)
  phi_matrix <- complexity_results$proximity$proximity
  ind_codes <- complexity_results$ici$industry_code; n_inds <- nrow(phi_matrix)
  
  use_chunked_edges <- auto_chunk_method("edge list", n_inds, 3000)
  final_edges <- if (use_chunked_edges) create_edge_list_chunked(phi_matrix, ind_codes, top_pct) 
  else create_edge_list_vectorized(phi_matrix, ind_codes, top_pct)
  
  g <- graph_from_data_frame(final_edges, directed = FALSE)
  node_df <- tibble(industry_code = V(g)$name) %>%
    left_join(complexity_results$ici %>% rename(ICI = industry_complexity_index), by = "industry_code") %>%
    left_join(complexity_results$ubiquity %>% rename(ubiq = ubiquity), by = "industry_code") %>%
    left_join(industry_names, by = "industry_code")
  V(g)$ICI <- node_df$ICI; V(g)$ubiquity <- node_df$ubiq; V(g)$label <- substr(node_df$industry_description, 1, 20)
  
  ggraph(g, layout = "fr") +
    geom_edge_link(aes(alpha = weight), color = "gray70", show.legend = FALSE) +
    geom_node_point(aes(color = ICI, size = ubiquity), alpha = 0.8) +
    scale_color_viridis_c(option = "plasma", name = "ICI") +
    scale_size_continuous(range = c(1, 6), name = "Ubiquity") +
    labs(title = paste("Industry Space:", geo_level_name),
         subtitle = paste("Top", top_pct*100, "% proximity connections"),
         caption = "Node size = ubiquity, color = ICI") +
    theme_void() + theme(plot.title = element_text(hjust = 0.5, size = 14, face = "bold"),
                         plot.subtitle = element_text(hjust = 0.5, size = 10), legend.position = "right")
}

# PEER GEOGRAPHY ANALYSIS
calculate_geography_similarity <- function(M_matrix) {
  sim <- as.matrix(proxy::simil(M_matrix, method = "Jaccard"))
  rownames(sim) <- colnames(sim) <- rownames(M_matrix)
  sim
}

get_similar_geographies <- function(target_geoid, similarity_matrix, n = 5) {
  if (!target_geoid %in% rownames(similarity_matrix)) return(NULL)
  sims <- similarity_matrix[target_geoid, ]; sims <- sims[names(sims) != target_geoid]
  top_sim <- sort(sims, decreasing = TRUE)[1:min(n, length(sims))]
  data.frame(peer_geoid = names(top_sim), jaccard_similarity = as.vector(top_sim), stringsAsFactors = FALSE)
}

compute_peer_geography_analysis <- function(complexity_results, geo_names, n_peers = 5) {
  M_matrix <- complexity_results$M_matrix
  similarity_matrix <- calculate_geography_similarity(M_matrix)
  log_msg("  Similarity matrix: ", nrow(M_matrix), " x ", nrow(M_matrix), "\n")
  
  geo_ids <- rownames(M_matrix)
  all_peers <- lapply(geo_ids, function(geoid) {
    peers <- get_similar_geographies(geoid, similarity_matrix, n_peers)
    if (!is.null(peers)) { peers$geoid <- geoid; peers$peer_rank <- 1:nrow(peers) }
    peers
  })
  peer_df <- bind_rows(all_peers) %>% select(geoid, peer_rank, peer_geoid, jaccard_similarity) %>%
    left_join(geo_names, by = "geoid") %>% rename(geography_name = name) %>%
    left_join(geo_names %>% rename(peer_geoid = geoid, peer_name = name), by = "peer_geoid")
  log_msg("  Peer relationships: ", format(nrow(peer_df), big.mark = ","), "\n")
  list(similarity_matrix = similarity_matrix, peer_relationships = peer_df)
}

print_top10_rankings <- function(complexity_results, geo_names, industry_names, geo_level_name, state_lookup = NULL) {
  # Top 5 ECI - include state for counties
  top_eci <- complexity_results$eci %>% arrange(desc(economic_complexity_index)) %>% head(5) %>%
    left_join(geo_names, by = "geoid")
  
  if (!is.null(state_lookup) && "state_abbreviation" %in% names(state_lookup)) {
    top_eci <- top_eci %>% left_join(state_lookup, by = "geoid") %>%
      mutate(display = paste0(name, ", ", state_abbreviation, " (", round(economic_complexity_index, 2), ")"))
  } else {
    top_eci <- top_eci %>% mutate(display = paste0(name, " (", round(economic_complexity_index, 2), ")"))
  }
  
  top_ici <- complexity_results$ici %>% arrange(desc(industry_complexity_index)) %>% head(5) %>%
    left_join(industry_names, by = "industry_code") %>%
    mutate(display = paste0(substr(industry_description, 1, 25), " (", round(industry_complexity_index, 2), ")"))
  
  cat("  Top 5 ECI: ", paste(top_eci$display, collapse = " | "), "\n", sep = "")
  cat("  Top 5 ICI: ", paste(top_ici$display, collapse = " | "), "\n", sep = "")
}

# STEP 1: Alaska Population
log_step(1, "Alaska Population Data")
alaska_pop <- getCensus(name = "acs/acs5", vintage = 2023, vars = c("NAME", "B01003_001E"),
                        region = "county:*", regionin = "state:02") %>%
  mutate(county_geoid = paste0(state, county), population = as.numeric(B01003_001E))
chugach_share <- alaska_pop$population[alaska_pop$county_geoid == "02063"] /
  sum(alaska_pop$population[alaska_pop$county_geoid %in% c("02063", "02066")])
copper_river_share <- 1 - chugach_share

# STEP 2: TIGRIS Geography
log_step(2, "Loading TIGRIS Geography")
tigris_2024_counties <- counties(cb = FALSE, year = 2024, class = "sf") %>%
  filter(!STATEFP %in% c("60", "66", "69", "72", "78")) %>% st_drop_geometry() %>%
  select(county_geoid = GEOID, county_name = NAMELSAD)
tigris_2024_states <- states(cb = TRUE, year = 2024, class = "sf") %>%
  filter(!STATEFP %in% c("60", "66", "69", "72", "78")) %>% st_drop_geometry() %>%
  select(state_fips = STATEFP, state_name = NAME)

# STEP 3: Lightcast Data
log_step(3, "Processing Lightcast Data")
lightcast_2024_data <- read_csv(lightcast_file, show_col_types = FALSE) %>%
  { bind_rows(filter(., COUNTY_ID != 2261),
              filter(., COUNTY_ID == 2261) %>% mutate(COUNTY_ID = 2063, EMPLOYMENT = round(EMPLOYMENT * chugach_share)),
              filter(., COUNTY_ID == 2261) %>% mutate(COUNTY_ID = 2066, EMPLOYMENT = round(EMPLOYMENT * copper_river_share))) } %>%
  mutate(county_geoid = str_pad(COUNTY_ID, width = 5, side = "left", pad = "0")) %>%
  rename(industry_code = IND_ID, industry_description = IND_DESCRIPTION, industry_employment_county = EMPLOYMENT) %>%
  mutate(industry_code = as.character(industry_code)) %>%
  select(county_geoid, industry_code, industry_description, industry_employment_county) %>%
  { industry_lookup <- distinct(., industry_code, industry_description)
  complete(., county_geoid, industry_code, fill = list(industry_employment_county = 0)) %>%
    select(-industry_description) %>% left_join(industry_lookup, by = "industry_code") } %>%
  mutate(state_fips = str_sub(county_geoid, 1, 2), county_fips = str_sub(county_geoid, 3, 5),
         unknown_undefined_county = (county_fips == "999")) %>%
  group_by(county_geoid) %>% mutate(total_employment_county = sum(industry_employment_county, na.rm = TRUE)) %>% ungroup() %>%
  group_by(industry_code) %>% mutate(industry_employment_nation = sum(industry_employment_county, na.rm = TRUE)) %>% ungroup() %>%
  mutate(total_employment_nation = sum(industry_employment_county[!duplicated(paste0(county_geoid, industry_code))], na.rm = TRUE)) %>%
  mutate(industry_employment_share_county = if_else(total_employment_county == 0, 0, industry_employment_county / total_employment_county),
         industry_employment_share_nation = industry_employment_nation / total_employment_nation,
         industry_location_quotient_county = if_else(industry_employment_share_nation == 0 | total_employment_county == 0, 0,
                                                     industry_employment_share_county / industry_employment_share_nation)) %>%
  left_join(tigris_2024_counties, by = "county_geoid") %>% left_join(tigris_2024_states, by = "state_fips") %>%
  select(county_geoid, state_fips, county_fips, county_name, state_name, unknown_undefined_county,
         industry_code, industry_description, industry_employment_county, total_employment_county,
         industry_employment_nation, total_employment_nation, industry_employment_share_county,
         industry_employment_share_nation, industry_location_quotient_county)
data_summary(lightcast_2024_data, "Lightcast")

# STEP 4: Commuting Zones
log_step(4, "Loading Commuting Zones")
cz_url <- "https://www.ers.usda.gov/media/6968/2020-commuting-zones.csv?v=56155"
cz <- read_csv(cz_url, show_col_types = FALSE) %>%
  transmute(county_geoid = FIPStxt, commuting_zone_geoid = as.character(CZ2020), commuting_zone_name = CZName)
clean_cz_name <- function(x) {
  x <- str_squish(as.character(x)); m <- str_match(x, "^(.*),\\s*(.+)$")
  place <- ifelse(is.na(m[,2]), x, m[,2]); st <- ifelse(is.na(m[,3]), NA_character_, m[,3]) %>%
    str_replace_all("--", "-") %>% str_replace_all("\\s*-\\s*", "-") %>% str_squish()
  place <- place %>% str_replace(regex("\\s+(city and borough|consolidated government \\(balance\\)|city|town|village|CDP)\\s*$", TRUE), "") %>% str_squish()
  str_squish(str_replace(ifelse(is.na(st), place, paste0(place, ", ", st)), ",\\s+", ", "))
}
cz <- cz %>% mutate(commuting_zone_name_original = commuting_zone_name, commuting_zone_name = clean_cz_name(commuting_zone_name))

# STEP 5: Geographic Crosswalk
log_step(5, "Building Geographic Crosswalk")
counties_2024 <- counties(year = 2024, cb = FALSE) %>% filter(!STATEFP %in% c("60", "66", "69", "72", "78")) %>%
  transmute(state_fips = STATEFP, county_fips = COUNTYFP, county_geoid = GEOID, county_name = NAMELSAD,
            cbsa_geoid = if_else(is.na(CBSAFP) | CBSAFP == "", NA_character_, CBSAFP),
            csa_geoid = if_else(is.na(CSAFP) | CSAFP == "", NA_character_, CSAFP), geometry,
            county_in_cbsa = !is.na(CBSAFP) & CBSAFP != "", county_in_csa = !is.na(CSAFP) & CSAFP != "")
states_2024 <- states(year = 2024, cb = FALSE) %>% st_set_geometry(NULL) %>%
  transmute(state_fips = STATEFP, state_abbreviation = STUSPS, state_name = NAME)
cbsa_2024 <- core_based_statistical_areas(year = 2024, cb = FALSE) %>% st_set_geometry(NULL) %>%
  transmute(cbsa_geoid = CBSAFP, cbsa_name = NAMELSAD)
csa_2024 <- combined_statistical_areas(year = 2024, cb = FALSE) %>% st_set_geometry(NULL) %>%
  transmute(csa_geoid = CSAFP, csa_name = NAMELSAD)

tigris_county_state_cbsa_csa_cz_2024 <- counties_2024 %>%
  left_join(states_2024, by = "state_fips") %>% left_join(cbsa_2024, by = "cbsa_geoid") %>%
  left_join(csa_2024, by = "csa_geoid") %>%
  left_join(cz %>% select(county_geoid, commuting_zone_geoid, commuting_zone_name), by = "county_geoid") %>%
  select(state_fips, state_name, state_abbreviation, county_fips, county_geoid, county_name,
         cbsa_geoid, cbsa_name, county_in_cbsa, csa_geoid, csa_name, county_in_csa,
         commuting_zone_geoid, commuting_zone_name, geometry)

county_state_cbsa_csa_cz_crosswalk <- tigris_county_state_cbsa_csa_cz_2024 %>% st_drop_geometry()
county_geometry <- tigris_county_state_cbsa_csa_cz_2024 %>% select(county_geoid, geometry) %>% st_as_sf()
state_geometry <- tigris_county_state_cbsa_csa_cz_2024 %>% group_by(state_fips, state_name, state_abbreviation) %>%
  summarise(geometry = st_union(geometry), .groups = "drop") %>% st_as_sf()
cbsa_geometry <- tigris_county_state_cbsa_csa_cz_2024 %>% filter(!is.na(cbsa_geoid)) %>%
  group_by(cbsa_geoid, cbsa_name) %>% summarise(geometry = st_union(geometry), .groups = "drop") %>% st_as_sf()
csa_geometry <- tigris_county_state_cbsa_csa_cz_2024 %>% filter(!is.na(csa_geoid)) %>%
  group_by(csa_geoid, csa_name) %>% summarise(geometry = st_union(geometry), .groups = "drop") %>% st_as_sf()
cz_geometry <- tigris_county_state_cbsa_csa_cz_2024 %>% filter(!is.na(commuting_zone_geoid)) %>%
  group_by(commuting_zone_geoid, commuting_zone_name) %>% summarise(geometry = st_union(geometry), .groups = "drop") %>% st_as_sf()

# STEP 6: Calculate State-Level Metrics (INCLUDING unknown/undefined counties)
log_step(6, "Calculating State-Level Metrics (incl. unknown/undefined counties)")
# State-level LQs must include unknown/undefined county employment before those counties are filtered out
# This ensures state totals and LQs reflect ALL employment in the state
national_shares <- lightcast_2024_data %>% select(industry_code, industry_employment_share_nation) %>% distinct()

state_totals <- lightcast_2024_data %>% group_by(state_fips) %>% 
  summarise(total_employment_state = sum(industry_employment_county, na.rm = TRUE), .groups = "drop")
state_industry <- lightcast_2024_data %>% group_by(state_fips, industry_code) %>%
  summarise(industry_employment_state = sum(industry_employment_county, na.rm = TRUE), .groups = "drop") %>%
  left_join(state_totals, by = "state_fips") %>% left_join(national_shares, by = "industry_code") %>%
  mutate(industry_employment_share_state = if_else(total_employment_state == 0, 0, industry_employment_state / total_employment_state),
         industry_location_quotient_state = if_else(industry_employment_share_nation == 0 | total_employment_state == 0, 0,
                                                    industry_employment_share_state / industry_employment_share_nation))

# Report how much employment is in unknown/undefined counties
unknown_emp <- lightcast_2024_data %>% filter(unknown_undefined_county) %>% summarise(emp = sum(industry_employment_county, na.rm = TRUE)) %>% pull(emp)
total_emp <- lightcast_2024_data %>% summarise(emp = sum(industry_employment_county, na.rm = TRUE)) %>% pull(emp)
log_msg("  Unknown/undefined county employment: ", format(unknown_emp, big.mark = ","), " (", round(100 * unknown_emp / total_emp, 2), "% of total)\n")
data_summary(state_industry, "State-industry (incl. unknown)")

# STEP 7: Join Geography and Calculate Other Geographic Level Metrics
log_step(7, "Calculating CBSA/CSA/CZ Metrics (excluding unknown/undefined)")
lightcast_with_geodata <- lightcast_2024_data %>%
  left_join(county_state_cbsa_csa_cz_crosswalk %>% select(state_abbreviation, county_geoid, county_in_cbsa, cbsa_geoid, cbsa_name,
                                                          county_in_csa, csa_geoid, csa_name, commuting_zone_geoid, commuting_zone_name), by = "county_geoid") %>%
  mutate(county_name = if_else(unknown_undefined_county, "Unknown/Undefined", county_name),
         county_in_cbsa = if_else(unknown_undefined_county, FALSE, county_in_cbsa),
         county_in_csa = if_else(unknown_undefined_county, FALSE, county_in_csa))

# CBSA/CSA/CZ calculations exclude unknown/undefined counties (they don't belong to these geographies)
lightcast_known <- lightcast_with_geodata %>% filter(!unknown_undefined_county)

cbsa_totals <- lightcast_known %>% filter(!is.na(cbsa_geoid)) %>% group_by(cbsa_geoid) %>%
  summarise(total_employment_cbsa = sum(industry_employment_county, na.rm = TRUE), .groups = "drop")
cbsa_industry <- lightcast_known %>% filter(!is.na(cbsa_geoid)) %>% group_by(cbsa_geoid, industry_code) %>%
  summarise(industry_employment_cbsa = sum(industry_employment_county, na.rm = TRUE), .groups = "drop") %>%
  left_join(cbsa_totals, by = "cbsa_geoid") %>% left_join(national_shares, by = "industry_code") %>%
  mutate(industry_employment_share_cbsa = if_else(total_employment_cbsa == 0, 0, industry_employment_cbsa / total_employment_cbsa),
         industry_location_quotient_cbsa = if_else(industry_employment_share_nation == 0 | total_employment_cbsa == 0, 0,
                                                   industry_employment_share_cbsa / industry_employment_share_nation)) %>%
  select(-industry_employment_share_nation)

csa_totals <- lightcast_known %>% filter(!is.na(csa_geoid)) %>% group_by(csa_geoid) %>%
  summarise(total_employment_csa = sum(industry_employment_county, na.rm = TRUE), .groups = "drop")
csa_industry <- lightcast_known %>% filter(!is.na(csa_geoid)) %>% group_by(csa_geoid, industry_code) %>%
  summarise(industry_employment_csa = sum(industry_employment_county, na.rm = TRUE), .groups = "drop") %>%
  left_join(csa_totals, by = "csa_geoid") %>% left_join(national_shares, by = "industry_code") %>%
  mutate(industry_employment_share_csa = if_else(total_employment_csa == 0, 0, industry_employment_csa / total_employment_csa),
         industry_location_quotient_csa = if_else(industry_employment_share_nation == 0 | total_employment_csa == 0, 0,
                                                  industry_employment_share_csa / industry_employment_share_nation)) %>%
  select(-industry_employment_share_nation)

cz_totals <- lightcast_known %>% filter(!is.na(commuting_zone_geoid)) %>% group_by(commuting_zone_geoid) %>%
  summarise(total_employment_cz = sum(industry_employment_county, na.rm = TRUE), .groups = "drop")
cz_industry <- lightcast_known %>% filter(!is.na(commuting_zone_geoid)) %>% group_by(commuting_zone_geoid, industry_code) %>%
  summarise(industry_employment_cz = sum(industry_employment_county, na.rm = TRUE), .groups = "drop") %>%
  left_join(cz_totals, by = "commuting_zone_geoid") %>% left_join(national_shares, by = "industry_code") %>%
  mutate(industry_employment_share_cz = if_else(total_employment_cz == 0, 0, industry_employment_cz / total_employment_cz),
         industry_location_quotient_cz = if_else(industry_employment_share_nation == 0 | total_employment_cz == 0, 0,
                                                 industry_employment_share_cz / industry_employment_share_nation)) %>%
  select(-industry_employment_share_nation)

# Join all geographic level metrics back to the known counties data frame
lightcast_with_geodata <- lightcast_known %>%
  left_join(state_industry %>% select(state_fips, industry_code, industry_employment_state, total_employment_state,
                                      industry_employment_share_state, industry_location_quotient_state), by = c("state_fips", "industry_code")) %>%
  left_join(cbsa_industry %>% select(cbsa_geoid, industry_code, industry_employment_cbsa, total_employment_cbsa,
                                     industry_employment_share_cbsa, industry_location_quotient_cbsa), by = c("cbsa_geoid", "industry_code")) %>%
  left_join(csa_industry %>% select(csa_geoid, industry_code, industry_employment_csa, total_employment_csa,
                                    industry_employment_share_csa, industry_location_quotient_csa), by = c("csa_geoid", "industry_code")) %>%
  left_join(cz_industry %>% select(commuting_zone_geoid, industry_code, industry_employment_cz, total_employment_cz,
                                   industry_employment_share_cz, industry_location_quotient_cz), by = c("commuting_zone_geoid", "industry_code")) %>%
  rename(industry_employment_commuting_zone = industry_employment_cz, total_employment_commuting_zone = total_employment_cz,
         industry_employment_share_commuting_zone = industry_employment_share_cz,
         industry_location_quotient_commuting_zone = industry_location_quotient_cz) %>%
  select(-unknown_undefined_county) %>%
  select(state_fips, state_name, state_abbreviation, county_fips, county_geoid, county_name,
         county_in_cbsa, cbsa_geoid, cbsa_name, county_in_csa, csa_geoid, csa_name,
         commuting_zone_geoid, commuting_zone_name, industry_code, industry_description,
         industry_employment_county, total_employment_county, industry_employment_share_county, industry_location_quotient_county,
         industry_employment_state, total_employment_state, industry_employment_share_state, industry_location_quotient_state,
         industry_employment_cbsa, total_employment_cbsa, industry_employment_share_cbsa, industry_location_quotient_cbsa,
         industry_employment_csa, total_employment_csa, industry_employment_share_csa, industry_location_quotient_csa,
         industry_employment_commuting_zone, total_employment_commuting_zone, industry_employment_share_commuting_zone,
         industry_location_quotient_commuting_zone, industry_employment_nation, industry_employment_share_nation, total_employment_nation)

# STEP 8: Create EmpShare/LQ Data Frames
log_step(8, "Creating EmpShare/LQ Data Frames")
county_empshare_lq <- lightcast_with_geodata %>% select(county_geoid, industry_code, industry_employment_share_county, industry_location_quotient_county) %>% distinct()
state_empshare_lq <- lightcast_with_geodata %>% select(state_fips, industry_code, industry_employment_share_state, industry_location_quotient_state) %>% distinct()
cbsa_empshare_lq <- lightcast_with_geodata %>% filter(!is.na(cbsa_geoid)) %>% select(cbsa_geoid, industry_code, industry_employment_share_cbsa, industry_location_quotient_cbsa) %>% distinct()
csa_empshare_lq <- lightcast_with_geodata %>% filter(!is.na(csa_geoid)) %>% select(csa_geoid, industry_code, industry_employment_share_csa, industry_location_quotient_csa) %>% distinct()
commuting_zone_empshare_lq <- lightcast_with_geodata %>% filter(!is.na(commuting_zone_geoid)) %>% select(commuting_zone_geoid, industry_code, industry_employment_share_commuting_zone, industry_location_quotient_commuting_zone) %>% distinct()

# STEP 9: Combined EmpShare/LQ
log_step(9, "Creating Combined EmpShare/LQ Frame")
geo_aggregation_levels <- tibble(geo_aggregation_level = 1:5L, geo_aggregation_name = c("county", "state", "cbsa", "csa", "commuting_zone"))
industry_titles <- lightcast_with_geodata %>% select(industry_code, industry_description) %>% distinct() %>% arrange(industry_code)

combined_empshare_lq <- bind_rows(
  county_empshare_lq %>% transmute(geoid = county_geoid, geo_aggregation_level = 1L, industry_code, industry_empshare = industry_employment_share_county, industry_lq = industry_location_quotient_county),
  state_empshare_lq %>% transmute(geoid = state_fips, geo_aggregation_level = 2L, industry_code, industry_empshare = industry_employment_share_state, industry_lq = industry_location_quotient_state),
  cbsa_empshare_lq %>% transmute(geoid = cbsa_geoid, geo_aggregation_level = 3L, industry_code, industry_empshare = industry_employment_share_cbsa, industry_lq = industry_location_quotient_cbsa),
  csa_empshare_lq %>% transmute(geoid = csa_geoid, geo_aggregation_level = 4L, industry_code, industry_empshare = industry_employment_share_csa, industry_lq = industry_location_quotient_csa),
  commuting_zone_empshare_lq %>% transmute(geoid = commuting_zone_geoid, geo_aggregation_level = 5L, industry_code, industry_empshare = industry_employment_share_commuting_zone, industry_lq = industry_location_quotient_commuting_zone)
)

# STEP 10: Calculate Complexity Metrics
log_step(10, "Calculating Complexity Metrics (All Levels)")
county_names <- lightcast_with_geodata %>% select(geoid = county_geoid, name = county_name) %>% distinct()
state_names <- lightcast_with_geodata %>% select(geoid = state_fips, name = state_name) %>% distinct()
cbsa_names <- lightcast_with_geodata %>% filter(!is.na(cbsa_geoid)) %>% select(geoid = cbsa_geoid, name = cbsa_name) %>% distinct()
csa_names <- lightcast_with_geodata %>% filter(!is.na(csa_geoid)) %>% select(geoid = csa_geoid, name = csa_name) %>% distinct()
cz_names <- lightcast_with_geodata %>% filter(!is.na(commuting_zone_geoid)) %>% select(geoid = commuting_zone_geoid, name = commuting_zone_name) %>% distinct()

# Prepare data for each level
complexity_inputs <- list(
  county = list(data = combined_empshare_lq %>% filter(geo_aggregation_level == 1L) %>% select(geoid, industry_code, industry_lq), 
                name = "county", names_df = county_names),
  state = list(data = combined_empshare_lq %>% filter(geo_aggregation_level == 2L) %>% select(geoid, industry_code, industry_lq), 
               name = "state", names_df = state_names),
  cbsa = list(data = combined_empshare_lq %>% filter(geo_aggregation_level == 3L) %>% select(geoid, industry_code, industry_lq), 
              name = "cbsa", names_df = cbsa_names),
  csa = list(data = combined_empshare_lq %>% filter(geo_aggregation_level == 4L) %>% select(geoid, industry_code, industry_lq), 
             name = "csa", names_df = csa_names),
  cz = list(data = combined_empshare_lq %>% filter(geo_aggregation_level == 5L) %>% select(geoid, industry_code, industry_lq), 
            name = "commuting_zone", names_df = cz_names)
)

# Run complexity calculations
start_time <- Sys.time()
log_msg("  Running complexity calculations", if (use_parallel) paste0(" (parallel, ", n_cores, " cores)") else " (sequential)", "...\n")

complexity_results <- parallel_lapply(complexity_inputs, function(inp) {
  calculate_complexity_metrics(inp$data, inp$name, run_stress_tests = TRUE, create_industry_space = TRUE)
})

# Check for NULL results (parallel failure) and retry sequentially if needed
if (any(sapply(complexity_results, is.null))) {
  log_msg("  Parallel processing failed, retrying sequentially...\n")
  complexity_results <- lapply(complexity_inputs, function(inp) {
    calculate_complexity_metrics(inp$data, inp$name, run_stress_tests = TRUE, create_industry_space = TRUE)
  })
}

elapsed <- round(as.numeric(difftime(Sys.time(), start_time, units = "secs")), 1)
log_msg("  Complexity calculations completed in ", elapsed, " seconds\n")

county_complexity <- complexity_results$county
state_complexity <- complexity_results$state
cbsa_complexity <- complexity_results$cbsa
csa_complexity <- complexity_results$csa
cz_complexity <- complexity_results$cz

# Create state lookup for counties
county_state_lookup <- lightcast_with_geodata %>% 
  select(geoid = county_geoid, state_abbreviation) %>% distinct()

# Print top 5 rankings for each level
for (level_name in names(complexity_inputs)) {
  state_lookup <- if (level_name == "county") county_state_lookup else NULL
  print_top10_rankings(complexity_results[[level_name]], complexity_inputs[[level_name]]$names_df, 
                       industry_titles, complexity_inputs[[level_name]]$name, state_lookup)
}

# STEP 11: Industry Space Visualizations
log_step(11, "Creating Industry Space Visualizations")
county_industry_space <- create_industry_space_plot(county_complexity, industry_titles, "County")
cbsa_industry_space <- create_industry_space_plot(cbsa_complexity, industry_titles, "CBSA")
cz_industry_space <- create_industry_space_plot(cz_complexity, industry_titles, "Commuting Zone")
state_industry_space <- create_industry_space_plot(state_complexity, industry_titles, "State")
csa_industry_space <- create_industry_space_plot(csa_complexity, industry_titles, "CSA")
print(county_industry_space)
print(cbsa_industry_space)
print(cz_industry_space)
print(state_industry_space)
print(csa_industry_space)


# STEP 12: Peer Geography Analysis
log_step(12, "Peer Geography Analysis")
county_peers <- compute_peer_geography_analysis(county_complexity, county_names, n_peers = 5)
cbsa_peers <- compute_peer_geography_analysis(cbsa_complexity, cbsa_names, n_peers = 5)

# STEP 13: Create Reorganized Output Data Frames
log_step(13, "Creating Reorganized Output Data Frames")

# Helper: compute percentile (100 = best/highest value)
compute_percentile <- function(x) round(100 * rank(x, na.last = "keep") / sum(!is.na(x)), 2)

# Helper: extract geography-industry pair data from complexity results (with chunking for large matrices)
extract_geo_industry_pairs <- function(complexity_result, empshare_lq_df, geo_level, geoid_col, chunk_size = 50000) {
  M_mat <- complexity_result$M_matrix
  density_mat <- complexity_result$proximity$density
  sg_mat <- complexity_result$proximity$strategic_gain
  geo_ids <- rownames(M_mat); ind_codes <- colnames(M_mat)
  n_pairs <- length(geo_ids) * length(ind_codes)
  
  # For very large matrices, process in chunks
  if (n_pairs > chunk_size) {
    n_chunks <- ceiling(n_pairs / chunk_size)
    pairs_list <- vector("list", n_chunks)
    all_pairs <- expand.grid(geoid = geo_ids, industry_code = ind_codes, stringsAsFactors = FALSE)
    
    for (i in seq_len(n_chunks)) {
      start_idx <- (i - 1) * chunk_size + 1
      end_idx <- min(i * chunk_size, n_pairs)
      chunk_pairs <- all_pairs[start_idx:end_idx, ]
      
      # Get matrix indices for this chunk
      row_idx <- match(chunk_pairs$geoid, geo_ids)
      col_idx <- match(chunk_pairs$industry_code, ind_codes)
      linear_idx <- (col_idx - 1) * length(geo_ids) + row_idx
      
      chunk_pairs$industry_comparative_advantage <- as.vector(M_mat)[linear_idx]
      chunk_pairs$industry_feasibility <- as.vector(density_mat)[linear_idx]
      chunk_pairs$strategic_gain <- as.vector(sg_mat)[linear_idx]
      pairs_list[[i]] <- chunk_pairs
    }
    pairs <- bind_rows(pairs_list)
  } else {
    pairs <- expand.grid(geoid = geo_ids, industry_code = ind_codes, stringsAsFactors = FALSE) %>%
      mutate(
        industry_comparative_advantage = as.vector(t(M_mat)),
        industry_feasibility = as.vector(t(density_mat)),
        strategic_gain = as.vector(t(sg_mat))
      )
  }
  
  pairs %>%
    left_join(empshare_lq_df %>% rename(geoid = !!geoid_col), by = c("geoid", "industry_code")) %>%
    mutate(
      geo_aggregation_level = geo_level,
      industry_present = as.integer(location_quotient > 0),
      strategic_gain_possible = as.integer(strategic_gain > 0)
    ) %>%
    group_by(industry_code) %>%  # Rank within each industry (national ranking)
    mutate(
      industry_feasibility_percentile_score = compute_percentile(industry_feasibility),
      strategic_gain_percentile_score = compute_percentile(strategic_gain)
    ) %>%
    ungroup() %>%
    select(geo_aggregation_level, geoid, industry_code, industry_employment_share, location_quotient,
           industry_present, industry_comparative_advantage, industry_feasibility, 
           industry_feasibility_percentile_score, strategic_gain_possible, strategic_gain,
           strategic_gain_percentile_score)
}

# Helper: extract geography-specific data
extract_geo_specific <- function(complexity_result, geo_level, geo_names_df) {
  tibble(
    geo_aggregation_level = geo_level,
    geoid = complexity_result$eci$geoid,
    industrial_diversity = complexity_result$diversity$diversity,
    economic_complexity_index = complexity_result$eci$economic_complexity_index,
    strategic_index = complexity_result$proximity$strategic_index[complexity_result$eci$geoid]
  ) %>%
    left_join(geo_names_df, by = "geoid") %>%
    mutate(
      economic_complexity_percentile_score = compute_percentile(economic_complexity_index),
      strategic_index_percentile = compute_percentile(strategic_index)
    ) %>%
    select(geo_aggregation_level, geoid, name, industrial_diversity, economic_complexity_index,
           economic_complexity_percentile_score, strategic_index, strategic_index_percentile)
}

# Helper: extract industry-specific data
extract_industry_specific <- function(complexity_result, geo_level, nat_shares, ind_titles) {
  tibble(
    geo_aggregation_level = geo_level,
    industry_code = complexity_result$ici$industry_code,
    industry_ubiquity = complexity_result$ubiquity$ubiquity,
    industry_complexity = complexity_result$ici$industry_complexity_index
  ) %>%
    left_join(nat_shares, by = "industry_code") %>%
    left_join(ind_titles, by = "industry_code") %>%
    mutate(industry_complexity_percentile = compute_percentile(industry_complexity)) %>%
    select(geo_aggregation_level, industry_code, industry_description, industry_ubiquity, industry_employment_share_nation,
           industry_complexity, industry_complexity_percentile)
}

# Helper: extract industry space edge list and node data
extract_industry_space_data <- function(complexity_result, geo_level, ind_titles, top_pct = 0.05) {
  phi_mat <- complexity_result$proximity$proximity
  ind_codes <- rownames(phi_mat)
  
  # Create edge list (top edges + strongest per node) - use chunked for large matrices
  use_chunked <- auto_chunk_method("edge list", nrow(phi_mat), 3000, verbose = FALSE)
  edges <- if (use_chunked) create_edge_list_chunked(phi_mat, ind_codes, top_pct, verbose = FALSE)
  else create_edge_list_vectorized(phi_mat, ind_codes, top_pct)
  edges$geo_aggregation_level <- geo_level
  
  # Node attributes
  nodes <- tibble(
    geo_aggregation_level = geo_level,
    industry_code = ind_codes,
    industry_complexity = complexity_result$ici$industry_complexity_index,
    industry_ubiquity = complexity_result$ubiquity$ubiquity,
    industry_centrality = complexity_result$proximity$centrality
  ) %>% left_join(ind_titles, by = "industry_code")
  list(edges = edges, nodes = nodes)
}

# Prepare empshare/lq data frames with standardized column names
county_lq <- county_empshare_lq %>% rename(industry_employment_share = industry_employment_share_county, location_quotient = industry_location_quotient_county)
state_lq <- state_empshare_lq %>% rename(industry_employment_share = industry_employment_share_state, location_quotient = industry_location_quotient_state)
cbsa_lq <- cbsa_empshare_lq %>% rename(industry_employment_share = industry_employment_share_cbsa, location_quotient = industry_location_quotient_cbsa)
csa_lq <- csa_empshare_lq %>% rename(industry_employment_share = industry_employment_share_csa, location_quotient = industry_location_quotient_csa)
cz_lq <- commuting_zone_empshare_lq %>% rename(industry_employment_share = industry_employment_share_commuting_zone, location_quotient = industry_location_quotient_commuting_zone)

# Pre-compute national shares for industry_specific extraction
nat_shares <- lightcast_with_geodata %>% select(industry_code, industry_employment_share_nation) %>% distinct()

# Define extraction parameters for each level
extraction_params <- list(
  list(complexity = county_complexity, lq = county_lq, level = 1L, geoid_col = "county_geoid", names = county_names),
  list(complexity = state_complexity, lq = state_lq, level = 2L, geoid_col = "state_fips", names = state_names),
  list(complexity = cbsa_complexity, lq = cbsa_lq, level = 3L, geoid_col = "cbsa_geoid", names = cbsa_names),
  list(complexity = csa_complexity, lq = csa_lq, level = 4L, geoid_col = "csa_geoid", names = csa_names),
  list(complexity = cz_complexity, lq = cz_lq, level = 5L, geoid_col = "commuting_zone_geoid", names = cz_names)
)

# Extract geography_industry_complexity_pairs
log_msg("  Extracting: geo-industry pairs")
start_time <- Sys.time()

# Use sequential processing (parallel mclapply has issues with complex nested functions)
geo_ind_pairs_list <- lapply(extraction_params, function(p) {
  extract_geo_industry_pairs(p$complexity, p$lq, p$level, p$geoid_col)
})

geography_industry_complexity_pairs <- bind_rows(geo_ind_pairs_list)
cat(" →", format(nrow(geography_industry_complexity_pairs), big.mark = ","), "rows", 
    "(", round(as.numeric(difftime(Sys.time(), start_time, units = "secs")), 1), "s)\n")

# Extract geography_specific_data
log_msg("  Extracting: geography-specific")
geo_specific_list <- lapply(extraction_params, function(p) {
  extract_geo_specific(p$complexity, p$level, p$names)
})
geography_specific_data <- bind_rows(geo_specific_list)
cat(" →", format(nrow(geography_specific_data), big.mark = ","), "rows\n")

# Extract industry_specific_data
log_msg("  Extracting: industry-specific")
ind_specific_list <- lapply(extraction_params, function(p) {
  extract_industry_specific(p$complexity, p$level, nat_shares, industry_titles)
})
industry_specific_data <- bind_rows(ind_specific_list)
cat(" →", format(nrow(industry_specific_data), big.mark = ","), "rows\n")

# Extract industry space data
log_msg("  Extracting: industry space")
space_data <- lapply(extraction_params, function(p) {
  extract_industry_space_data(p$complexity, p$level, industry_titles)
})
industry_space_edges <- bind_rows(lapply(space_data, `[[`, "edges")) %>% select(geo_aggregation_level, from, to, weight)
industry_space_nodes <- bind_rows(lapply(space_data, `[[`, "nodes"))
cat(" → edges:", format(nrow(industry_space_edges), big.mark = ","), "| nodes:", format(nrow(industry_space_nodes), big.mark = ","), "\n")

log_msg("  Creating: complexity_diagnostics")
complexity_list <- list(county_complexity, state_complexity, cbsa_complexity, csa_complexity, cz_complexity)
complexity_diagnostics <- tibble(
  geo_aggregation_level = 1:5,
  geo_aggregation_name = c("county", "state", "cbsa", "csa", "commuting_zone"),
  n_geographies = sapply(complexity_list, function(x) x$diagnostics$n_geos),
  n_industries = sapply(complexity_list, function(x) x$diagnostics$n_inds),
  fill_rate_pct = sapply(complexity_list, function(x) round(x$diagnostics$fill_rate, 2)),
  first_eigenvalue = sapply(complexity_list, function(x) round(x$diagnostics$first_eigenvalue_C, 6)),
  second_eigenvalue = sapply(complexity_list, function(x) round(x$diagnostics$second_eigenvalue_C, 6)),
  ici_ubiquity_correlation = sapply(complexity_list, function(x) round(x$diagnostics$ici_ubiquity_correlation, 4)),
  eci_diversity_correlation = sapply(complexity_list, function(x) round(x$diagnostics$eci_diversity_correlation, 4)),
  eci_avgici_correlation = sapply(complexity_list, function(x) round(x$diagnostics$eci_avgici_correlation, 4)),
  ici_sign_flipped = sapply(complexity_list, function(x) x$diagnostics$ici_sign_flipped),
  eci_sign_flipped = sapply(complexity_list, function(x) x$diagnostics$eci_sign_flipped)
)
cat(" → 5 levels\n")

# Standardize geometry to MULTIPOLYGON
log_msg("  Standardizing geometries to MULTIPOLYGON...")
standardize_geometry <- function(sf_obj) sf_obj %>% st_cast("MULTIPOLYGON")
county_geometry <- standardize_geometry(county_geometry)
state_geometry <- standardize_geometry(state_geometry)
cbsa_geometry <- standardize_geometry(cbsa_geometry)
csa_geometry <- standardize_geometry(csa_geometry)
cz_geometry <- standardize_geometry(cz_geometry)
cat(" done\n")

# STEP 14: Validation Summary
log_step(14, "Final Validation Summary")

# Compact diagnostic output
print_header("COMPLEXITY VALIDATION")
cat("  ", trow("Level", "λ1", "λ2", "ICI-Ubiq", "ECI-Div", "ECI-AvgICI", "Fill%", widths = c(10, 8, 8, 10, 10, 12, 8)), "\n")
print_divider("─", 70)
level_names <- c("County", "State", "CBSA", "CSA", "CZ")
for (i in 1:5) {
  d <- complexity_list[[i]]$diagnostics
  cat("  ", trow(level_names[i], 
                 sprintf("%.4f", d$first_eigenvalue_C),
                 sprintf("%.4f", d$second_eigenvalue_C),
                 sprintf("%.3f", d$ici_ubiquity_correlation),
                 sprintf("%.3f", d$eci_diversity_correlation),
                 sprintf("%.4f", d$eci_avgici_correlation),
                 sprintf("%.1f%%", d$fill_rate),
                 widths = c(10, 8, 8, 10, 10, 12, 8)), "\n")
}
print_divider("─", 70)

# Quick pass/fail summary
checks <- sapply(complexity_list, function(x) {
  c(abs(x$diagnostics$first_eigenvalue_C - 1) < 1e-6,
    x$diagnostics$ici_ubiquity_correlation < 0,
    abs(x$diagnostics$eci_avgici_correlation) > 0.99)
})
pass_counts <- rowSums(checks)
cat("  Checks: λ1≈1 [", pass_counts[1], "/5] | ICI-Ubiq<0 [", pass_counts[2], "/5] | |ECI-AvgICI|>.99 [", pass_counts[3], "/5]\n", sep = "")

# Warn about sign anomalies (negative ECI-avgICI correlation indicates sign mismatch)
anomalies <- sapply(1:5, function(i) complexity_list[[i]]$diagnostics$eci_avgici_correlation < 0)
if (any(anomalies)) {
  anomaly_levels <- level_names[anomalies]
  cat("  ⚠ Sign anomaly detected at: ", paste(anomaly_levels, collapse = ", "), 
      " (ECI-avgICI < 0; may indicate high fill rate or small sample)\n", sep = "")
}

# DATA FRAME INVENTORY
print_header("DATA FRAMES FOR EXPORT")
data_frame_inventory <- tibble(
  Object = c("geo_aggregation_levels", "county_state_cbsa_csa_cz_crosswalk", "industry_titles",
             "county_geometry", "state_geometry", "cbsa_geometry", "csa_geometry", "cz_geometry",
             "geography_industry_complexity_pairs", "geography_specific_data", "industry_specific_data",
             "industry_space_edges", "industry_space_nodes", "complexity_diagnostics"),
  Type = c(rep("df", 3), rep("sf", 5), rep("df", 6))
) %>% rowwise() %>% mutate(Rows = if (exists(Object)) format(nrow(get(Object)), big.mark = ",") else "NA",
                           Cols = if (exists(Object)) ncol(get(Object)) else NA_integer_) %>% ungroup()

cat("  ", trow("Object", "Type", "Rows", "Cols", widths = c(38, 6, 12, 6)), "\n")
print_divider("─", 70)
for (i in 1:nrow(data_frame_inventory)) {
  cat("  ", trow(data_frame_inventory$Object[i], data_frame_inventory$Type[i], 
                 data_frame_inventory$Rows[i], data_frame_inventory$Cols[i], 
                 widths = c(38, 6, 12, 6)), "\n")
}
print_divider("─", 70)

#Glimpse geography_industry_complexity_pairs
cat("\n  Sample of 'geography_industry_complexity_pairs':\n")
glimpse(geography_industry_complexity_pairs)
 
#Data frames for reference
data_frames_for_reference <- c(
"geo_aggregation_levels",
"county_state_cbsa_csa_cz_crosswalk",
"industry_titles",
"county_geometry",
"state_geometry",
"cbsa_geometry",
"csa_geometry",
"cz_geometry",
"geography_industry_complexity_pairs",
"geography_specific_data",
"industry_specific_data",
"industry_space_edges",
"industry_space_nodes"
)

#Loop through each data frame in data_frames_for_reference and give a glimpse
for (df_name in data_frames_for_reference) {
  if (exists(df_name)) {
    cat("\n  Sample of '", df_name, "':\n", sep = "")
    str(get(df_name))
  } else {
    cat("\n  Data frame '", df_name, "' does not exist.\n", sep = "")
  }
}

# 1) Build a single “bundle” object
analysis_bundle <- list(
  metadata = list(
    method_version  = "LIGHTCAST EMPLOYMENT ANALYSIS - DABOIN METHOD v2.2",
    run_timestamp   = Sys.time(),
    source_file     = lightcast_file,
    n_cores         = n_cores,
    verbosity       = VERBOSITY
  ),
  
  # 2) Reference tables
  reference = list(
    geo_aggregation_levels              = geo_aggregation_levels,
    county_state_cbsa_csa_cz_crosswalk  = county_state_cbsa_csa_cz_crosswalk,
    industry_titles                     = industry_titles
  ),
  
  # 3) Geometry (sf objects)
  geometry = list(
    county_geometry = county_geometry,
    state_geometry  = state_geometry,
    cbsa_geometry   = cbsa_geometry,
    csa_geometry    = csa_geometry,
    cz_geometry     = cz_geometry
  ),
  
  # 4) Main output tables
  outputs = list(
    geography_industry_complexity_pairs = geography_industry_complexity_pairs,
    geography_specific_data             = geography_specific_data,
    industry_specific_data              = industry_specific_data,
    industry_space_edges                = industry_space_edges,
    industry_space_nodes                = industry_space_nodes,
    complexity_diagnostics              = complexity_diagnostics
  )
)

# 2) Save EVERYTHING into a single file
saveRDS(
  analysis_bundle,
  file = "lightcast_daboin_v2_2_bundle.rds",
  compress = "xz"   # "xz" = smallest but slower; "gzip" is a good middle ground
)

#Save specific objects to .RData
save(
  geo_aggregation_levels,
  county_state_cbsa_csa_cz_crosswalk,
  industry_titles,
  county_geometry,
  state_geometry,
  cbsa_geometry,
  csa_geometry,
  cz_geometry,
  geography_industry_complexity_pairs,
  geography_specific_data,
  industry_specific_data,
  industry_space_edges,
  industry_space_nodes,
  complexity_diagnostics,
  file = "lightcast_complexity_analysis.RData"
)

# To reload later:
load("lightcast_complexity_analysis.RData")

#Now attempt to display my county map in Albers USA


# STEP 15: Interactive Industry Feasibility Lookup
log_step(15, "Industry Feasibility Lookup")

# Function to print top 10 geographies by feasibility for a given industry
print_top10_feasibility <- function(industry_code_input, geo_ind_pairs, geo_names_list, industry_titles_df, 
                                    county_state_lookup_df, level_names_vec) {
  # Validate industry code
  if (!industry_code_input %in% industry_titles_df$industry_code) {
    cat("  ✗ Industry code '", industry_code_input, "' not found.\n", sep = "")
    return(invisible(NULL))
  }
  
  industry_desc <- industry_titles_df %>% filter(industry_code == industry_code_input) %>% pull(industry_description)
  cat("\n  Industry: ", industry_code_input, " - ", industry_desc, "\n", sep = "")
  print_divider("─", 70)
  
  # Get ICI for this industry at each level
  ind_data <- geo_ind_pairs %>% filter(industry_code == industry_code_input)
  
  for (lvl in 1:5) {
    lvl_name <- level_names_vec[lvl]
    lvl_data <- ind_data %>% filter(geo_aggregation_level == lvl) %>%
      arrange(desc(industry_feasibility)) %>% head(10)
    
    if (nrow(lvl_data) == 0) next
    
    # Get names
    geo_names_df <- geo_names_list[[lvl]]
    lvl_data <- lvl_data %>% left_join(geo_names_df, by = "geoid")
    
    # Add state for counties
    if (lvl == 1 && !is.null(county_state_lookup_df)) {
      lvl_data <- lvl_data %>% left_join(county_state_lookup_df, by = "geoid") %>%
        mutate(name = paste0(name, ", ", state_abbreviation))
    }
    
    cat("\n  ", toupper(lvl_name), " - Top 10 by Feasibility:\n", sep = "")
    cat("  ", trow("Rank", "Name", "Feasibility", "Pctl", "RCA", "StratGain", 
                   widths = c(5, 35, 12, 8, 8, 10)), "\n")
    
    for (i in 1:nrow(lvl_data)) {
      r <- lvl_data[i, ]
      cat("  ", trow(i, substr(r$name, 1, 34), 
                     sprintf("%.4f", r$industry_feasibility),
                     sprintf("%.1f", r$industry_feasibility_percentile_score),
                     sprintf("%.2f", r$location_quotient),
                     sprintf("%.3f", r$strategic_gain),
                     widths = c(5, 35, 12, 8, 8, 10)), "\n")
    }
  }
  print_divider("─", 70)
}

# Create lookup structures for interactive use
geo_names_list <- list(county_names, state_names, cbsa_names, csa_names, cz_names)
level_names_vec <- c("county", "state", "cbsa", "csa", "commuting_zone")

# Interactive industry lookup
INTERACTIVE_INDUSTRY_LOOKUP <- TRUE
if (INTERACTIVE_INDUSTRY_LOOKUP && interactive()) {
  cat("\n  Enter a 6-digit NAICS code to see top 10 geographies by feasibility.\n")
  cat("  Type 'list' to see available industries, 'q' to continue to export.\n\n")
  
  repeat {
    user_input <- readline(prompt = "  NAICS code (or 'q' to quit): ")
    user_input <- trimws(user_input)
    
    if (tolower(user_input) == "q" || user_input == "") break
    
    if (tolower(user_input) == "list") {
      cat("\n  Sample industries (", nrow(industry_titles), " total):\n", sep = "")
      sample_inds <- industry_titles %>% head(20)
      for (i in 1:nrow(sample_inds)) {
        cat("    ", sample_inds$industry_code[i], " - ", sample_inds$industry_description[i], "\n", sep = "")
      }
      cat("    ... (", nrow(industry_titles) - 20, " more)\n\n", sep = "")
      next
    }
    
    # Search by partial code or description
    if (nchar(user_input) < 6 || !grepl("^[0-9]+$", user_input)) {
      matches <- industry_titles %>% 
        filter(grepl(user_input, industry_code, ignore.case = TRUE) | 
                 grepl(user_input, industry_description, ignore.case = TRUE)) %>%
        head(10)
      if (nrow(matches) > 0) {
        cat("\n  Matches for '", user_input, "':\n", sep = "")
        for (i in 1:nrow(matches)) {
          cat("    ", matches$industry_code[i], " - ", matches$industry_description[i], "\n", sep = "")
        }
        cat("\n")
      } else {
        cat("  No matches found for '", user_input, "'\n", sep = "")
      }
      next
    }
    
    print_top10_feasibility(user_input, geography_industry_complexity_pairs, geo_names_list, 
                            industry_titles, county_state_lookup, level_names_vec)
  }
}

# ==============================================================================
# STEP 16: GitHub Export
# ==============================================================================

log_step(16, "Export and GitHub")

# --- Configuration ---
INTERACTIVE_EXPORT <- FALSE
SKIP_REGENERABLE_LARGE_FILES <- FALSE
SIMPLIFY_GEOMETRY <- TRUE
SIMPLIFY_TOLERANCE <- 0.001
MAX_FILE_SIZE_MB <- 100
MAX_GEOPARQUET_SIZE_MB <- 25

# --- Export object list ---
export_objects <- c(
  
  "geo_aggregation_levels",
  "county_state_cbsa_csa_cz_crosswalk",
  "industry_titles",
  "county_geometry",
  "state_geometry",
  "cbsa_geometry",
  "csa_geometry",
  "cz_geometry",
  "geography_industry_complexity_pairs",
  "geography_specific_data",
  "industry_specific_data",
  "industry_space_edges",
  "industry_space_nodes",
  "complexity_diagnostics"
)

# --- Interactive export review ---
if (INTERACTIVE_EXPORT && interactive()) {
  cat("\n")
  cat("================================================================================\n")
  cat("EXPORT REVIEW\n")
  cat("================================================================================\n")
  
  for (obj_name in export_objects) {
    if (exists(obj_name)) {
      cat("\n", obj_name, ":\n", sep = "")
      glimpse(get(obj_name))
    }
  }
  
  proceed <- readline(prompt = "Proceed with GitHub export? (y/n): ")
  if (tolower(substr(proceed, 1, 1)) != "y") {
    cat("Export cancelled.\n")
    stop("Export cancelled", call. = FALSE)
  }
}

# --- GitHub token retrieval ---
github_token <- Sys.getenv("GITHUB_PAT")
if (github_token == "") {
  github_token <- Sys.getenv("GITHUB_TOKEN")
}

if (github_token == "") {
  cat("No GitHub PAT found. Skipping export.\n")
} else {
  
  # --- Repository configuration ---
  owner <- "bsf-rmi"
  repo <- "BSF_RMI_2026"
  branch <- "main"
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  export_folder <- paste0("lightcast_2024_complexity_naics6d_", timestamp)
  
  # --- Local export directory ---
  local_export_dir <- file.path(tempdir(), export_folder)
  dir.create(local_export_dir, showWarnings = FALSE, recursive = TRUE)
  
  # --- Large files tracker (environment for proper scoping) ---
  large_files_env <- new.env(parent = emptyenv())
  large_files_env$files <- list()
  
  # ===========================================================================
  # Helper function: Upload file to GitHub
  # ===========================================================================
  upload_to_github <- function(local_path, github_path, message, max_retries = 3) {
    
    file_size_mb <- file.info(local_path)$size / (1024 * 1024)
    is_geofile <- grepl("\\.(geoparquet|gpkg|rds)$", local_path, ignore.case = TRUE)
    size_limit <- if (is_geofile) MAX_GEOPARQUET_SIZE_MB else MAX_FILE_SIZE_MB
    
    # Check file size limit
    if (file_size_mb > size_limit) {
      large_files_env$files[[basename(local_path)]] <- list(
        local_path = local_path,
        filename = basename(local_path),
        size_mb = round(file_size_mb, 1)
      )
      return("LARGE")
    }
    
    # Encode file content
    content <- base64enc::base64encode(local_path)
    check_url <- paste0(
      "https://api.github.com/repos/", owner, "/", repo, "/contents/", github_path
    )
    
    for (attempt in seq_len(max_retries)) {
      result <- tryCatch({
        
        # Check if file already exists
        check_response <- httr::GET(
          check_url,
          httr::add_headers(
            Authorization = paste("token", github_token),
            Accept = "application/vnd.github.v3+json"
          ),
          httr::timeout(60)
        )
        
        # Build request body
        body <- list(
          message = message,
          content = content,
          branch = branch
        )
        
        if (httr::status_code(check_response) == 200) {
          body$sha <- httr::content(check_response)$sha
        }
        
        # Upload file
        response <- httr::PUT(
          check_url,
          httr::add_headers(
            Authorization = paste("token", github_token),
            Accept = "application/vnd.github.v3+json"
          ),
          body = jsonlite::toJSON(body, auto_unbox = TRUE),
          encode = "raw",
          httr::content_type_json(),
          httr::timeout(120)
        )
        
        if (httr::status_code(response) %in% c(200, 201)) {
          return(TRUE)
        }
        
        NULL
        
      }, error = function(e) {
        NULL
      })
      
      if (isTRUE(result)) {
        return(TRUE)
      }
      
      if (attempt < max_retries) {
        Sys.sleep(2 * attempt)
      }
    }
    
    return(FALSE)
  }
  
  # ===========================================================================
  # Helper function: Create GitHub release
  # ===========================================================================
  create_github_release <- function(tag_name, release_name, body_text, max_retries = 3) {
    
    release_url <- paste0(
      "https://api.github.com/repos/", owner, "/", repo, "/releases"
    )
    
    for (attempt in seq_len(max_retries)) {
      result <- tryCatch({
        
        response <- httr::POST(
          release_url,
          httr::add_headers(
            Authorization = paste("token", github_token),
            Accept = "application/vnd.github.v3+json"
          ),
          body = jsonlite::toJSON(
            list(
              tag_name = tag_name,
              name = release_name,
              body = body_text,
              draft = FALSE,
              prerelease = FALSE
            ),
            auto_unbox = TRUE
          ),
          encode = "raw",
          httr::content_type_json(),
          httr::timeout(60)
        )
        
        if (httr::status_code(response) %in% c(200, 201)) {
          return(httr::content(response))
        }
        
        # If release already exists, try to get it
        get_url <- paste0(
          "https://api.github.com/repos/", owner, "/", repo,
          "/releases/tags/", tag_name
        )
        
        get_response <- httr::GET(
          get_url,
          httr::add_headers(
            Authorization = paste("token", github_token),
            Accept = "application/vnd.github.v3+json"
          ),
          httr::timeout(60)
        )
        
        if (httr::status_code(get_response) == 200) {
          return(httr::content(get_response))
        }
        
        NULL
        
      }, error = function(e) {
        NULL
      })
      
      if (!is.null(result)) {
        return(result)
      }
      
      if (attempt < max_retries) {
        Sys.sleep(2 * attempt)
      }
    }
    
    return(NULL)
  }
  
  # ===========================================================================
  # Helper function: Upload release asset
  # ===========================================================================
  upload_release_asset <- function(release_upload_url, local_path, filename,
                                   max_retries = 3) {
    
    upload_url <- paste0(
      gsub("\\{.*\\}", "", release_upload_url),
      "?name=", URLencode(filename)
    )
    
    for (attempt in seq_len(max_retries)) {
      result <- tryCatch({
        
        response <- httr::POST(
          upload_url,
          httr::add_headers(
            Authorization = paste("token", github_token),
            Accept = "application/vnd.github.v3+json",
            `Content-Type` = "application/octet-stream"
          ),
          body = httr::upload_file(local_path),
          httr::timeout(600)
        )
        
        if (httr::status_code(response) %in% c(200, 201)) {
          return(TRUE)
        }
        
        NULL
        
      }, error = function(e) {
        NULL
      })
      
      if (isTRUE(result)) {
        return(TRUE)
      }
      
      if (attempt < max_retries) {
        Sys.sleep(5 * attempt)
      }
    }
    
    return(FALSE)
  }
  
  # ===========================================================================
  # Main export loop
  # ===========================================================================
  cat("\nExporting data frames...\n")
  
  export_success <- 0
  export_failed <- 0
  
  for (obj_name in export_objects) {
    
    if (!exists(obj_name)) {
      next
    }
    
    obj <- get(obj_name)
    is_sf <- inherits(obj, "sf")
    
    export_result <- tryCatch({
      
      # === RDS export (all objects) ===
      rds_file <- file.path(local_export_dir, paste0(obj_name, ".rds"))
      saveRDS(obj, rds_file)
      
      rds_result <- upload_to_github(
        rds_file,
        paste0(export_folder, "/", obj_name, ".rds"),
        paste("Add", obj_name, "(RDS)")
      )
      
      if (is_sf) {
        # === Geodata processing ===
        obj_processed <- obj
        
        # Simplify geometry if enabled
        if (SIMPLIFY_GEOMETRY) {
          obj_processed <- tryCatch({
            suppressWarnings(
              st_simplify(obj_processed, dTolerance = SIMPLIFY_TOLERANCE,
                          preserveTopology = TRUE)
            )
          }, error = function(e) obj_processed)
        }
        
        # Validate geometry
        obj_processed <- tryCatch({
          if (any(!st_is_valid(obj_processed))) {
            st_make_valid(obj_processed)
          } else {
            obj_processed
          }
        }, error = function(e) obj_processed)
        
        # === Geoparquet ===
        geoparquet_file <- file.path(local_export_dir, paste0(obj_name, ".geoparquet"))
        suppressWarnings(sfarrow::st_write_parquet(obj_processed, geoparquet_file))
        suppressWarnings(gc(verbose = FALSE))
        
        geoparquet_result <- upload_to_github(
          geoparquet_file,
          paste0(export_folder, "/", obj_name, ".geoparquet"),
          paste("Add", obj_name, "(geoparquet)")
        )
        
        # === Geopackage ===
        gpkg_file <- file.path(local_export_dir, paste0(obj_name, ".gpkg"))
        suppressWarnings(
          st_write(obj_processed, gpkg_file, layer = obj_name,
                   delete_dsn = TRUE, quiet = TRUE)
        )
        
        gpkg_result <- upload_to_github(
          gpkg_file,
          paste0(export_folder, "/", obj_name, ".gpkg"),
          paste("Add", obj_name, "(gpkg)")
        )
        
        # Report status (use geoparquet as primary indicator)
        if (identical(geoparquet_result, "LARGE")) {
          cat("  \U25D0", obj_name, "(-> Release)\n")
          "success"
        } else if (isTRUE(geoparquet_result)) {
          cat("  \U2713", obj_name, "\n")
          "success"
        } else {
          cat("  \U2717", obj_name, "(upload failed)\n")
          "failed"
        }
        
      } else {
        # === Non-geodata processing ===
        
        # === Parquet ===
        parquet_file <- file.path(local_export_dir, paste0(obj_name, ".parquet"))
        arrow::write_parquet(obj, parquet_file)
        
        parquet_result <- upload_to_github(
          parquet_file,
          paste0(export_folder, "/", obj_name, ".parquet"),
          paste("Add", obj_name, "(parquet)")
        )
        
        # === CSV ===
        csv_file <- file.path(local_export_dir, paste0(obj_name, ".csv"))
        data.table::fwrite(obj, csv_file)
        
        csv_result <- upload_to_github(
          csv_file,
          paste0(export_folder, "/", obj_name, ".csv"),
          paste("Add", obj_name, "(csv)")
        )
        
        # === CSV.gz ===
        csvgz_file <- file.path(local_export_dir, paste0(obj_name, ".csv.gz"))
        data.table::fwrite(obj, csvgz_file, compress = "gzip")
        
        csvgz_result <- upload_to_github(
          csvgz_file,
          paste0(export_folder, "/", obj_name, ".csv.gz"),
          paste("Add", obj_name, "(csv.gz)")
        )
        
        # Report status (use parquet as primary indicator)
        if (identical(parquet_result, "LARGE")) {
          cat("  \U25D0", obj_name, "(-> Release)\n")
          "success"
        } else if (isTRUE(parquet_result)) {
          cat("  \U2713", obj_name, "\n")
          "success"
        } else {
          # Upload failed - add all formats to large_files for Release upload
          cat("  \U25D0", obj_name, "(upload failed -> Release)\n")
          
          large_files_env$files[[paste0(obj_name, ".parquet")]] <- list(
            local_path = parquet_file,
            filename = paste0(obj_name, ".parquet"),
            size_mb = round(file.info(parquet_file)$size / (1024^2), 1)
          )
          large_files_env$files[[paste0(obj_name, ".csv")]] <- list(
            local_path = csv_file,
            filename = paste0(obj_name, ".csv"),
            size_mb = round(file.info(csv_file)$size / (1024^2), 1)
          )
          large_files_env$files[[paste0(obj_name, ".csv.gz")]] <- list(
            local_path = csvgz_file,
            filename = paste0(obj_name, ".csv.gz"),
            size_mb = round(file.info(csvgz_file)$size / (1024^2), 1)
          )
          
          "success"
        }
      }
      
    }, error = function(e) {
      cat("  \U2717", obj_name, ":", conditionMessage(e), "\n")
      "failed"
    })
    
    if (identical(export_result, "success")) {
      export_success <- export_success + 1
    } else {
      export_failed <- export_failed + 1
    }
  }
  
  # ===========================================================================
  # README generation and upload
  # ===========================================================================
  readme_content <- paste0(
    "# Lightcast 2024 Economic Complexity Analysis\n\n",
    "Generated: ", Sys.time(), "\n\n",
    "Methodology: Daboin et al.\n\n",
    "## File Formats\n",
    "Each data frame is exported in multiple formats:\n",
    "- **RDS**: R native format (all objects)\n",
    "- **Parquet**: Efficient columnar format (non-geo)\n",
    "- **CSV / CSV.gz**: Universal interchange (non-geo)\n",
    "- **Geoparquet**: Efficient geo format (geo)\n",
    "- **Geopackage (.gpkg)**: OGC standard geo format (geo)\n\n",
    "## Data Frames\n",
    "- `geography_industry_complexity_pairs`: All geo-industry pairs with LQ, density, strategic gain\n",
    "- `geography_specific_data`: ECI, diversity, strategic index by geography\n",
    "- `industry_specific_data`: ICI, ubiquity by industry\n",
    "- `industry_space_edges/nodes`: Network data for industry co-location visualization\n",
    "- `*_geometry`: Standardized MULTIPOLYGON boundaries for mapping\n"
  )
  
  local_readme <- file.path(local_export_dir, "README.md")
  writeLines(readme_content, local_readme)
  upload_to_github(local_readme, paste0(export_folder, "/README.md"), "Add README")
  
  # ===========================================================================
  # Upload large files to Release
  # ===========================================================================
  large_files <- large_files_env$files
  
  if (length(large_files) > 0) {
    cat("\nUploading large files to Release...\n")
    
    release <- create_github_release(
      paste0("v", timestamp),
      paste0("Lightcast Data - ", format(Sys.time(), "%Y-%m-%d")),
      "Large data files"
    )
    
    if (!is.null(release)) {
      for (file_info in large_files) {
        result <- upload_release_asset(
          release$upload_url,
          file_info$local_path,
          file_info$filename
        )
        
        if (isTRUE(result)) {
          cat("  \U2713", file_info$filename, "(Release)\n")
        } else {
          cat("  \U2717", file_info$filename, "(Release upload failed)\n")
          export_failed <- export_failed + 1
          export_success <- export_success - 1
        }
      }
    } else {
      cat("  \U2717 Could not create release\n")
      export_failed <- export_failed + length(large_files)
      export_success <- export_success - length(large_files)
    }
  }
  
  # ===========================================================================
  # Export summary
  # ===========================================================================
  cat("\nExport summary: Success=", export_success, " Failed=", export_failed, "\n")
  cat(
    "GitHub folder: https://github.com/", owner, "/", repo, "/tree/",
    branch, "/", export_folder, "\n",
    sep = ""
  )
  
  # ===========================================================================
  # Cleanup temporary files
  # ===========================================================================
  tryCatch({
    files_to_remove <- list.files(local_export_dir, full.names = TRUE)
    for (f in files_to_remove) {
      tryCatch(file.remove(f), error = function(e) NULL)
    }
    unlink(local_export_dir, recursive = TRUE, force = TRUE)
  }, error = function(e) NULL)
  
  suppressWarnings(gc(verbose = FALSE))
  
}

# ==============================================================================
# STEP 17: Download Verification (optional)
# ==============================================================================

log_step(17, "Download Verification")

RUN_DOWNLOAD_TEST <- FALSE

if (RUN_DOWNLOAD_TEST && exists("github_token") && github_token != "") {
  cat("Download verification would run here...\n")
}

suppressWarnings(gc(verbose = FALSE))

# ==============================================================================
# Final run summary
# ==============================================================================

print_header("RUN SUMMARY")

cat(
  "  ",
  kv("Total geographies", paste(sapply(complexity_list, function(x) {
    x$diagnostics$n_geos
  }), collapse = " / ")),
  "\n"
)
cat("  ", kv("Industries", complexity_list[[1]]$diagnostics$n_inds), "\n")
cat(
  "  ",
  kv("Geo-industry pairs", format(nrow(geography_industry_complexity_pairs), big.mark = ",")),
  "\n"
)
cat(
  "  ",
  kv("Industry space edges", format(nrow(industry_space_edges), big.mark = ",")),
  "\n"
)
cat(
  "  ",
  kv("Validation", if (all(pass_counts == 5)) "\U2713 All checks passed" else "\U26A0 Some checks failed"),
  "\n"
)

print_divider("\U2500", 70)
cat("\n\U2713 SCRIPT COMPLETE\n")

# ==============================================================================
# STEP 16a: Local Export to Working Directory
# ==============================================================================

log_step("16a", "Local Export to Working Directory")

# --- Create timestamped output folder ---
local_timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
local_output_folder <- file.path(
  getwd(),
  paste0("lightcast_2024_complexity_naics6d_", local_timestamp)
)
dir.create(local_output_folder, showWarnings = FALSE, recursive = TRUE)

cat("  Output folder:", local_output_folder, "\n\n")

# --- Export counters ---
local_export_success <- 0
local_export_failed <- 0

# --- Main export loop ---
for (obj_name in export_objects) {
  
  if (!exists(obj_name)) {
    cat("  \U26A0", obj_name, "- not found, skipping\n")
    next
  }
  
  obj <- get(obj_name)
  is_sf <- inherits(obj, "sf")
  
  export_result <- tryCatch({
    
    # === RDS export (all objects) ===
    rds_file <- file.path(local_output_folder, paste0(obj_name, ".rds"))
    saveRDS(obj, rds_file)
    
    if (is_sf) {
      # === Geodata: simplify and validate if enabled ===
      obj_export <- obj
      
      if (exists("SIMPLIFY_GEOMETRY") && isTRUE(SIMPLIFY_GEOMETRY)) {
        obj_export <- tryCatch({
          tol <- if (exists("SIMPLIFY_TOLERANCE")) SIMPLIFY_TOLERANCE else 0.001
          suppressWarnings(
            st_simplify(obj_export, dTolerance = tol, preserveTopology = TRUE)
          )
        }, error = function(e) obj_export)
      }
      
      obj_export <- tryCatch({
        if (any(!st_is_valid(obj_export))) {
          st_make_valid(obj_export)
        } else {
          obj_export
        }
      }, error = function(e) obj_export)
      
      # === Geoparquet ===
      geoparquet_file <- file.path(local_output_folder, paste0(obj_name, ".geoparquet"))
      suppressWarnings(sfarrow::st_write_parquet(obj_export, geoparquet_file))
      
      # === Geopackage ===
      gpkg_file <- file.path(local_output_folder, paste0(obj_name, ".gpkg"))
      suppressWarnings(
        st_write(obj_export, gpkg_file, layer = obj_name,
                 delete_dsn = TRUE, quiet = TRUE)
      )
      
      file_sizes <- paste0(
        "rds:", round(file.info(rds_file)$size / 1024^2, 1), "MB | ",
        "geoparquet:", round(file.info(geoparquet_file)$size / 1024^2, 1), "MB | ",
        "gpkg:", round(file.info(gpkg_file)$size / 1024^2, 1), "MB"
      )
      
      cat("  \U2713", obj_name, "(geo) -", file_sizes, "\n")
      
    } else {
      # === Non-geodata ===
      
      # === Parquet ===
      parquet_file <- file.path(local_output_folder, paste0(obj_name, ".parquet"))
      arrow::write_parquet(obj, parquet_file)
      
      # === CSV ===
      csv_file <- file.path(local_output_folder, paste0(obj_name, ".csv"))
      data.table::fwrite(obj, csv_file)
      
      # === CSV.gz ===
      csvgz_file <- file.path(local_output_folder, paste0(obj_name, ".csv.gz"))
      data.table::fwrite(obj, csvgz_file, compress = "gzip")
      
      file_sizes <- paste0(
        "rds:", round(file.info(rds_file)$size / 1024^2, 1), "MB | ",
        "parquet:", round(file.info(parquet_file)$size / 1024^2, 1), "MB | ",
        "csv:", round(file.info(csv_file)$size / 1024^2, 1), "MB | ",
        "csv.gz:", round(file.info(csvgz_file)$size / 1024^2, 1), "MB"
      )
      
      cat("  \U2713", obj_name, "-", file_sizes, "\n")
    }
    
    suppressWarnings(gc(verbose = FALSE))
    "success"
    
  }, error = function(e) {
    cat("  \U2717", obj_name, ":", conditionMessage(e), "\n")
    "failed"
  })
  
  if (identical(export_result, "success")) {
    local_export_success <- local_export_success + 1
  } else {
    local_export_failed <- local_export_failed + 1
  }
}

# === README ===
readme_content <- paste0(
  "# Lightcast 2024 Economic Complexity Analysis\n\n",
  "Generated: ", Sys.time(), "\n",
  "Output folder: ", basename(local_output_folder), "\n\n",
  "## Methodology\n",
  "Daboin et al. \"Economic Complexity and Technological Relatedness: ",
  "Findings for American Cities\"\n\n",
  "## File Formats\n\n",
  "### Non-geographic data frames:\n",
  "- `.rds` - R native format (preserves all R attributes)\n",
  "- `.parquet` - Efficient columnar format (cross-platform)\n",
  "- `.csv` - Universal text format\n",
  "- `.csv.gz` - Compressed CSV\n\n",
  "### Geographic data frames:\n",
  "- `.rds` - R native format\n",
  "- `.geoparquet` - Efficient geo format (recommended for large files)\n",
  "- `.gpkg` - OGC GeoPackage standard\n\n",
  "## Data Frames\n\n",
  "### Core complexity outputs:\n",
  "- `geography_industry_complexity_pairs` - All geo-industry pairs with LQ, density, strategic gain\n",
  "- `geography_specific_data` - ECI, diversity, strategic index by geography\n",
  "- `industry_specific_data` - ICI, ubiquity by industry\n\n",
  "### Industry space network:\n",
  "- `industry_space_edges` - Proximity-weighted edges between industries\n",
  "- `industry_space_nodes` - Industry attributes (ICI, ubiquity, centrality)\n\n",
  "### Reference tables:\n",
  "- `geo_aggregation_levels` - Level codes (1=county, 2=state, etc.)\n",
  "- `county_state_cbsa_csa_cz_crosswalk` - Geographic hierarchy crosswalk\n",
  "- `industry_titles` - NAICS codes and descriptions\n",
  "- `complexity_diagnostics` - Validation metrics by level\n\n",
  "### Geometry files:\n",
  "- `county_geometry`, `state_geometry`, `cbsa_geometry`, `csa_geometry`, `cz_geometry`\n",
  "- All standardized to MULTIPOLYGON\n"
)

readme_file <- file.path(local_output_folder, "README.md")
writeLines(readme_content, readme_file)
cat("  \U2713 README.md\n")

# === Summary manifest ===
manifest <- data.frame(
  filename = list.files(local_output_folder),
  size_mb = round(
    file.info(list.files(local_output_folder, full.names = TRUE))$size / 1024^2,
    2
  ),
  stringsAsFactors = FALSE
)
manifest <- manifest[order(-manifest$size_mb), ]

manifest_file <- file.path(local_output_folder, "MANIFEST.csv")
write.csv(manifest, manifest_file, row.names = FALSE)
cat("  \U2713 MANIFEST.csv\n")

# === Final summary ===
total_size_mb <- sum(
  file.info(list.files(local_output_folder, full.names = TRUE))$size
) / 1024^2
n_files <- length(list.files(local_output_folder))

print_divider("\U2500", 70)
cat("\n  Local export complete:\n")
cat("  ", kv("Folder", local_output_folder), "\n")
cat(
  "  ",
  kv("Objects exported", paste0(local_export_success, " success / ", local_export_failed, " failed")),
  "\n"
)
cat("  ", kv("Total files", n_files), "\n")
cat("  ", kv("Total size", paste0(round(total_size_mb, 1), " MB")), "\n")
print_divider("\U2500", 70)

# Store path for later reference
LOCAL_EXPORT_PATH <- local_output_folder

# ==============================================================================
# STEP 18: GitHub Repository Cleanup (Interactive)
# ==============================================================================

log_step(18, "GitHub Repository Cleanup")

INTERACTIVE_CLEANUP <- TRUE

if (INTERACTIVE_CLEANUP && interactive() && exists("github_token") && github_token != "") {
  
  cat("\n")
  cat("================================================================================\n")
  cat("GITHUB REPOSITORY CLEANUP\n")
  cat("================================================================================\n")
  cat("\nThis will DELETE all files in the repository EXCEPT:\n")
  cat("  - The most recent export folder: ", export_folder, "\n", sep = "")
  cat("\nThis includes:\n")
  cat("  - All other folders/files in the repo\n")
  cat("  - All old Release assets\n")
  cat("\n\U26A0\UFE0F This action is IRREVERSIBLE.\n\n")
  
  proceed_cleanup <- readline(prompt = "Do you want to proceed with cleanup? (yes/no): ")
  
  if (tolower(trimws(proceed_cleanup)) == "yes") {
    
    # =========================================================================
    # Helper function: Get all files in repo recursively
    # =========================================================================
    get_repo_contents_recursive <- function(path = "", max_retries = 3) {
      
      all_items <- list()
      
      fetch_path <- function(p) {
        url <- paste0(
          "https://api.github.com/repos/", owner, "/", repo, "/contents/",
          p, "?ref=", branch
        )
        
        for (attempt in seq_len(max_retries)) {
          result <- tryCatch({
            response <- httr::GET(
              url,
              httr::add_headers(
                Authorization = paste("token", github_token),
                Accept = "application/vnd.github.v3+json"
              ),
              httr::timeout(60)
            )
            
            if (httr::status_code(response) == 200) {
              return(httr::content(response))
            } else if (httr::status_code(response) == 404) {
              return(list())
            }
            
            NULL
          }, error = function(e) NULL)
          
          if (!is.null(result)) {
            return(result)
          }
          
          if (attempt < max_retries) {
            Sys.sleep(2 * attempt)
          }
        }
        
        list()
      }
      
      items <- fetch_path(path)
      
      if (length(items) == 0) {
        return(list())
      }
      
      for (item in items) {
        if (item$type == "file") {
          all_items[[length(all_items) + 1]] <- list(
            path = item$path,
            sha = item$sha,
            type = "file"
          )
        } else if (item$type == "dir") {
          all_items[[length(all_items) + 1]] <- list(
            path = item$path,
            sha = item$sha,
            type = "dir"
          )
          # Recurse into subdirectory
          sub_items <- get_repo_contents_recursive(item$path, max_retries)
          all_items <- c(all_items, sub_items)
        }
      }
      
      all_items
    }
    
    # =========================================================================
    # Helper function: Delete a file from repo
    # =========================================================================
    delete_github_file <- function(file_path, sha, max_retries = 3) {
      
      url <- paste0(
        "https://api.github.com/repos/", owner, "/", repo, "/contents/", file_path
      )
      
      for (attempt in seq_len(max_retries)) {
        result <- tryCatch({
          response <- httr::DELETE(
            url,
            httr::add_headers(
              Authorization = paste("token", github_token),
              Accept = "application/vnd.github.v3+json"
            ),
            body = jsonlite::toJSON(
              list(
                message = paste("Cleanup: delete", basename(file_path)),
                sha = sha,
                branch = branch
              ),
              auto_unbox = TRUE
            ),
            encode = "raw",
            httr::content_type_json(),
            httr::timeout(60)
          )
          
          if (httr::status_code(response) == 200) {
            return(TRUE)
          }
          
          NULL
        }, error = function(e) NULL)
        
        if (isTRUE(result)) {
          return(TRUE)
        }
        
        if (attempt < max_retries) {
          Sys.sleep(1 * attempt)
        }
      }
      
      FALSE
    }
    
    # =========================================================================
    # Helper function: Get all releases
    # =========================================================================
    get_all_releases <- function(max_retries = 3) {
      
      url <- paste0(
        "https://api.github.com/repos/", owner, "/", repo, "/releases"
      )
      
      for (attempt in seq_len(max_retries)) {
        result <- tryCatch({
          response <- httr::GET(
            url,
            httr::add_headers(
              Authorization = paste("token", github_token),
              Accept = "application/vnd.github.v3+json"
            ),
            httr::timeout(60)
          )
          
          if (httr::status_code(response) == 200) {
            return(httr::content(response))
          }
          
          NULL
        }, error = function(e) NULL)
        
        if (!is.null(result)) {
          return(result)
        }
        
        if (attempt < max_retries) {
          Sys.sleep(2 * attempt)
        }
      }
      
      list()
    }
    
    # =========================================================================
    # Helper function: Delete a release
    # =========================================================================
    delete_release <- function(release_id, max_retries = 3) {
      
      url <- paste0(
        "https://api.github.com/repos/", owner, "/", repo, "/releases/", release_id
      )
      
      for (attempt in seq_len(max_retries)) {
        result <- tryCatch({
          response <- httr::DELETE(
            url,
            httr::add_headers(
              Authorization = paste("token", github_token),
              Accept = "application/vnd.github.v3+json"
            ),
            httr::timeout(60)
          )
          
          if (httr::status_code(response) == 204) {
            return(TRUE)
          }
          
          NULL
        }, error = function(e) NULL)
        
        if (isTRUE(result)) {
          return(TRUE)
        }
        
        if (attempt < max_retries) {
          Sys.sleep(1 * attempt)
        }
      }
      
      FALSE
    }
    
    # =========================================================================
    # Helper function: Delete a release asset
    # =========================================================================
    delete_release_asset <- function(asset_id, max_retries = 3) {
      
      url <- paste0(
        "https://api.github.com/repos/", owner, "/", repo, "/releases/assets/", asset_id
      )
      
      for (attempt in seq_len(max_retries)) {
        result <- tryCatch({
          response <- httr::DELETE(
            url,
            httr::add_headers(
              Authorization = paste("token", github_token),
              Accept = "application/vnd.github.v3+json"
            ),
            httr::timeout(60)
          )
          
          if (httr::status_code(response) == 204) {
            return(TRUE)
          }
          
          NULL
        }, error = function(e) NULL)
        
        if (isTRUE(result)) {
          return(TRUE)
        }
        
        if (attempt < max_retries) {
          Sys.sleep(1 * attempt)
        }
      }
      
      FALSE
    }
    
    # =========================================================================
    # Helper function: Delete a git tag
    # =========================================================================
    delete_git_tag <- function(tag_name, max_retries = 3) {
      
      url <- paste0(
        "https://api.github.com/repos/", owner, "/", repo, "/git/refs/tags/", tag_name
      )
      
      for (attempt in seq_len(max_retries)) {
        result <- tryCatch({
          response <- httr::DELETE(
            url,
            httr::add_headers(
              Authorization = paste("token", github_token),
              Accept = "application/vnd.github.v3+json"
            ),
            httr::timeout(60)
          )
          
          if (httr::status_code(response) == 204) {
            return(TRUE)
          }
          
          NULL
        }, error = function(e) NULL)
        
        if (isTRUE(result)) {
          return(TRUE)
        }
        
        if (attempt < max_retries) {
          Sys.sleep(1 * attempt)
        }
      }
      
      FALSE
    }
    
    # =========================================================================
    # PHASE 1: Inventory what will be deleted
    # =========================================================================
    cat("\n[Phase 1] Scanning repository contents...\n")
    
    all_repo_items <- get_repo_contents_recursive("")
    
    # Identify the current release tag (from this run)
    current_release_tag <- paste0("v", timestamp)
    
    # Separate items to keep vs delete
    items_to_delete <- list()
    items_to_keep <- list()
    
    for (item in all_repo_items) {
      is_in_current_folder <- (
        startsWith(item$path, paste0(export_folder, "/")) ||
          item$path == export_folder
      )
      
      if (is_in_current_folder) {
        items_to_keep[[length(items_to_keep) + 1]] <- item
      } else {
        items_to_delete[[length(items_to_delete) + 1]] <- item
      }
    }
    
    # Get releases
    cat("[Phase 1] Scanning releases...\n")
    
    all_releases <- get_all_releases()
    releases_to_delete <- list()
    releases_to_keep <- list()
    
    for (rel in all_releases) {
      if (rel$tag_name == current_release_tag) {
        releases_to_keep[[length(releases_to_keep) + 1]] <- rel
      } else {
        releases_to_delete[[length(releases_to_delete) + 1]] <- rel
      }
    }
    
    # Count assets in releases to delete
    total_assets_to_delete <- sum(sapply(releases_to_delete, function(r) {
      length(r$assets)
    }))
    
    # =========================================================================
    # PHASE 2: Show summary and confirm
    # =========================================================================
    cat("\n")
    cat("================================================================================\n")
    cat("CLEANUP SUMMARY\n")
    cat("================================================================================\n")
    
    files_to_delete <- Filter(function(x) x$type == "file", items_to_delete)
    dirs_to_delete <- Filter(function(x) x$type == "dir", items_to_delete)
    
    cat("\n  WILL BE DELETED:\n")
    cat("    Repository files:   ", length(files_to_delete), "\n")
    cat("    Repository folders: ", length(dirs_to_delete), "\n")
    cat("    Releases:           ", length(releases_to_delete), "\n")
    cat("    Release assets:     ", total_assets_to_delete, "\n")
    
    cat("\n  WILL BE KEPT:\n")
    cat("    Export folder:      ", export_folder, "\n")
    cat("    Files in folder:    ", length(Filter(function(x) x$type == "file", items_to_keep)), "\n")
    cat(
      "    Current release:    ",
      if (length(releases_to_keep) > 0) current_release_tag else "(none)",
      "\n"
    )
    
    # Show some examples of what will be deleted
    if (length(files_to_delete) > 0) {
      cat("\n  Sample files to delete (first 10):\n")
      for (i in seq_len(min(10, length(files_to_delete)))) {
        cat("    - ", files_to_delete[[i]]$path, "\n", sep = "")
      }
      if (length(files_to_delete) > 10) {
        cat("    ... and ", length(files_to_delete) - 10, " more files\n", sep = "")
      }
    }
    
    if (length(releases_to_delete) > 0) {
      cat("\n  Releases to delete:\n")
      for (rel in releases_to_delete) {
        n_assets <- length(rel$assets)
        cat("    - ", rel$tag_name, " (", rel$name, ") - ", n_assets, " assets\n", sep = "")
      }
    }
    
    cat("\n")
    
    if (length(files_to_delete) == 0 && length(releases_to_delete) == 0) {
      cat("  \U2713 Nothing to delete - repository is clean!\n")
    } else {
      
      final_confirm <- readline(prompt = "Type 'DELETE' to confirm deletion: ")
      
      if (toupper(trimws(final_confirm)) == "DELETE") {
        
        # =====================================================================
        # PHASE 3: Delete repository files
        # =====================================================================
        if (length(files_to_delete) > 0) {
          cat("\n[Phase 3] Deleting repository files...\n")
          
          delete_success <- 0
          delete_failed <- 0
          
          for (i in seq_along(files_to_delete)) {
            item <- files_to_delete[[i]]
            result <- delete_github_file(item$path, item$sha)
            
            if (isTRUE(result)) {
              delete_success <- delete_success + 1
              cat("  \U2713 Deleted: ", item$path, "\n", sep = "")
            } else {
              delete_failed <- delete_failed + 1
              cat("  \U2717 Failed: ", item$path, "\n", sep = "")
            }
            
            # Progress indicator every 10 files
            if (i %% 10 == 0) {
              cat("    ... ", i, "/", length(files_to_delete), " files processed\n", sep = "")
            }
            
            # Rate limiting - pause briefly between deletions
            Sys.sleep(0.3)
          }
          
          cat(
            "\n  File deletion summary: ", delete_success, " deleted, ",
            delete_failed, " failed\n",
            sep = ""
          )
        }
        
        # =====================================================================
        # PHASE 4: Delete releases and their assets
        # =====================================================================
        if (length(releases_to_delete) > 0) {
          cat("\n[Phase 4] Deleting releases...\n")
          
          release_delete_success <- 0
          release_delete_failed <- 0
          asset_delete_success <- 0
          asset_delete_failed <- 0
          
          for (rel in releases_to_delete) {
            cat("  Processing release: ", rel$tag_name, "\n", sep = "")
            
            # First delete all assets in this release
            if (length(rel$assets) > 0) {
              for (asset in rel$assets) {
                result <- delete_release_asset(asset$id)
                
                if (isTRUE(result)) {
                  asset_delete_success <- asset_delete_success + 1
                  cat("    \U2713 Deleted asset: ", asset$name, "\n", sep = "")
                } else {
                  asset_delete_failed <- asset_delete_failed + 1
                  cat("    \U2717 Failed asset: ", asset$name, "\n", sep = "")
                }
                
                Sys.sleep(0.3)
              }
            }
            
            # Now delete the release itself
            result <- delete_release(rel$id)
            
            if (isTRUE(result)) {
              release_delete_success <- release_delete_success + 1
              cat("  \U2713 Deleted release: ", rel$tag_name, "\n", sep = "")
              
              # Also try to delete the associated git tag
              tag_result <- delete_git_tag(rel$tag_name)
              if (isTRUE(tag_result)) {
                cat("  \U2713 Deleted tag: ", rel$tag_name, "\n", sep = "")
              }
            } else {
              release_delete_failed <- release_delete_failed + 1
              cat("  \U2717 Failed release: ", rel$tag_name, "\n", sep = "")
            }
            
            Sys.sleep(0.5)
          }
          
          cat("\n  Release deletion summary:\n")
          cat(
            "    Releases: ", release_delete_success, " deleted, ",
            release_delete_failed, " failed\n",
            sep = ""
          )
          cat(
            "    Assets:   ", asset_delete_success, " deleted, ",
            asset_delete_failed, " failed\n",
            sep = ""
          )
        }
        
        # =====================================================================
        # PHASE 5: Final summary
        # =====================================================================
        cat("\n")
        cat("================================================================================\n")
        cat("CLEANUP COMPLETE\n")
        cat("================================================================================\n")
        cat("\n  Remaining in repository:\n")
        cat("    Export folder: ", export_folder, "\n", sep = "")
        cat(
          "    Release:       ",
          if (length(releases_to_keep) > 0) current_release_tag else "(none)",
          "\n",
          sep = ""
        )
        cat("\n  Repository URL: https://github.com/", owner, "/", repo, "\n", sep = "")
        
      } else {
        cat("\n  Cleanup cancelled - no files were deleted.\n")
      }
    }
    
  } else {
    cat("\n  Cleanup skipped.\n")
  }
  
} else if (!exists("github_token") || github_token == "") {
  cat("  No GitHub token available - cleanup skipped.\n")
} else {
  cat("  Non-interactive mode - cleanup skipped.\n")
  cat("  Set INTERACTIVE_CLEANUP <- TRUE and run interactively to enable.\n")
}

cat("\n\U2713 CLEANUP STEP COMPLETE\n")
