#!/usr/bin/env Rscript

options(stringsAsFactors = FALSE, scipen = 999)

required_cols <- c("Data Type", "Country", "Year", "HTS Number", "January")
month_names <- month.name

parse_numeric <- function(x) {
  x <- trimws(as.character(x))
  x <- gsub(",", "", x, fixed = TRUE)
  x[x %in% c("", "NA", "N/A", "NULL", ".")] <- NA_character_
  suppressWarnings(as.numeric(x))
}

left_pad_zeros <- function(x, width = 10) {
  x <- as.character(x)
  n <- nchar(x)
  pad <- pmax(width - n, 0)
  prefix <- vapply(pad, function(k) {
    if (k <= 0) {
      ""
    } else {
      paste(rep("0", k), collapse = "")
    }
  }, character(1))
  paste0(prefix, x)
}

clean_hts10 <- function(x) {
  x <- trimws(as.character(x))
  x <- sub("\\.0+$", "", x)
  x <- gsub("\\s+", "", x)
  is_digits <- grepl("^[0-9]+$", x)
  needs_pad <- is_digits & nchar(x) < 10
  x[needs_pad] <- left_pad_zeros(x[needs_pad], width = 10)
  x
}

metric_from_data_type <- function(x) {
  x <- tolower(trimws(as.character(x)))
  out <- rep(NA_character_, length(x))
  out[grepl("customs", x)] <- "customs_value"
  out[grepl("first\\s*unit", x) | (grepl("quantity", x) & !grepl("customs", x))] <- "first_unit_qty"
  out
}

safe_header <- function(path) {
  tryCatch(
    names(read.csv(path, nrows = 0, check.names = FALSE)),
    error = function(e) character(0)
  )
}

has_required_columns <- function(path) {
  hdr <- safe_header(path)
  all(required_cols %in% hdr)
}

get_month_cols <- function(df_names) {
  out <- vapply(month_names, function(m) {
    idx <- which(tolower(df_names) == tolower(m))
    if (length(idx) == 0) NA_character_ else df_names[idx[1]]
  }, character(1))
  out
}

sum_or_na <- function(z) {
  if (all(is.na(z))) {
    NA_real_
  } else {
    sum(z, na.rm = TRUE)
  }
}

collapse_unique_text <- function(z) {
  z <- trimws(as.character(z))
  z <- z[!is.na(z) & nzchar(z)]
  u <- unique(z)
  if (length(u) == 0) {
    NA_character_
  } else {
    paste(u, collapse = " | ")
  }
}

read_and_reshape <- function(path) {
  df <- tryCatch(
    read.csv(path, check.names = FALSE, stringsAsFactors = FALSE),
    error = function(e) {
      message(sprintf("Skipping unreadable CSV: %s (%s)", path, e$message))
      NULL
    }
  )
  if (is.null(df)) {
    return(NULL)
  }

  if (!all(required_cols[1:4] %in% names(df))) {
    message(sprintf("Skipping CSV missing required key columns: %s", path))
    return(NULL)
  }

  month_cols <- get_month_cols(names(df))
  if (any(is.na(month_cols))) {
    message(sprintf("Skipping CSV missing one or more month columns: %s", path))
    return(NULL)
  }

  year_vec <- parse_numeric(df[["Year"]])
  keep_year <- !is.na(year_vec) & year_vec >= 1996 & year_vec <= 2005
  df <- df[keep_year, , drop = FALSE]
  year_vec <- year_vec[keep_year]

  if (nrow(df) == 0) {
    return(NULL)
  }

  stacked <- stack(df[month_cols])
  month_norm <- month_names[match(tolower(as.character(stacked$ind)), tolower(month_names))]

  n_month <- length(month_cols)
  n_rows <- nrow(df)

  long_df <- data.frame(
    Country = rep(as.character(df[["Country"]]), times = n_month),
    Year = rep(as.integer(year_vec), times = n_month),
    hts10 = rep(clean_hts10(df[["HTS Number"]]), times = n_month),
    data_type = rep(as.character(df[["Data Type"]]), times = n_month),
    quantity_description = rep(
      if ("Quantity Description" %in% names(df)) as.character(df[["Quantity Description"]]) else NA_character_,
      times = n_month
    ),
    Month = month_norm,
    value = parse_numeric(stacked$values),
    source_file = basename(path),
    stringsAsFactors = FALSE
  )

  if (nrow(long_df) != (n_rows * n_month)) {
    stop(sprintf("Unexpected reshape size for %s", path))
  }

  long_df$metric <- metric_from_data_type(long_df$data_type)
  long_df <- long_df[!is.na(long_df$metric), c("Country", "Year", "Month", "hts10", "metric", "quantity_description", "value", "source_file")]
  long_df
}

discover_files <- function(root = ".") {
  all_csv <- sort(list.files(root, pattern = "\\.csv$", recursive = TRUE, full.names = TRUE, ignore.case = TRUE))
  if (length(all_csv) == 0) {
    stop("No CSV files found.")
  }

  candidates <- all_csv[vapply(all_csv, has_required_columns, logical(1))]
  if (length(candidates) == 0) {
    stop("No CSV files found with required columns: Data Type, Country, Year, HTS Number, January.")
  }

  cat("Discovered CSVs with required columns:\n")
  for (p in candidates) {
    cat(sprintf("  - %s\n", p))
  }

  combined_pat <- "^china_imports_1996_2005\\.csv$"
  annual_pat <- "^china_imports_(199[6-9]|200[0-5])\\.csv$"

  base_names <- basename(candidates)
  combined_hits <- candidates[grepl(combined_pat, tolower(base_names))]
  annual_hits <- candidates[grepl(annual_pat, tolower(base_names))]

  if (length(combined_hits) > 0) {
    cat("\nUsing combined source file(s) matching 1996_2005 pattern:\n")
    for (p in combined_hits) {
      cat(sprintf("  - %s\n", p))
    }
    return(combined_hits)
  }

  if (length(annual_hits) >= 1) {
    if (length(annual_hits) < 10) {
      cat("\nFilename pattern is incomplete for 1996-2005; using available annual matches and filtering by Year column.\n")
    } else {
      cat("\nUsing annual source files matching expected pattern.\n")
    }
    for (p in annual_hits) {
      cat(sprintf("  - %s\n", p))
    }
    return(annual_hits)
  }

  cat("\nDetected filename mismatch with expected China_Imports_YYYY.csv pattern.\n")
  cat("Adapting automatically: using all header-matched CSV files and filtering by Year column (1996-2005).\n")
  for (p in candidates) {
    cat(sprintf("  - %s\n", p))
  }
  candidates
}

build_panel <- function(file_paths) {
  long_parts <- lapply(file_paths, read_and_reshape)
  long_parts <- long_parts[!vapply(long_parts, is.null, logical(1))]
  if (length(long_parts) == 0) {
    stop("No usable rows found after reading candidate files.")
  }

  long_all <- do.call(rbind, long_parts)
  long_all <- long_all[!is.na(long_all$Year) & !is.na(long_all$Month) & nzchar(long_all$hts10), ]
  long_all <- long_all[long_all$Year >= 1996 & long_all$Year <= 2005, ]

  if (nrow(long_all) == 0) {
    stop("No usable data rows remained after filtering to 1996-2005.")
  }

  exact_key <- c("Country", "Year", "Month", "hts10", "metric", "quantity_description", "value")
  n_exact_dupes <- sum(duplicated(long_all[exact_key]))
  if (n_exact_dupes > 0) {
    cat(sprintf("\nRemoving %s exact duplicate long rows.\n", format(n_exact_dupes, big.mark = ",")))
    long_all <- long_all[!duplicated(long_all[exact_key]), ]
  }

  metric_key <- c("Country", "Year", "Month", "hts10", "metric")
  metric_dup_rows <- sum(duplicated(long_all[metric_key]) | duplicated(long_all[metric_key], fromLast = TRUE))
  if (metric_dup_rows > 0) {
    cat(sprintf("Found %s long rows with duplicate metric keys; aggregating with sum(value).\n", format(metric_dup_rows, big.mark = ",")))
  }

  long_agg <- aggregate(
    value ~ Country + Year + Month + hts10 + metric,
    data = long_all,
    FUN = sum_or_na
  )

  first_qty_desc <- long_all[long_all$metric == "first_unit_qty", c("Country", "Year", "Month", "hts10", "quantity_description")]
  first_qty_desc <- aggregate(
    quantity_description ~ Country + Year + Month + hts10,
    data = first_qty_desc,
    FUN = collapse_unique_text
  )

  customs <- long_agg[long_agg$metric == "customs_value", c("Country", "Year", "Month", "hts10", "value")]
  first_qty <- long_agg[long_agg$metric == "first_unit_qty", c("Country", "Year", "Month", "hts10", "value")]
  names(customs)[5] <- "customs_value"
  names(first_qty)[5] <- "first_unit_qty"

  panel <- merge(customs, first_qty, by = c("Country", "Year", "Month", "hts10"), all = TRUE)
  panel <- merge(panel, first_qty_desc, by = c("Country", "Year", "Month", "hts10"), all.x = TRUE)

  panel$date <- as.Date(sprintf("%04d-%02d-01", as.integer(panel$Year), match(panel$Month, month_names)))
  panel$unit_value <- ifelse(!is.na(panel$first_unit_qty) & panel$first_unit_qty > 0, panel$customs_value / panel$first_unit_qty, NA_real_)

  panel <- panel[order(panel$Country, panel$hts10, panel$date), ]
  panel <- panel[, c("Country", "Year", "Month", "hts10", "customs_value", "first_unit_qty", "quantity_description", "date", "unit_value")]
  rownames(panel) <- NULL

  panel
}

write_outputs <- function(panel_df, output_file = "analysis/results/data/panel_hts10_monthly.csv") {
  dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)
  write.csv(panel_df, output_file, row.names = FALSE, na = "")
  cat(sprintf("\nSaved: %s\n", output_file))
}

print_diagnostics <- function(panel_df) {
  key_cols <- c("Country", "Year", "Month", "hts10")
  n_dupe_keys <- sum(duplicated(panel_df[key_cols]))

  unique_hts10 <- length(unique(panel_df$hts10[!is.na(panel_df$hts10) & nzchar(panel_df$hts10)]))
  min_date <- suppressWarnings(min(panel_df$date, na.rm = TRUE))
  max_date <- suppressWarnings(max(panel_df$date, na.rm = TRUE))

  cat("\nDiagnostics:\n")
  cat(sprintf("  Rows: %s\n", format(nrow(panel_df), big.mark = ",")))
  cat(sprintf("  Unique HTS10: %s\n", format(unique_hts10, big.mark = ",")))
  cat(sprintf("  Date range: %s to %s\n", as.character(min_date), as.character(max_date)))
  cat(sprintf("  Duplicate (Country, Year, Month, hts10) keys: %s\n", format(n_dupe_keys, big.mark = ",")))
}

main <- function() {
  file_paths <- discover_files(".")
  panel_df <- build_panel(file_paths)
  write_outputs(panel_df)
  print_diagnostics(panel_df)
}

main()
