# Load required libraries
library(data.table)
library(haven)
library(dplyr)
library(lubridate)

# Define paths
in_med_path <- "R:/IQVIA PharMetrics Plus (2024)/Claims - Medical - Inpatient"
out_med_path <- "R:/IQVIA PharMetrics Plus (2024)/Claims - Medical - Outpatient"
mylib_path <- "R:/IQVIA PharMetrics Plus (2024) Members/WatsonWilliam/Pain_Project"

# Define pain diagnosis codes for upper bound
pain_diagnoses <- c("R52.0", "R52.1", "R52.2", "R52.9", "M54.2", "M54.5", 
                    "M25.50", "M25.51", "M25.52", "M25.53", "M25.54", "M25.55", 
                    "M25.56", "M25.57", "R07.9", "R10.9", "M79.2", "M79.6", 
                    "G89.0", "G89.2", "G89.3", "G89.18", "G89.28")

# Function to extract claims and track eligibility
extract_primary_pain_claims <- function(file_path, file_name, pain_dx_codes) {
  cat("Extracting primary pain diagnosis claims from", file_name, "...\n")
  
  eligible_claims <- data.table()
  eligibility_stats <- data.table()
  
  # Extract year and claim type from filename
  year <- ifelse(grepl("_21", file_name), "2021", "2023")
  claim_type <- ifelse(grepl("inpat", file_name), "inpatient", "outpatient")
  
  file_size_gb <- file.info(file_path)$size / (1024^3)
  cat("File size:", round(file_size_gb, 2), "GB\n")
  
  # Fields to extract
  fields_to_keep <- c("pat_id", "claimno", "linenum", "pos", "proc_cde", "cpt_mod", 
                      "from_dt", "to_dt", "diag_admit", paste0("diag", 1:12), 
                      paste0("icdprc", 1:12), "allowed", "paid", "rend_id", "rend_spec")
  
  # Initialize tracking variables
  total_claims <- 0
  total_claim_lines <- 0
  total_patients <- 0
  eligible_claim_count <- 0
  eligible_claim_lines <- 0
  eligible_patient_count <- 0
  
  if(file_size_gb > 5) {
    cat("Large file - processing in chunks\n")
    
    offset <- 0
    chunk_num <- 1
    chunk_size <- 1000000
    
    repeat {
      cat("Processing chunk", chunk_num, "...\n")
      
      chunk <- tryCatch({
        haven::read_sas(file_path, skip = offset, n_max = chunk_size)
      }, error = function(e) {
        cat("Error reading chunk:", e$message, "\n")
        NULL
      })
      
      if(is.null(chunk) || nrow(chunk) == 0) {
        cat("No more data to read\n")
        break
      }
      
      setDT(chunk)
      
      # Track total metrics for this chunk
      chunk_total_claims <- uniqueN(chunk$claimno)
      chunk_total_lines <- nrow(chunk)
      chunk_total_patients <- uniqueN(chunk$pat_id)
      
      # Filter for PRIMARY pain diagnoses only (diag_admit or diag1)
      pain_chunk <- chunk[
        diag_admit %in% pain_dx_codes | diag1 %in% pain_dx_codes
      ]
      
      if(nrow(pain_chunk) > 0) {
        # Track eligible metrics for this chunk
        chunk_eligible_claims <- uniqueN(pain_chunk$claimno)
        chunk_eligible_lines <- nrow(pain_chunk)
        chunk_eligible_patients <- uniqueN(pain_chunk$pat_id)
        
        # Select fields and add metadata
        chunk_selected <- pain_chunk[, ..fields_to_keep]
        chunk_selected[, `:=`(
          data_year = year,
          service_type = claim_type,
          primary_pain_dx = ifelse(diag_admit %in% pain_dx_codes, diag_admit,
                                   ifelse(diag1 %in% pain_dx_codes, diag1, NA_character_))
        )]
        
        eligible_claims <- rbindlist(list(eligible_claims, chunk_selected), use.names = TRUE)
        
        # Update running totals
        eligible_claim_count <- eligible_claim_count + chunk_eligible_claims
        eligible_claim_lines <- eligible_claim_lines + chunk_eligible_lines
        eligible_patient_count <- eligible_patient_count + chunk_eligible_patients
      }
      
      # Update total running counts
      total_claims <- total_claims + chunk_total_claims
      total_claim_lines <- total_claim_lines + chunk_total_lines  
      total_patients <- total_patients + chunk_total_patients
      
      cat("Chunk", chunk_num, "- Eligible:", nrow(pain_chunk), "lines out of", nrow(chunk), "total\n")
      
      # Clean up
      rm(chunk)
      if(exists("pain_chunk")) rm(pain_chunk)
      if(exists("chunk_selected")) rm(chunk_selected)
      gc()
      
      offset <- offset + chunk_size
      chunk_num <- chunk_num + 1
    }
    
  } else {
    # Process smaller files normally
    cat("Loading entire file\n")
    claims <- haven::read_sas(file_path)
    setDT(claims)
    
    if(!is.null(claims) && nrow(claims) > 0) {
      # Track totals
      total_claims <- uniqueN(claims$claimno)
      total_claim_lines <- nrow(claims)
      total_patients <- uniqueN(claims$pat_id)
      
      # Filter for primary pain diagnoses
      pain_chunk <- claims[
        diag_admit %in% pain_dx_codes | diag1 %in% pain_dx_codes
      ]
      
      if(nrow(pain_chunk) > 0) {
        eligible_claim_count <- uniqueN(pain_chunk$claimno)
        eligible_claim_lines <- nrow(pain_chunk)
        eligible_patient_count <- uniqueN(pain_chunk$pat_id)
        
        eligible_claims <- pain_chunk[, ..fields_to_keep]
        eligible_claims[, `:=`(
          data_year = year,
          service_type = claim_type,
          primary_pain_dx = ifelse(diag_admit %in% pain_dx_codes, diag_admit,
                                   ifelse(diag1 %in% pain_dx_codes, diag1, NA_character_))
        )]
      }
    }
    
    rm(claims)
    if(exists("pain_chunk")) rm(pain_chunk)
    gc()
  }
  
  # Create eligibility summary
  eligibility_stats <- data.table(
    file_name = file_name,
    data_year = year,
    service_type = claim_type,
    total_claims = total_claims,
    total_claim_lines = total_claim_lines,
    total_patients = total_patients,
    eligible_claims = eligible_claim_count,
    eligible_claim_lines = eligible_claim_lines,
    eligible_patients = eligible_patient_count,
    pct_claims_eligible = round((eligible_claim_count / total_claims) * 100, 2),
    pct_lines_eligible = round((eligible_claim_lines / total_claim_lines) * 100, 2),
    pct_patients_eligible = round((eligible_patient_count / total_patients) * 100, 2)
  )
  
  cat("Eligibility Summary for", file_name, ":\n")
  cat("  Total claims:", total_claims, "| Eligible:", eligible_claim_count, 
      "(", round((eligible_claim_count / total_claims) * 100, 2), "%)\n")
  cat("  Total lines:", total_claim_lines, "| Eligible:", eligible_claim_lines,
      "(", round((eligible_claim_lines / total_claim_lines) * 100, 2), "%)\n")
  cat("  Total patients:", total_patients, "| Eligible:", eligible_patient_count,
      "(", round((eligible_patient_count / total_patients) * 100, 2), "%)\n")
  
  return(list(claims = eligible_claims, stats = eligibility_stats))
}

# Extract all primary pain diagnosis claims (upper bound)
cat("\n=== Extracting primary pain diagnosis claims (UPPER BOUND) for 2021 and 2023 ===\n")
all_pain_claims_upper <- data.table()
all_eligibility_stats <- data.table()

files_to_check <- list(
  list(path = file.path(in_med_path, "clm_inpat_21.sas7bdat"), name = "inpat_21"),
  list(path = file.path(in_med_path, "clm_inpat_23.sas7bdat"), name = "inpat_23"),
  list(path = file.path(out_med_path, "clm_outpat_21.sas7bdat"), name = "outpat_21"),
  list(path = file.path(out_med_path, "clm_outpat_23.sas7bdat"), name = "outpat_23")
)

for(file_info in files_to_check) {
  if(file.exists(file_info$path)) {
    results <- extract_primary_pain_claims(file_info$path, file_info$name, pain_diagnoses)
    
    if(!is.null(results$claims) && nrow(results$claims) > 0) {
      all_pain_claims_upper <- rbindlist(list(all_pain_claims_upper, results$claims), use.names = TRUE)
    }
    
    all_eligibility_stats <- rbindlist(list(all_eligibility_stats, results$stats), use.names = TRUE)
    cat("Total eligible claim lines so far:", nrow(all_pain_claims_upper), "\n\n")
  } else {
    cat("WARNING: File not found:", file_info$path, "\n")
  }
}

# Save results
if(nrow(all_pain_claims_upper) > 0) {
  fwrite(all_pain_claims_upper, file.path(mylib_path, "primary_pain_claims_upper_bound_2021_2023.csv"))
  fwrite(all_eligibility_stats, file.path(mylib_path, "pain_claims_eligibility_summary.csv"))
  
  cat("SUCCESS: Saved", nrow(all_pain_claims_upper), "primary pain diagnosis claim lines (UPPER BOUND)\n")
  
  # Overall summary
  cat("\nOverall Upper Bound Summary:\n")
  cat("Unique patients with primary pain diagnosis:", uniqueN(all_pain_claims_upper$pat_id), "\n")
  cat("Unique claims with primary pain diagnosis:", uniqueN(all_pain_claims_upper$claimno), "\n")
  cat("Total claim lines with primary pain diagnosis:", nrow(all_pain_claims_upper), "\n")
  
  cat("\nEligibility Summary Across All Files:\n")
  print(all_eligibility_stats)
  
} else {
  cat("WARNING: No primary pain diagnosis claims found\n")
}