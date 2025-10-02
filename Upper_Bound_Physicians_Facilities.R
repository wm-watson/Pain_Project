library(data.table)
library(haven)
library(dplyr)

# Define paths
in_med_path <- "R:/IQVIA PharMetrics Plus (2024)/Claims - Medical - Inpatient"
out_med_path <- "R:/IQVIA PharMetrics Plus (2024)/Claims - Medical - Outpatient"
enroll_path <- "R:/IQVIA PharMetrics Plus (2024)/Enrollment"
mylib_path <- "R:/IQVIA PharMetrics Plus (2024) Members/WatsonWilliam/Pain_Project"

cat("=== ULTRA-FAST UPPER BOUND ANALYSIS ===\n")
start_time <- Sys.time()

# =============================================================================
# BUILD OPTIMIZED LOOKUP
# =============================================================================

icd10_codes <- fread(file.path(mylib_path, "Aim2_ICD10CodeFamily.csv"))
cat("Loaded", nrow(icd10_codes), "code families\n")

# Create lookup based on code length for ultra-fast substring matching
code_lengths <- unique(nchar(icd10_codes$CodeFamily))
cat("Code lengths:", code_lengths, "\n")

# Create separate lookup lists by length for faster matching
lookup_by_length <- list()
for(len in code_lengths) {
  lookup_by_length[[as.character(len)]] <- icd10_codes$CodeFamily[nchar(icd10_codes$CodeFamily) == len]
}

# =============================================================================
# LOAD REFERENCE DATA ONCE
# =============================================================================

cat("Loading reference data...\n")

demographics_sample <- fread(file.path(mylib_path, "aim1_demographics_sex_state_final.csv"))

enroll_synth <- haven::read_sas(file.path(enroll_path, "enroll_synth.sas7bdat"))
setDT(enroll_synth)
demographics <- enroll_synth[, .(pat_id, der_sex, pat_state)] %>% distinct()
rm(enroll_synth); gc()

enroll2 <- haven::read_sas(file.path(enroll_path, "enroll2.sas7bdat"))
setDT(enroll2)

get_primary_paytype <- function(pay_string) {
  if(is.na(pay_string)) return(NA_character_)
  year_2023 <- substr(pay_string, 265, 276)  # Correct 2023 positions
  chars <- strsplit(year_2023, "")[[1]]
  chars <- chars[chars != "" & !is.na(chars) & chars != " "]
  if(length(chars) == 0) return(NA_character_)
  char_counts <- table(chars)
  names(char_counts)[which.max(char_counts)]
}

payer_info <- enroll2[string_type == "pay_type", .(
  pat_id, 
  primary_pay_type_2023 = sapply(string_value, get_primary_paytype)
)]
rm(enroll2); gc()

# =============================================================================
# ULTRA-FAST MATCHING FUNCTION (FIXED)
# =============================================================================

match_codes_ultrafast <- function(diag_codes) {
  result <- data.table(
    diagnosis_code = diag_codes,
    code_family = NA_character_
  )
  
  for(len in sort(code_lengths, decreasing = TRUE)) {
    codes_at_length <- lookup_by_length[[as.character(len)]]
    substr_codes <- substr(result$diagnosis_code, 1, len)
    matches <- substr_codes %in% codes_at_length
    needs_assignment <- matches & is.na(result$code_family)
    if(sum(needs_assignment) > 0) {
      result[needs_assignment, code_family := substr_codes[needs_assignment]]
    }
  }
  return(result)
}

# =============================================================================
# STREAMLINED PROCESSING FUNCTION (FIXED)
# =============================================================================

process_file_ultrafast <- function(file_path, provider_type, file_name) {
  cat("\n=== Processing", file_name, "for provider type", provider_type, "===\n")
  
  claims <- haven::read_sas(file_path)
  setDT(claims)
  cat("Loaded", nrow(claims), "rows\n")
  
  claims <- claims[ptypeflg == provider_type]
  cat("Provider filter:", nrow(claims), "rows\n")
  
  if(nrow(claims) == 0) return(data.table())
  
  claims <- merge(claims, payer_info, by = "pat_id", all.x = TRUE)
  claims <- merge(claims, demographics, by = "pat_id", all.x = TRUE)
  claims <- claims[!is.na(primary_pay_type_2023) & !is.na(der_sex) & !is.na(pat_state)]
  
  cat("After merging:", nrow(claims), "rows\n")
  
  financial_cols <- c("allowed", "paid", "deductible", "copay", "coinsamt", "cobamt")
  for(col in financial_cols) {
    if(col %in% names(claims)) claims[, (col) := as.numeric(get(col))]
  }
  
  diag_cols <- c("diag_admit", paste0("diag", 1:12))
  diag_cols <- diag_cols[diag_cols %in% names(claims)]
  
  cat("Processing", length(diag_cols), "diagnosis columns\n")
  
  all_results <- data.table()
  
  for(dc in diag_cols) {
    cat("  ", dc, "...", sep = "")
    
    temp <- claims[!is.na(get(dc)) & get(dc) != "", 
                   c("pat_id", "claimno", "primary_pay_type_2023", "der_sex", "pat_state",
                     financial_cols, dc), with = FALSE]
    
    if(nrow(temp) > 0) {
      diag_vector <- temp[[dc]]
      matched <- match_codes_ultrafast(diag_vector)
      
      # Keep only rows with valid code_family matches
      matched <- matched[!is.na(code_family)]
      
      if(nrow(matched) > 0) {
        # FIXED: Properly merge matched codes back to temp data using row indices
        temp[, temp_row_id := .I]
        matched[, temp_row_id := 1:.N]
        
        # Only keep rows that matched
        temp_matched <- temp[temp_row_id %in% matched$temp_row_id]
        temp_matched[, diagnosis_code := matched$diagnosis_code]
        temp_matched[, code_family := matched$code_family]
        temp_matched[, temp_row_id := NULL]
        temp_matched[, (dc) := NULL]
        
        all_results <- rbindlist(list(all_results, temp_matched), fill = TRUE)
        cat(nrow(temp_matched), "matches\n")
      } else {
        cat("0 matches\n")
      }
    } else {
      cat("empty\n")
    }
  }
  
  cat("Total:", nrow(all_results), "rows\n")
  return(all_results)
}

# =============================================================================
# PROCESS ALL FILES
# =============================================================================

cat("\n=== PHYSICIANS ===\n")

inpat_p <- process_file_ultrafast(file.path(in_med_path, "clm_inpat_23.sas7bdat"), "0", "Inpatient")
fwrite(inpat_p, file.path(mylib_path, "temp_inp_p.csv"))
rm(inpat_p); gc()

outpat_p <- process_file_ultrafast(file.path(out_med_path, "clm_outpat_23.sas7bdat"), "0", "Outpatient")
fwrite(outpat_p, file.path(mylib_path, "temp_outp_p.csv"))
rm(outpat_p); gc()

cat("Combining physicians...\n")
all_p <- rbindlist(list(
  fread(file.path(mylib_path, "temp_inp_p.csv")),
  fread(file.path(mylib_path, "temp_outp_p.csv"))
), fill = TRUE)

cat("Creating breakdown...\n")
p_results <- all_p[, .(
  total_allowed = sum(allowed, na.rm = TRUE),
  total_paid = sum(paid, na.rm = TRUE),
  total_deductible = sum(deductible, na.rm = TRUE),
  total_copay = sum(copay, na.rm = TRUE),
  total_coinsamt = sum(coinsamt, na.rm = TRUE),
  total_cobamt = sum(cobamt, na.rm = TRUE),
  claim_lines = .N,
  distinct_patients = uniqueN(pat_id),
  distinct_claims = uniqueN(claimno)
), by = .(der_sex, pat_state, primary_pay_type_2023, code_family)]

setnames(p_results, c("der_sex", "pat_state", "primary_pay_type_2023"), 
         c("sex", "state", "payer_type"))

p_results[, allowed_divided_by_pat := total_allowed / distinct_patients]
demographics_clean <- demographics_sample[, .(state = pat_state, sex = der_sex, SampleN = N)]
p_results <- merge(p_results, demographics_clean, by = c("state", "sex"), all.x = TRUE)
p_results[, pain_patient_percent := distinct_patients / SampleN * 100]
p_results <- p_results[order(-total_allowed)]

fwrite(p_results, file.path(mylib_path, "aim2_upper_bound_physicians_comprehensive.csv"))
cat("Saved:", nrow(p_results), "rows\n")

rm(all_p, p_results); gc()

cat("\n=== FACILITIES ===\n")

inpat_f <- process_file_ultrafast(file.path(in_med_path, "clm_inpat_23.sas7bdat"), "1", "Inpatient")
fwrite(inpat_f, file.path(mylib_path, "temp_inp_f.csv"))
rm(inpat_f); gc()

outpat_f <- process_file_ultrafast(file.path(out_med_path, "clm_outpat_23.sas7bdat"), "1", "Outpatient")
fwrite(outpat_f, file.path(mylib_path, "temp_outp_f.csv"))
rm(outpat_f); gc()

cat("Combining facilities...\n")
all_f <- rbindlist(list(
  fread(file.path(mylib_path, "temp_inp_f.csv")),
  fread(file.path(mylib_path, "temp_outp_f.csv"))
), fill = TRUE)

cat("Creating breakdown...\n")
f_results <- all_f[, .(
  total_allowed = sum(allowed, na.rm = TRUE),
  total_paid = sum(paid, na.rm = TRUE),
  total_deductible = sum(deductible, na.rm = TRUE),
  total_copay = sum(copay, na.rm = TRUE),
  total_coinsamt = sum(coinsamt, na.rm = TRUE),
  total_cobamt = sum(cobamt, na.rm = TRUE),
  claim_lines = .N,
  distinct_patients = uniqueN(pat_id),
  distinct_claims = uniqueN(claimno)
), by = .(der_sex, pat_state, primary_pay_type_2023, code_family)]

setnames(f_results, c("der_sex", "pat_state", "primary_pay_type_2023"), 
         c("sex", "state", "payer_type"))

f_results[, allowed_divided_by_pat := total_allowed / distinct_patients]
f_results <- merge(f_results, demographics_clean, by = c("state", "sex"), all.x = TRUE)
f_results[, pain_patient_percent := distinct_patients / SampleN * 100]
f_results <- f_results[order(-total_allowed)]

fwrite(f_results, file.path(mylib_path, "aim2_upper_bound_facilities_comprehensive.csv"))
cat("Saved:", nrow(f_results), "rows\n")

# Cleanup temp files
file.remove(file.path(mylib_path, c("temp_inp_p.csv", "temp_outp_p.csv", "temp_inp_f.csv", "temp_outp_f.csv")))

end_time <- Sys.time()
cat("\nTOTAL TIME:", end_time - start_time, "\n")
cat("\n=== COMPLETE ===\n")