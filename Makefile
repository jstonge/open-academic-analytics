# Academic Analytics Data Pipeline
# ================================

# Directory Configuration
DATA_DIR := ./data
DATA_DIR_RAW := $(DATA_DIR)/raw
DATA_DIR_PROCESSED := $(DATA_DIR)/preprocessed
DATA_DIR_CLEAN := $(DATA_DIR)/clean
DATA_DIR_TRAINING := $(DATA_DIR)/training

FRAMEWORK_DIR := ./web
DATA_DIR_OBS := $(FRAMEWORK_DIR)/data

SCRIPT_DIR := ./scripts
MODEL_DIR := $(SCRIPT_DIR)/models

# Input Files
TIMELINE_RESEARCHERS_PARQUET := $(DATA_DIR_RAW)/uvm_profs_2023.parquet

# Database and Output Files
DATABASE := $(DATA_DIR_RAW)/oa_data_raw.db
RESEARCHERS_TSV := $(DATA_DIR_RAW)/researchers.tsv
PAPER_PARQUET := $(DATA_DIR_OBS)/paper.parquet
AUTHOR_PARQUET := $(DATA_DIR_OBS)/author.parquet
COAUTHOR_PARQUET := $(DATA_DIR_OBS)/coauthor.parquet
TRAINING_DATA_PARQUET := $(DATA_DIR_OBS)/training_data.parquet

# Python Interpreter
PYTHON := python

# Script Paths
IMPORT_SCRIPTS := $(SCRIPT_DIR)/import
PREPROCESSING_SCRIPTS := $(SCRIPT_DIR)/preprocessing

#####################
#   MAIN TARGETS    #
#####################

.PHONY: all update-timeline clean help

# Default target - show help
all: help

# Main pipeline for updating timeline data
update-timeline: researchers import preprocess-paper updateDB-coauthor preprocess-coauthor
	@echo "Timeline data update complete!"

# Clean all generated files
clean:
	@echo "Cleaning generated files..."
	rm -rf $(FRAMEWORK_DIR)/.observablehq/cache
	@echo "Clean complete!"

# Show help information
help:
	@echo "Academic Analytics Data Pipeline"
	@echo "================================"
	@echo ""
	@echo "Main targets:"
	@echo "  update-timeline    - Run complete timeline data pipeline"
	@echo "  clean             - Remove all generated files"
	@echo "  help              - Show this help message"
	@echo ""
	@echo "Individual pipeline steps:"
	@echo "  researchers       - Generate researchers.tsv from source data"
	@echo "  import            - Import papers and authors from OpenAlex API"
	@echo "  preprocess-paper  - Clean and filter paper data"
	@echo "  updateDB-coauthor - Generate coauthor relationships"
	@echo "  preprocess-coauthor - Process author and coauthor data for analysis"
	@echo ""
	@echo "Data flow:"
	@echo "  $(TIMELINE_RESEARCHERS_PARQUET) → researchers → import → preprocess-paper → updateDB-coauthor → preprocess-coauthor"

#####################
#  PIPELINE STEPS   #
#####################

# Step 1: Generate researchers.tsv from source parquet
.PHONY: researchers
researchers: $(RESEARCHERS_TSV)

$(RESEARCHERS_TSV): $(TIMELINE_RESEARCHERS_PARQUET)
	@echo "Generating researcher data..."
	$(PYTHON) $(IMPORT_SCRIPTS)/researchers.py -o $(DATA_DIR_RAW)

# Step 2: Import papers and authors from OpenAlex API
.PHONY: import
import: $(DATABASE)

$(DATABASE): $(RESEARCHERS_TSV)
	@echo "Importing papers and authors from OpenAlex..."
	$(PYTHON) $(IMPORT_SCRIPTS)/timeline-paper.py \
		-i $(RESEARCHERS_TSV) \
		-o $(DATA_DIR_RAW)

# Step 3: Preprocess papers (clean, filter, deduplicate)
.PHONY: preprocess-paper
preprocess-paper: $(PAPER_PARQUET)

$(PAPER_PARQUET): $(DATABASE)
	@echo "Preprocessing paper data..."
	$(PYTHON) $(PREPROCESSING_SCRIPTS)/paper.py \
		-i $(DATA_DIR_RAW) \
		-o $(DATA_DIR_OBS)

# Step 4: Generate coauthor relationships
.PHONY: updateDB-coauthor
updateDB-coauthor: $(PAPER_PARQUET)
	@echo "Generating coauthor relationships..."
	$(PYTHON) $(IMPORT_SCRIPTS)/timeline-coauthor.py \
		-i $(DATA_DIR_OBS) \
		-o $(DATA_DIR_RAW)

# Step 5: Process author and coauthor data for analysis
.PHONY: preprocess-coauthor
preprocess-coauthor: $(AUTHOR_PARQUET) $(COAUTHOR_PARQUET)

$(AUTHOR_PARQUET): updateDB-coauthor
	@echo "Processing author data..."
	$(PYTHON) $(PREPROCESSING_SCRIPTS)/author.py \
		-i $(DATA_DIR_RAW) \
		-o $(DATA_DIR_OBS)

$(COAUTHOR_PARQUET): updateDB-coauthor
	@echo "Processing coauthor data..."
	$(PYTHON) $(PREPROCESSING_SCRIPTS)/coauthor.py \
		-i $(DATA_DIR_RAW) \
		-o $(DATA_DIR_OBS)

#####################
# OPTIONAL TARGETS  #
#####################

# Update author ages only (for corrections)
.PHONY: update-author-ages
update-author-ages: $(RESEARCHERS_TSV)
	@echo "Updating author ages..."
	$(PYTHON) $(IMPORT_SCRIPTS)/timeline-paper.py \
		-i $(RESEARCHERS_TSV) \
		-o $(DATA_DIR_RAW) \
		--update

# Generate training data (requires additional dependencies)
.PHONY: generate-training-data
generate-training-data: $(TRAINING_DATA_PARQUET)

$(TRAINING_DATA_PARQUET): $(COAUTHOR_PARQUET) $(PAPER_PARQUET)
	@echo "Generating training data..."
	$(PYTHON) $(SCRIPT_DIR)/split_training.py \
		-i $(DATA_DIR_OBS) \
		-a $(DATA_DIR_RAW) \
		-o $(DATA_DIR_OBS)

# Run Bayesian change point analysis (requires Stan)
.PHONY: switchpoint-analysis
switchpoint-analysis: $(TRAINING_DATA_PARQUET)
	@echo "Running switchpoint analysis..."
	$(PYTHON) $(MODEL_DIR)/change_point_bayesian.py \
		-i $(DATA_DIR_OBS) \
		-m $(MODEL_DIR)/stan \
		-o $(DATA_DIR_OBS)

#####################
#  UTILITY TARGETS  #
#####################

# Create necessary directories
.PHONY: create-dirs
create-dirs:
	@echo "Creating directories..."
	mkdir -p $(DATA_DIR_RAW) $(DATA_DIR_PROCESSED) $(DATA_DIR_CLEAN) $(DATA_DIR_TRAINING) $(DATA_DIR_OBS)

# Validate file existence
.PHONY: check-files
check-files:
	@echo "Checking required files..."
	@test -f $(TIMELINE_RESEARCHERS_PARQUET) || (echo "Error: $(TIMELINE_RESEARCHERS_PARQUET) not found" && exit 1)
	@echo "Required files present"

# Show pipeline status
.PHONY: status
status:
	@echo "Pipeline Status:"
	@echo "==============="
	@echo "Input file:        $(if $(wildcard $(TIMELINE_RESEARCHERS_PARQUET)),✓ Present,✗ Missing)"
	@echo "Researchers TSV:   $(if $(wildcard $(RESEARCHERS_TSV)),✓ Present,✗ Missing)"
	@echo "Database:          $(if $(wildcard $(DATABASE)),✓ Present,✗ Missing)"
	@echo "Paper data:        $(if $(wildcard $(PAPER_PARQUET)),✓ Present,✗ Missing)"
	@echo "Author data:       $(if $(wildcard $(AUTHOR_PARQUET)),✓ Present,✗ Missing)"
	@echo "Coauthor data:     $(if $(wildcard $(COAUTHOR_PARQUET)),✓ Present,✗ Missing)"

#####################
#    DEBUGGING      #
#####################

# Debug: Print all variables
.PHONY: debug-vars
debug-vars:
	@echo "Makefile Variables:"
	@echo "==================="
	@echo "DATA_DIR: $(DATA_DIR)"
	@echo "DATA_DIR_RAW: $(DATA_DIR_RAW)"
	@echo "DATA_DIR_OBS: $(DATA_DIR_OBS)"
	@echo "SCRIPT_DIR: $(SCRIPT_DIR)"
	@echo "DATABASE: $(DATABASE)"
	@echo "RESEARCHERS_TSV: $(RESEARCHERS_TSV)"

# Debug: Test individual script
.PHONY: test-script
test-script:
	@echo "Testing script execution..."
	$(PYTHON) --version
	@echo "Python path ready"