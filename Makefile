DATA_DIR=./data
DATA_DIR_RAW=$(DATA_DIR)/raw
DATA_DIR_PROCESSED=$(DATA_DIR)/preprocessed
DATA_DIR_CLEAN=$(DATA_DIR)/clean
DATA_DIR_TRAINING=$(DATA_DIR)/training


FRAMEWORK_DIR=./web
DATA_DIR_OBS=$(FRAMEWORK_DIR)/data

SCRIPT_DIR=./scripts

MODEL_DIR=$(SCRIPT_DIR)/models

TIMELINE_RESEARCHERS_PARQUET=$(DATA_DIR_RAW)/uvm_profs_2023.parquet

#####################
#                   #
#       GLOBAL      #
#                   #
#####################

clean:
	rm -rf web/.observablehq/cache
	rm web/data/author.parquet web/data/coauthor.parquet web/data/paper.parquet web/data/training_data.parquet web/data/oa_data_raw.db
	rm data/raw/oa_data_raw.db data/raw/researchers.tsv

##############################
#                            #
#        TIMELINE DATA       #
#                            #
##############################

.PHONY: update-timeline

# We don't import papers in this command
update-timeline: import preprocess-paper updateDB-coauthor preprocess-coauthor 

# data-dirs:
# 	mkdir -p $(DATA_DIR_CLEAN)

researchers: #uvm_profs_2023.parquet -> researchers.tsv
	python $(SCRIPT_DIR)/import/researchers.py -o data/raw

import:
	python $(SCRIPT_DIR)/import/timeline-paper.py \
		-i $(DATA_DIR_RAW)/researchers.tsv \
		-o $(DATA_DIR_RAW)


# updateDB-paper: #researchers.tsv -> UPDATE oa_data_raw.db (paper/author table)
# 	python $(SCRIPT_DIR)/import/timeline-paper.py \
# 		-i $(DATA_DIR_RAW)/researchers.tsv \
# 		-U \
# 		-o $(DATA_DIR_RAW) 

# We get rid of a bunch of papers in this step, based on work type, duplicates, etc.
# This is why preprocessing papers come before coauthorship, as we need to have the papers
# to find the coauthors.
preprocess-paper: # oa_data_raw.db/paper -> paper.parquet
	python $(SCRIPT_DIR)/preprocessing/paper.py -i $(DATA_DIR_RAW) -o $(DATA_DIR_OBS)

updateDB-coauthor: # oa_data_raw.db+paper.parquet -> oa_data_raw (coauthor2 table)
	python $(SCRIPT_DIR)/import/timeline-coauthor.py -i $(DATA_DIR_OBS) -o $(DATA_DIR_RAW)

preprocess-coauthor: # oa_data_raw.db -> author.parquet x coauthor.parquet
	python $(SCRIPT_DIR)/preprocessing/author.py -i $(DATA_DIR_RAW) -o $(DATA_DIR_OBS)
	python $(SCRIPT_DIR)/preprocessing/coauthor.py -i $(DATA_DIR_RAW) -o $(DATA_DIR_OBS)

# ksplit: # coauthor.parquet x paper.parquet x researchers.parquet x dept2fos.json ->  training_data.parquet
# 	python $(SCRIPT_DIR)/split_training.py -i $(DATA_DIR_OBS) -a $(DATA_DIR_RAW) -o $(DATA_DIR_OBS)

# switchpoint: # training_data.parquet x change_point02.stan -> UPDATE training_data.parquet
# 	python $(SCRIPT_DIR)/models/change_point_bayesian.py -i $(DATA_DIR_OBS) -m $(MODEL_DIR)/stan  -o $(DATA_DIR_OBS)

