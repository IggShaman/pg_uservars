MODULES = pg_uservars
DATA_built = pg_uservars.sql
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
