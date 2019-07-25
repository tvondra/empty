# empty/Makefile
#

MODULE_big = empty

OBJS = empty.o

EXTENSION = empty
DATA = empty--1.0.sql

REGRESS = empty

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

ifndef MAJORVERSION
    MAJORVERSION := $(basename $(VERSION))
endif
