.PHONY: all

ifndef ARCH
ARCH := $(shell uname -m)
endif

all: pause.c
	gcc -Os -Wall -Werror -static -o pause-$(ARCH) pause.c
	strip pause-$(ARCH)
