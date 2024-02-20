#define FUSE_USE_VERSION 31
#define _FILE_OFFSET_BITS 64

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fuse.h>
#include <stdio.h>
