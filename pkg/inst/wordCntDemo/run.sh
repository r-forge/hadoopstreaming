#! /usr/bin/env sh
#export R_LIBS="pathToLocalRLibraryIfNeede"
cat anna.txt | ./hsWordCnt.R -m | sort | ./hsWordCnt.R -r
