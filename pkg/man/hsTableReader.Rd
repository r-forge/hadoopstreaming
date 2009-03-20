\name{hsTableReader}
\alias{hsTableReader}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{Chunks input data into data frames}
\description{
This function repeatedly reads a chunk of data from an input connection,
packages the data as a data.frame, optionally ensures that all the rows
for certain keys are contained in the data.frame, and passes the data.frame to
a handler for processing.  This continues until the end of file.
}
\usage{
hsTableReader(file = "", cols = "character", chunkSize = -1, FUN = print, ignoreKey = T, singleKey = T, skip = 0, sep = "\t", keyCol = "key",PFUN=NULL)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{file}{Any file specification accepted by scan}
  \item{cols}{A list of column names, as accepted by the 'what' arg to scan}
  \item{chunkSize}{Number of lines to read at a time}
  \item{FUN}{A function accepting a dataframe with columns given by cols}
  \item{ignoreKey}{If TRUE, always passes chunkSize rows to FUN, regardless of
  whether the chunk has only some of the rows for a given key. If TRUE,
  the singleKey arg is ignored.}
  \item{singleKey}{If TRUE, then each data frame passed to FUN will
  contain all rows corresponding to a single key. If FALSE, then will
  contain several complete keys.}
  \item{skip}{Number of lines to skip at the beginning of the file.}
  \item{sep}{Any separator character accepted by scan}
  \item{keyCol}{The column name of the column with the keys.}
  \item{PFUN}{Same as FUN, except handles incomplete keys. See below.}
}
\details{
  With ignoreKey=T, hsTableReader reads from file, chunkSize lines at
  a time, packages the lines into a data.frame, and passes the
  data.frame to FUN. This mode would most commonly be used for the
  MAPPER job in Hadoop.

  Everything below pertains to ignoreKey=F.

  With ignoreKey=F, hsTableReader breaks up the data read from file
  into chunks comprising all rows of the same key.  This ASSUMES that
  all rows with the same key are stored consecutively in the input
  file.  (This is always the case if the input file is taken to be the
  STDIN pipe to a Hadoop reducer.)

  We first disucss the case of PFUN=NULL. This is the recommended
  setting when all the values for a given key can comfortably fit in
  memory.

  When singleKey=T, FUN is called with all the rows for a single key
  at a time.  When singleKey=F, FUN made be called with rows
  corresponding to multiple keys, but we guarantee that the data.frame
  contains all the rows corresponding to any key that appears in the
  data.frame.

  When PFUN != NULL, hsTableReader does not wait to collect all rows
  for a given key before passing the data to FUN.  When hsTableReader
  reads the first chunk of rows from file, it first passes all the
  rows of complete keys to FUN.  Then, it passes the rows
  corresponding to the last key, call it PREVKEY, which may be
  incomplete, to PFUN. When it reads the next chunk, it first passes
  all the rows corresponding to PREVKEY to PFUN. It continues reading
  chunks and passing them to PREVKEY until we have reached a new key.
  At this point, PFUN is called with an empty data.frame, to indicate
  that it is no done handling (key,value) pairs for PREVKEY.  Then,
  the subsequent complete keys are passed to FUN, the new incomplete
  keys to PFUN, and the process continues.
    
  By using a PFUN function, we can process more values for a given key
  than can fit into memory.  See below for examples of using PFUN.
}

\value{
  No return value.
}
\references{ ~put references to the literature/web site here ~ }
\author{David S. Rosenberg <drosen@sensenetworks.com> }
\note{ ~~further notes~~ 

 ~Make other sections like Warning with \section{Warning }{....} ~
}
\seealso{ ~~objects to See Also as \code{\link{help}}, ~~~ }
\examples{
  ## This function is useful as a reader for Hadoop reduce scripts
  str <- "key1\t3.9\nkey1\t8.9\nkey1\t1.2\nkey1\t3.9\nkey1\t8.9\nkey1\t1.2\nkey2\t9.9\nkey2\t10.1\nkey3\t1.0\nkey3\t3.4\nkey4\t9.4\n"
  cat(str)
  cols = list(key='',val=0)
  con <- textConnection(str, open = "r")
  hsTableReader(con,cols,chunkSize=6,FUN=print,ignoreKey=T)
  close(con)
  con <- textConnection(str, open = "r")
  hsTableReader(con,cols,chunkSize=6,FUN=print,ignoreKey=F,singleKey=T)
  close(con)
  con <- textConnection(str, open = "r")
  hsTableReader(con,cols,chunkSize=6,FUN=print,ignoreKey=F,singleKey=F)
  close(con)

  ## The next two examples compute the mean, by key, in 2 different ways
  reducerKeyAtOnce <-function(d) {
    key = d[1,'key']
    ave = mean(d[,'val'])
    cat(key,ave,'\n',sep='\t')
  }
  con <- textConnection(str, open = "r")
  hsTableReader(con,cols,chunkSize=6,FUN=reducerKeyAtOnce,ignoreKey=F,singleKey=T)
  close(con)

  
  reducerManyCompleteKeys <-function(d) {
    a=aggregate(d$val,by=list(d$key),FUN=mean)
    write.table(a,quote=FALSE,sep='\t',row.names=FALSE,col.names=FALSE)
  }

  con <- textConnection(str, open = "r")
  hsTableReader(con,cols,chunkSize=6,FUN=reducerManyCompleteKeys,ignoreKey=F,singleKey=F)
  close(con)

### ADVANCED: When we have more values for each key  than can fit in memory
  ## Test example to see how the input is broken up
  reducerFullKeys <- function(d) {
    print("Processing complete keys.")
    print(d)
  }
  reducerPartialKey <- function(d) {
    if (nrow(d)==0) {
      print("Done with partial key")
    } else {
      print("Processing partial key...")
      print(d)
    }
  }
  con <- textConnection(str, open = "r")
  hsTableReader(con,cols,chunkSize=5,FUN=reducerFullKeys,ignoreKey=F,singleKey=F,PFUN=reducerPartialKey)
  close(con)

  ## Repeats the mean example, with partial key processing
  partialSum = 0
  partialCnt = 0
  partialKey = NA
  reducerPartialKey <- function(d) {
    if (nrow(d)==0) {
      ## empty data.frame indicates that we have seen all rows for the previous key
      ave = partialSum / partialCnt
      cat(partialKey,ave,'\n',sep='\t')
      partialSum <<- 0
      partialCnt <<- 0
      partialKey <<- NA
    } else {
      if (is.na(partialKey)) partialKey <<- d[1,'key']
      partialSum <<- partialSum + sum(d[,'val'])
      partialCnt <<- partialCnt + nrow(d)
    }
  }
  con <- textConnection(str, open = "r")
  hsTableReader(con,cols,chunkSize=6,FUN=reducerKeyAtOnce,ignoreKey=F,singleKey=T,PFUN=reducerPartialKey)
  close(con)
  con <- textConnection(str, open = "r")
  hsTableReader(con,cols,chunkSize=6,FUN=reducerManyCompleteKeys,ignoreKey=F,singleKey=F,PFUN=reducerPartialKey)
  close(con)
}