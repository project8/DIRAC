#!/usr/bin/env python

__RCSID__   = "$Id$"

from DIRAC           import exit as DIRACExit
from DIRAC.Core.Base import Script 

Script.setUsageMessage("""
Get user defined metadata for the given file specified by its Logical File Name or for a list of files
contained in the specifed file

Usage:
   %s [-e] <lfn | fileContainingLfns> [Catalog]

   -e   Expand metadata of nested files
""" % Script.scriptName)

Script.parseCommandLine()

from DIRAC.Resources.Catalog.FileCatalog import FileCatalog

import os
args = Script.getPositionalArgs()

expandFlag = False

if len(args) < 1:
  Script.showHelp()
  DIRACExit( -1 )
elif args[0] == '-e':
  expandFlag = True
  inputFileName = args[1]
  catalogs = []
  if len(args) == 3:
    catalogs = [args[2]]
else:
  inputFileName = args[0]
  catalogs = []
  if len(args) == 2:
    catalogs = [args[1]]
  
if os.path.exists(inputFileName):
  inputFile = open(inputFileName, 'r')
  string = inputFile.read()
  lfns = string.splitlines()
  inputFile.close()
else:
  lfns = [inputFileName]

lfns = [lfn.rstrip( '/' ) for lfn in lfns]

fc = FileCatalog( catalogs = catalogs )

for lfn in lfn
  print lfn + ':'
  # Decide if lfn points to a file or a directory
  result = fc.isFile(lfn)
  if not result['OK']:
    print "ERROR: Failed to contact the catalog"      
  if not result['Value']['Successful']:
    print "ERROR: Path not found"
  isDirectory = not result['Value']['Successful'][lfn]        
      
  if isDirectory:    
    result = fc.getDirectoryUserMetadata(lfn)
    if not result['OK']:
      print ("Error: %s" % result['Message']) 
      return
    if result['Value']:
      metaDict = result['MetadataOwner']
      metaTypeDict = result['MetadataType']
      for meta, value in result['Value'].items():
        setFlag = metaDict[meta] != 'OwnParameter' and metaTypeDict[meta] == "MetaSet"
        prefix = ''
        if setFlag:
          prefix = "+"
        if metaDict[meta] == 'ParentMetadata':
          prefix += "*"
          print (prefix+meta).rjust(20),':',value
        elif metaDict[meta] == 'OwnMetadata':
          prefix += "!"
          print (prefix+meta).rjust(20),':',value   
        else:
          print meta.rjust(20),':',value 
        if setFlag and expandFlag:
          result = fc.getMetadataSet(value,expandFlag)
          if not result['OK']:
            print ("Error: %s" % result['Message']) 
            return
          for m,v in result['Value'].items():
            print " "*10,m.rjust(20),':',v      
    else:
      print "No metadata defined for directory"   
  else:
    result = fc.getFileUserMetadata(lfn)      
    if not result['OK']:
      print ("Error: %s" % result['Message']) 
      return
    if result['Value']:      
      for meta,value in result['Value'].items():
        print meta.rjust(20),':', value
    else:
      print "No metadata found"