########################################################################
# $HeadURL$
########################################################################

""" DIRAC FileCatalog plug-in class to manage dynamic datasets defined by a metadata query
"""

__RCSID__ = "$Id$"

import hashlib as md5
import os
from datetime import datetime
from collections import defaultdict

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.List import stringListToString

class DatasetManager( object ):

  _tables = dict()
  _tables["FC_MetaDatasets"] = { "Fields": {
                                             "DatasetID": "INT AUTO_INCREMENT",
                                             "DatasetName": "VARCHAR(128) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL",
                                             "MetaQuery": "VARCHAR(512)",
                                             "DirID": "INT NOT NULL DEFAULT 0",
                                             "TotalSize": "BIGINT UNSIGNED NOT NULL",
                                             "NumberOfFiles": "INT NOT NULL",
                                             "UID": "SMALLINT UNSIGNED NOT NULL",
                                             "GID": "TINYINT UNSIGNED NOT NULL",
                                             "Status": "SMALLINT UNSIGNED NOT NULL",
                                             "CreationDate": "DATETIME",
                                             "ModificationDate": "DATETIME",
                                             "DatasetHash": "CHAR(36) NOT NULL",
                                             "Mode": "SMALLINT UNSIGNED NOT NULL DEFAULT 509"
                                            },
                                  "UniqueIndexes": { "DatasetName_DirID": ["DatasetName","DirID"] },
                                  "PrimaryKey": "DatasetID"
                                }
  _tables["FC_MetaDatasetFiles"] = { "Fields": {
                                                "DatasetID": "INT NOT NULL",
                                                "FileID": "INT NOT NULL",
                                               },
                                     "UniqueIndexes": {"DatasetID_FileID":["DatasetID","FileID"]}
                                   }

  _tables["FC_MetaDatasetFields"] = { "Fields": {
	                                            "FieldID": "VARCHAR(128) NOT NULL",
	                                            "FieldType": "VARCHAR(4) NOT NULL",
                                              "Length": "INT" # only specified for varchar fields
	                                           },
	                                 "UniqueIndexes": {"":["FieldID"]}
	                               }

  def __init__( self, database = None ):
    self.db = None
    if database is not None:
      self.setDatabase( database )

  def setDatabase( self, database ):
    self.db = database

    # Have to check the existing table should be dropped when
    # existing tables will not be recreated
    result = self.db._query("SHOW TABLES")
    if not result['OK']:
      return result
    tableList = [ x[0] for x in result['Value'] ]
    tablesToCreate = {}
    for table in self._tables:
      if not table in tableList:
        tablesToCreate[table] = self._tables[table]

    result = self.db._createTables( tablesToCreate )
    if not result['OK']:
      gLogger.error( "Failed to create tables", str( self._tables.keys() ) )
    elif result['Value']:
      gLogger.info( "Tables created: %s" % ','.join( result['Value'] ) )
    return result

  def _getConnection( self, connection=False ):
    if connection:
      return connection
    res = self.db._getConnection()
    if res['OK']:
      return res['Value']
    return connection

  def addDataset( self, datasets, credDict ):
    """
    :param dict datasets: dictionary describing dataset definitions
    :param credDict:  dictionary of the caller credentials
    :return: S_OK/S_ERROR bulk return structure
    """

    result = self.db.ugManager.getUserAndGroupID( credDict )
    if not result['OK']:
      return result
    uid, gid = result['Value']

    failed = dict()
    successful = dict()

    for datasetName, metaQuery in datasets.items():
      result = self.__addDataset( datasetName, metaQuery, credDict, uid, gid )
      if result['OK']:
        successful[datasetName] = True
      else:
        failed[datasetName] = result['Message']

    return S_OK( { "Successful": successful, "Failed": failed } )

  def __addDataset( self, datasetName, metaQuery, credDict, uid, gid ):

    result = self.__getMetaQueryParameters( metaQuery, credDict )
    if not result['OK']:
      return result
    totalSize = result['Value']['TotalSize']
    datasetHash = result['Value']['DatasetHash']
    numberOfFiles = result['Value']['NumberOfFiles']

    result = self.db.fileManager._getStatusInt( 'Dynamic' )
    if not result['OK']:
      return result
    intStatus = result['Value']

    dsDir = os.path.dirname( datasetName )
    dsName = os.path.basename( datasetName )
    if not dsDir:
      return S_ERROR( 'Dataset name should be specified with full path' )
    result = self.db.dtree.existsDir( dsDir )
    if not result['OK']:
      return result
    if result['Value']['Exists']:
      dirID = result['Value']['DirID']
    else:
      result = self.db.dtree.makeDirectories( dsDir )
      if not result['OK']:
        return result
      dirID = result['Value']

    # Add the new dataset entry now
    inDict = {
               'DatasetName': dsName,
               'MetaQuery': str(metaQuery),
               'DirID': dirID,
               'TotalSize': totalSize,
               'NumberOfFiles': numberOfFiles,
               'UID': uid,
               'GID': gid,
               'CreationDate': 'UTC_TIMESTAMP()',
               'ModificationDate': 'UTC_TIMESTAMP()',
               'DatasetHash': datasetHash,
               'Status': intStatus
             }
    result = self.db.insertFields( 'FC_MetaDatasets', inDict = inDict )
    if not result['OK']:
      if "Duplicate" in result['Message']:
        return S_ERROR( 'Dataset %s already exists' % datasetName )
      else:
        return result
    datasetID = result['lastRowId']
    return S_OK( datasetID )

  def checkFieldExistance(self, fname):
    result = self.db._query( "SELECT EXISTS( SELECT 1 FROM FC_MetaDatasetFields WHERE FieldName=%s)" %fname )
    
    if not result['OK']:
      return result
    if result['Value'][0][0] == 'true':
      return S_OK(True)
    else:
      return S_OK(False)

  def renameField(self, oldname, newname):
    if not checkFieldExistance(oldname)['Value']:
      return S_ERROR( 'The field %s is not defined' %  fname)

    # Not sure if update will work here, maybe need to just do a cursor.execute()
    result = self.db._update("RENAME TABLE FC_MetaDatasetFields_%s TO FC_MetaDatasetFields_%s" % (oldname,newname))
    result = self.db._update("UPDATE FC_MetaDatasetFields SET FieldName=%s WHERE FieldName=%s" % (newname, oldname))

    FieldID = result['lastRowId']

    return S_OK( "Modified field: %d" % FieldID )

  def _addField(self, fname, ftype, newTableReq, insReq, credDict):
    if not checkFieldExistance(oldname)['Value']:
      return S_ERROR( 'The field %s is already defined' %  fname)

    # Should probably be using transations here instead of two separate SQL statements
    result = self.db._query( newTableReq )
    if not result['OK']:
      return result

    result = self.db._insert( *insReq )
    if not result['OK']:
      return result

    FieldID = result['lastRowId']

    return S_OK( "Added new metadata: %d" % FieldID )

  def addFieldInt(self, fname, credDict):
    """
    The Active column is a replacement for row deletion. In cases where information
    should not be visible to the frontend, rather than deleting a row it can be
    deactivated by setting Active=0. This is so possibly important notes or values
    deleted or modified cannot be lost.
    """
    req = ("CREATE TABLE FC_DatasetField_%s ("
              "DatasetID INTEGER NOT NULL,"
              "Value INT,"
              "Active BOOLEAN NOT NULL DEFAULT 1, "
              "DateAdded DATETIME NOT NULL"
              "INDEX (Value), "
              "INDEX (DatasetID) )" ) % (fname, length)

    insreq = ('FC_MetaDatasetFields', ['FieldName', 'FieldType'], [fname, ftype])

    return _addField(self, fname, 'int', req, insreq, credDict)

  def addFieldFloat(self, fname, credDict):
    req = ("CREATE TABLE FC_DatasetField_%s ("
              "DatasetID INTEGER NOT NULL,"
              "Value DOUBLE,"
              "Active BOOLEAN NOT NULL DEFAULT 1, "
              "DateAdded DATETIME NOT NULL"
              "INDEX (Value), "
              "INDEX (DatasetID) )" ) % (fname, length)

    insreq = ('FC_MetaDatasetFields', ['FieldName', 'FieldType'], [fname, ftype])

    return _addField(self, fname, 'float', req, credDict)

  def addFieldDate(self, fname, credDict):
    req = ("CREATE TABLE FC_DatasetField_%s ("
              "DatasetID INTEGER NOT NULL,"
              "Value DATETIME,"
              "Active BOOLEAN NOT NULL DEFAULT 1, "
              "DateAdded DATETIME NOT NULL"
              "INDEX (Value), "
              "INDEX (DatasetID) )" ) % (fname, length)

    insreq = ('FC_MetaDatasetFields', ['FieldName', 'FieldType'], [fname, ftype])

    return _addField(self, fname, 'float', req, credDict)

  def addFieldString(self, fname, unique, length, credDict):
    req = ("CREATE TABLE FC_DatasetField_%s ( "
              "DatasetID INTEGER NOT NULL, "
              "Value VARCHAR(%s), "
              "Active BOOLEAN NOT NULL DEFAULT 1, "
              "DateAdded DATETIME NOT NULL"
              "INDEX (Value), "
              "INDEX (DatasetID) )" ) % (fname, length)

    insreq = ('FC_MetaDatasetFields', ['FieldName', 'FieldType', 'Length'], [fname, ftype, length])

    return _addField(self, fname, 'string', req, credDict)

  def addFieldText(self, fname, credDict):
    # Can efficently store more characters with text than string, but cant index
    req = ("CREATE TABLE FC_DatasetField_%s ("
              "DatasetID INTEGER NOT NULL, "
              "Value TEXT, "
              "Active BOOLEAN NOT NULL DEFAULT 1, "
              "DateAdded DATETIME NOT NULL"
              "INDEX (DatasetID) )") % fname

    insreq = ('FC_MetaDatasetFields', ['FieldName', 'FieldType'], [fname, ftype])

    return _addField(self, fname, 'text', req, credDict)

  def modifyFields( self, datasetName, addDict, removeDict, credDict):
    """
    datasetName -- String containing the dataset's name
    addDict -- Dictionary of field names and values to be added to the dataset eg
    			{fieldName:[value1, value2, value3]}
    removeDict -- Dictionary of field names and values to be removed from the dataset
    """
    fields = tuple(set(removeDict.keys() + addDict.keys()))

    req = 'SELECT FieldName FROM FC_MetaDatasetFields WHERE FieldName IN %s' % str(fields)
    result = self.db._query( req )
    if not result['OK']:
      return result

    missingFields = set(fieldDict.keys())-set([x[0] for x in result['Value']])

    if missingFields:
      return S_ERROR( 'Field names %s unrecognised' % str(tuple(missingFields)) )

    # TODO: Check if the type of each format is correct

    def generateStatements(template, dic):
      # insert data from dictionaries into templates making the SQL statements
      statements = []

      for fname, fvals in dic.items():
        for fval in fname:
          if isinstance(fval, str):
            fval = "('%s')" % str(fval).replace("'", r"\'") # escape single quotes and put inside (' ')
            statements.append( template % (fname, fval ))
          elif isinstance(fval, datetime):
            fval = "('%s')" % fval.strftime('%Y-%m-%d %H:%M:%S') # put into SQL datetime format
            statements.append( template % (fname, fval )
          else:
            statements.append( template % (fname, str(fval)) ))

      return statements

    updateTemplate = 'UPDATE FC_MetaDatasetFields_%s SET Active=0 WHERE DatasetID=' + datasetName + ' AND Value=%s;'
    insertTemplate = 'INSERT INTO FC_MetaDatasetFields_%s (DateAdded, DatasetID, Value) VALUES (NOW(), ' + datasetName + ',  %s);'

    updateStatements = generateStatements(updateTemplate, removeDict)
    insertStatements = generateStatements(insertTemplate, removeDict)

    # Add and remove data from dataset in a single transation
    req = "BEGIN;\n"+"\n".join(updateStatements+insertStatements)+"\nCOMMIT;"
    result = self.db._update(req)
    if not result['OK']:
      return result

    return S_OK()

  def getFieldNames(self, credDict):

  	req = "Select FieldID from FC_MetaDatasetFields"
  	result = self.db._query( req )
    
    if not result['OK']:
      return result
    if result['Value'][0]:
      return S_OK(True)

  def getFieldNamesTypes(self, credDict):

  	req = "Select FieldID, FieldType from FC_MetaDatasetFields"
  	result = self.db._query( req )
    
    if not result['OK']:
      return result

    fieldIDValueDict = {}
    for fid, fval in result['Value']:
      fieldIDValueDict[fid] = fval
    
    return S_OK(fieldIDValueDict)

  def getAllFieldValues(self, datasetName, includeDeactivated=False, credDict):
    """
    datasetName -- nome of dataset to get fields for
    includeDeactivated -- Includes field values which have been deactivated
    returns a dictionary:
    {field1Name: [(field1Value1, field1DateModified1), (field1Value2, field1DateModified2)],
      field2Name: [(field2Value1, field2DateModified1)],
      etc}
    """
    req = "SELECT FieldID, FieldType FROM FC_MetaDatasetFields"
    result1 = self.db._query( req )
    if not result1['OK']:
      return result1

    fieldDict = defaultdict(list)

    for fname, fval in result1['Value']:
      if includeDeactivated:
        req = "SELECT Value FROM FC_MetaDatasetField_%s WHERE DatasetID=%s" % (fname, datasetName)
      else:
        req = "SELECT Value FROM FC_MetaDatasetField_%s WHERE DatasetID=%s and Active=1" % (fname, datasetName)
      result2 = self.db._query( req )
      if not result2['OK']:
        return result2

      for val, date in result2['Value']:
        # Convert to correct type
        if fval == 'int':
          val = int(val)
        if fval == 'float':
          val = float(val)
        if val == 'date':
          val = datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
        # otherwise val is a string type

        fieldDict[fname].append((val,date))

    return S_OK(fieldDict)

  def _getDatasetDirectories( self, datasets ):
    dirDict = {}
    for path in datasets:
      dsDir = os.path.dirname( path )
      dsFile = os.path.basename( path )
      dirDict.setdefault( dsDir, [] )
      dirDict[dsDir].append( dsFile )
    return dirDict

  def _findDatasets( self, datasets, connection=False ):

    connection = self._getConnection( connection )
    fullNames = [ name for name in datasets if name.startswith('/') ]
    shortNames = [ name for name in datasets if not name.startswith('/') ]

    resultDict = { "Successful": {}, "Failed": {} }
    if fullNames:
      result = self.__findFullPathDatasets( fullNames, connection )
      if not result['OK']:
        return result
      resultDict = result['Value']

    if shortNames:
      result = self.__findNoPathDatasets( shortNames, connection )
      if not result['OK']:
        return result
      resultDict['Successful'].update( result['Value']['Successful'] )
      resultDict['Failed'].update( result['Value']['Failed'] )

    return S_OK( resultDict )

  def __findFullPathDatasets( self, datasets, connection ):

    dirDict = self._getDatasetDirectories( datasets )
    failed = {}
    successful = {}
    result = self.db.dtree.findDirs( dirDict.keys() )
    if not result['OK']:
      return result
    directoryIDs = result['Value']

    directoryPaths = {}
    for dirPath in dirDict:
      if not dirPath in directoryIDs:
        for dsName in dirDict[dirPath]:
          dname = '%s/%s' % (dirPath,dsName)
          dname = dname.replace('//','/')
          failed[dname] = 'No such dataset or directory'
      else:
        directoryPaths[directoryIDs[dirPath]] = dirPath

    wheres = []
    for dirPath in directoryIDs:
      dsNames = dirDict[dirPath]
      dirID = directoryIDs[dirPath]
      wheres.append( "( DirID=%d AND DatasetName IN (%s) )" % ( dirID, stringListToString( dsNames ) ) )

    req = "SELECT DatasetName,DirID,DatasetID FROM FC_MetaDatasets WHERE %s" % " OR ".join( wheres )
    result = self.db._query(req,connection)
    if not result['OK']:
      return result
    for dsName, dirID, dsID in result['Value']:
      dname = '%s/%s' % ( directoryPaths[dirID], dsName )
      dname = dname.replace('//','/')
      successful[dname] = { "DirID": dirID, "DatasetID": dsID }

    for dataset in datasets:
      if not dataset in successful:
        failed[dataset] = "No such dataset"

    return S_OK({"Successful":successful,"Failed":failed})

  def __findNoPathDatasets( self, nodirDatasets, connection ):

    failed = {}
    successful = {}
    dsIDs = {}
    req = "SELECT COUNT(DatasetName),DatasetName,DatasetID FROM FC_MetaDatasets WHERE DatasetName in "
    req += "( %s ) GROUP BY DatasetName" % stringListToString( nodirDatasets )
    result = self.db._query( req, connection )
    if not result['OK']:
      return result
    for dsCount, dsName, dsID in result['Value']:
      if dsCount > 1:
        failed[dsName] = "Ambiguous dataset name"
      else:
        dsIDs[dsName] = str( dsID )

    if dsIDs:
      req = "SELECT DatasetName,DatasetID,DirID FROM FC_MetaDatasets WHERE DatasetID in (%s)" % ','.join( dsIDs.values() )
      result = self.db._query( req, connection )
      if not result['OK']:
        return result
      for dsName, dsID, dirID in result['Value']:
        successful[dsName] = { "DirID": dirID, "DatasetID": dsID }

    for name in nodirDatasets:
      if name not in failed and name not in successful:
        failed[name] = "Dataset not found"

    return S_OK({"Successful":successful,"Failed":failed})

  def addDatasetAnnotation( self, datasets, credDict ):
    """ Add annotation to the given dataset
    """
    connection = self._getConnection()
    successful = {}
    result = self._findDatasets( datasets.keys(), connection )
    if not result['OK']:
      return result
    failed = result['Value']['Failed']
    datasetDict = result['Value']['Successful']
    for dataset, annotation in datasets.items():
      if dataset in datasetDict:
        req = "REPLACE FC_DatasetAnnotations (Annotation,DatasetID) VALUE ('%s',%d)" % (annotation,datasetDict[dataset]['DatasetID'])
        result = self.db._update( req, connection)
        if not result['OK']:
          failed[dataset] = "Failed to add annotation"
        else:
          successful[dataset] = True

    return S_OK( {'Successful':successful, 'Failed':failed} )

  def getDatasetAnnotation( self, datasets, credDict = {} ):
    """ Get annotations for the given datasets
    """
    successful = {}
    failed = {}
    if datasets:
      result = self._findDatasets( datasets )
      if not result['OK']:
        return result
      dsDict = {}
      for dataset in result['Value']['Successful']:
        dsDict[result['Value']['Successful'][dataset]['DatasetID']] = dataset
      for dataset in result['Value']['Failed']:
        failed[dataset] = "Dataset not found"

      idString = ','.join( [ str(x) for x in dsDict.keys() ] )
      req = "SELECT DatasetID, Annotation FROM FC_DatasetAnnotations WHERE DatasetID in (%s)" % idString
    else:
      req = "SELECT DatasetID, Annotation FROM FC_DatasetAnnotations"
    result = self.db._query( req )
    if not result['OK']:
      return result

    for dsID, annotation in result['Value']:
      successful[dsDict[dsID]] = annotation

    if datasets:
      for dataset in datasets:
        if dataset not in successful and dataset not in failed:
          successful[dataset] = '-'

    return S_OK( {'Successful':successful, 'Failed':failed} )

  def __getMetaQueryParameters( self, metaQuery, credDict ):
    """ Get parameters ( hash, total size, number of files ) for the given metaquery
    """
    findMetaQuery = dict( metaQuery )

    path = '/'
    if "Path" in findMetaQuery:
      path = findMetaQuery['Path']
      findMetaQuery.pop( 'Path' )

    result = self.db.fmeta.findFilesByMetadata( findMetaQuery, path, credDict, extra=True )
    if not result['OK']:
      return S_ERROR( 'Failed to apply the metaQuery' )
    if isinstance( result['Value'], list ):
      lfnList = result['Value']
    elif isinstance( result['Value'], dict ):
      # Process into the lfn list
      lfnList = []
      for dir_,fList in result['Value'].items():
        for f in fList:
          lfnList.append(dir_+'/'+f)
    lfnIDDict = result.get( 'LFNIDDict', {} )
    lfnIDList = result.get( 'LFNIDList', [] )
    if not lfnIDList:
      lfnIDList = lfnIDDict.keys()
    lfnList.sort()
    myMd5 = md5.md5()
    myMd5.update( str( lfnList ) )
    datasetHash = myMd5.hexdigest().upper()
    numberOfFiles = len( lfnList )
    result = self.db.fileManager.getFileSize( lfnList )
    totalSize = 0
    if result['OK']:
      totalSize = result['TotalSize']

    result = S_OK( { 'DatasetHash': datasetHash,
                     'NumberOfFiles': numberOfFiles,
                     'TotalSize': totalSize,
                     'LFNList': lfnList,
                     'LFNIDList': lfnIDList } )
    return result

  def removeDataset( self, datasets, credDict ):
    """ Remove the requested datasets

    :param dict datasets: dictionary describing dataset definitions
    :param credDict:  dictionary of the caller credentials
    :return: S_OK/S_ERROR bulk return structure
    """
    failed = dict()
    successful = dict()
    for datasetName in datasets:
      result = self.__removeDataset( datasetName, credDict )
      if result['OK']:
        successful[datasetName] = True
      else:
        failed[datasetName] = result['Message']

    return S_OK( { "Successful": successful, "Failed": failed } )


  def __removeDataset( self, datasetName, credDict ):
    """ Remove existing dataset
    """

    req = "SELECT DatasetID FROM FC_MetaDatasets WHERE DatasetName='%s'" % datasetName
    result = self.db._query( req )
    if not result['OK']:
      return result
    if not result['Value']:
      # No requested dataset
      return S_OK( 'Dataset %s does not exist' % datasetName  )
    datasetID = result['Value'][0][0]

    for table in ["FC_MetaDatasetFiles","FC_MetaDatasets","FC_DatasetAnnotations"]:
      req = "DELETE FROM %s WHERE DatasetID=%s" % (table, datasetID)
      result = self.db._update( req )

    return result

  def checkDataset( self, datasets, credDict ):
    """ Check that the dataset parameters correspond to the actual state

    :param dict datasets: dictionary describing dataset definitions
    :param credDict:  dictionary of the caller credentials
    :return: S_OK/S_ERROR bulk return structure
    """
    failed = dict()
    successful = dict()
    for datasetName in datasets.keys():
      result = self.__checkDataset( datasetName, credDict )
      if result['OK']:
        successful[datasetName] = result['Value']
      else:
        failed[datasetName] = result['Message']

    return S_OK( { "Successful": successful, "Failed": failed } )

  def __checkDataset( self, datasetName, credDict ):
    """ Check that the dataset parameters correspond to the actual state
    """
    req = "SELECT MetaQuery,DatasetHash,TotalSize,NumberOfFiles FROM FC_MetaDatasets"
    req += " WHERE DatasetName='%s'" % datasetName
    result = self.db._query( req )
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR( 'Unknown MetaDataset %s' % datasetName )

    row = result['Value'][0]
    metaQuery = eval( row[0] )
    datasetHashOld = row[1]
    totalSizeOld = int( row[2] )
    numberOfFilesOld = int( row[3] )

    result = self.__getMetaQueryParameters( metaQuery, credDict )
    if not result['OK']:
      return result
    totalSize = result['Value']['TotalSize']
    datasetHash = result['Value']['DatasetHash']
    numberOfFiles = result['Value']['NumberOfFiles']

    changeDict = {}
    if totalSize != totalSizeOld:
      changeDict['TotalSize'] = ( totalSizeOld, totalSize )
    if datasetHash != datasetHashOld:
      changeDict['DatasetHash'] = ( datasetHashOld, datasetHash )
    if numberOfFiles != numberOfFilesOld:
      changeDict['NumberOfFiles'] = ( numberOfFilesOld, numberOfFiles )

    result = S_OK( changeDict )
    return result

  def updateDataset( self, datasets, credDict ):
    """
    :param dict datasets: dictionary describing dataset definitions
    :param credDict:  dictionary of the caller credentials
    :return: S_OK/S_ERROR bulk return structure
    """

    failed = dict()
    successful = dict()

    for datasetName in datasets.keys():
      result = self.__updateDataset( datasetName, credDict )
      if result['OK']:
        successful[datasetName] = result['Value']
      else:
        failed[datasetName] = result['Message']

    return S_OK( { "Successful": successful, "Failed": failed } )

  def __updateDataset( self, datasetName, credDict ):
    """ Update the dataset parameters
    """

    changeDict = {}
    result = self.__checkDataset( datasetName, credDict )
    if not result['OK']:
      return result
    if not result['Value']:
      # The dataset is not changed
      return S_OK()
    else:
      changeDict = result['Value']

    req = "UPDATE FC_MetaDatasets SET "
    for field in changeDict:
      req += "%s='%s', " % ( field, str( changeDict[field][1] ) )
    req += "ModificationDate=UTC_TIMESTAMP() "
    req += "WHERE DatasetName='%s'" % datasetName
    result = self.db._update( req )
    return result

  def getDatasets( self, datasets, credDict ):
    """ Get dataset definitions

    :param dict datasets: dictionary describing dataset definitions
    :param credDict:  dictionary of the caller credentials
    :return: S_OK/S_ERROR bulk return structure
    """
    failed = dict()
    successful = dict()
    for datasetName in datasets:
      result = self.__getDatasets( datasetName )
      if result['OK']:
        successful[datasetName] = result['Value']
      else:
        failed[datasetName] = result['Message']

    return S_OK( { "Successful": successful, "Failed": failed } )

  def __getDatasets( self, datasetName ):
    """ Get information about existing datasets
    """

    parameterList = ['DatasetID','MetaQuery','DirID','TotalSize','NumberOfFiles',
                     'UID','GID','Status','CreationDate','ModificationDate',
                     'DatasetHash','Mode','DatasetName']
    parameterString = ','.join( parameterList )

    req = "SELECT %s FROM FC_MetaDatasets" % parameterString
    dsName = os.path.basename(datasetName)
    if '*' in dsName:
      dName = dsName.replace( '*', '%' )
      req += " WHERE DatasetName LIKE '%s'" % dName
    elif dsName:
      req += " WHERE DatasetName='%s'" % dsName

    result = self.db._query( req )
    if not result['OK']:
      return result

    resultDict = {}
    for row in result['Value']:
      dName = row[12]
      resultDict[dName] = self.__getDatasetDict( row )

    return S_OK( resultDict )

  def getDatasetsInDirectory( self, dirID, verbose = False, connection = False ):
    """ Get datasets in the given directory
    """
    datasets = {}
    parameterList = ['DatasetName','DatasetID','MetaQuery','DirID','TotalSize','NumberOfFiles',
                     'UID','GID','Status','CreationDate','ModificationDate',
                     'DatasetHash','Mode']
    parameterString = ','.join( parameterList )

    req = "SELECT %s FROM FC_MetaDatasets WHERE DirID=%s" % ( parameterString, str( dirID) )
    result = self.db._query( req )
    if not result['OK']:
      return result
    userDict = {}
    groupDict = {}
    for row in result['Value']:
      dsName = row[0]
      datasets[dsName] = {}
      dsDict = {}
      for i in range( 1, len( row ), 1 ):
        dsDict[parameterList[i]] = row[i]
        if parameterList[i] == 'UID':
          uid = row[i]
          if uid in userDict:
            owner = userDict[uid]
          else:
            owner = 'unknown'
            result = self.db.ugManager.getUserName( uid )
            if result['OK']:
              owner = result['Value']
            userDict[uid] = owner
          dsDict[dsName]['Owner'] = owner
        if parameterList[i] == 'GID':
          gid = row[i]
          if gid in groupDict:
            group = groupDict[gid]
          else:
            group = 'unknown'
            result = self.db.ugManager.getGroupName( gid )
            if result['OK']:
              group = result['Value']
            groupDict[gid] = group
          dsDict[dsName]['OwnerGroup'] = group
      datasets[dsName]['Metadata'] = dsDict

    if verbose and datasets:
      result = self.getDatasetAnnotation( datasets.keys() )
      if result['OK']:
        for dataset in result['Value']['Successful']:
          datasets[dataset]['Annotation'] = result['Value']['Successful'][dataset]
        for dataset in result['Value']['Failed']:
          datasets[dataset]['Annotation'] = result['Value']['Failed'][dataset]

    return S_OK( datasets )

  def __getDatasetDict( self, row ):

    resultDict = {}
    resultDict['DatasetID'] = int( row[0] )
    resultDict['MetaQuery'] = eval( row[1] )
    resultDict['DirID'] = int( row[2] )
    resultDict['TotalSize'] = int( row[3] )
    resultDict['NumberOfFiles'] = int( row[4] )
    uid = int( row[5] )
    gid = int( row[6] )
    result = self.db.ugManager.getUserName( uid )
    if result['OK']:
      resultDict['Owner'] = result['Value']
    else:
      resultDict['Owner'] = 'Unknown'
    result = self.db.ugManager.getGroupName( gid )
    if result['OK']:
      resultDict['OwnerGroup'] = result['Value']
    else:
      resultDict['OwnerGroup'] = 'Unknown'
    intStatus = int( row[7] )
    result = self.db.fileManager._getIntStatus( intStatus )
    if result['OK']:
      resultDict['Status'] = result['Value']
    else:
      resultDict['Status'] = 'Unknown'
    resultDict['CreationDate'] = row[8]
    resultDict['ModificationDate'] = row[9]
    resultDict['DatasetHash'] = row[10]
    resultDict['Mode'] = row[11]

    return resultDict

  def getDatasetParameters( self, datasets, credDict ):
    """ Get dataset definitions

    :param dict datasets: dictionary describing dataset definitions
    :param credDict:  dictionary of the caller credentials
    :return: S_OK/S_ERROR bulk return structure
    """
    failed = dict()
    successful = dict()
    for datasetName in datasets:
      result = self.__getDatasetParameters( datasetName, credDict )
      if result['OK']:
        successful[datasetName] = result['Value']
      else:
        failed[datasetName] = result['Message']

    return S_OK( { "Successful": successful, "Failed": failed } )

  def __getDatasetParameters( self, datasetName, credDict ):
    """ Get the currently stored dataset parameters
    """
    result = self._findDatasets( [datasetName] )
    if not result['OK']:
      return result
    if not result['Value']['Successful']:
      return S_ERROR( result['Value']['Failed'][datasetName] )
    dirID = result['Value']['Successful'][datasetName]['DirID']
    dsName = os.path.basename( datasetName )

    parameterList = ['DatasetID','MetaQuery','DirID','TotalSize','NumberOfFiles',
                     'UID','GID','Status','CreationDate','ModificationDate','DatasetHash','Mode']
    parameterString = ','.join( parameterList )

    req = "SELECT %s FROM FC_MetaDatasets WHERE DatasetName='%s' AND DirID=%d" % ( parameterString, dsName, dirID )
    result = self.db._query( req )
    if not result['OK']:
      return result

    row = result['Value'][0]
    resultDict = self.__getDatasetDict( row )

    return S_OK( resultDict )

  def setDatasetStatus( self, datasetName, status ):
    """ Set the given dataset status
    """
    result = self.db.fileManager._getStatusInt( status )
    if not result['OK']:
      return result
    intStatus = result['Value']
    req = "UPDATE FC_MetaDatasets SET Status=%d, ModificationDate=UTC_TIMESTAMP() " % intStatus
    req += "WHERE DatasetName='%s'" % datasetName
    result = self.db._update( req )
    return result

  def getDatasetStatus( self, datasetName, credDict ):
    """ Get status of the given dataset
    """

    result = self.__getDatasetParameters( datasetName, credDict )
    if not result['OK']:
      return result
    status = result['Value']['Status']
    return S_OK( status )

  def __getDynamicDatasetFiles( self, datasetID, credDict ):
    """ Get dataset lfns from a dynamic meta query
    """
    req = "SELECT MetaQuery FROM FC_MetaDatasets WHERE DatasetID=%d" % datasetID
    result = self.db._query( req )
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR( 'Unknown MetaDataset ID %d' % datasetID )

    metaQuery = eval( result['Value'][0][0] )
    result = self.__getMetaQueryParameters( metaQuery, credDict )
    if not result['OK']:
      return result

    lfnList = result['Value']['LFNList']
    finalResult = S_OK(lfnList)
    finalResult['FileIDList'] = result['Value']['LFNIDList']
    return finalResult

  def __getFrozenDatasetFiles( self, datasetID, credDict ):
    """ Get dataset lfns from a frozen snapshot
    """

    req = "SELECT FileID FROM FC_MetaDatasetFiles WHERE DatasetID=%d" % datasetID
    result = self.db._query( req )
    if not result['OK']:
      return result

    fileIDList = [ row[0] for row in result['Value'] ]
    result = self.db.fileManager._getFileLFNs( fileIDList )
    if not result['OK']:
      return result

    lfnDict = result['Value']['Successful']
    lfnList = [ lfnDict[i] for i in lfnDict.keys() ]
    result = S_OK( lfnList )
    result['FileIDList'] = lfnDict.keys()
    return result

  def getDatasetFiles( self, datasets, credDict ):
    """ Get dataset file contents

    :param dict datasets: dictionary describing dataset definitions
    :param credDict:  dictionary of the caller credentials
    :return: S_OK/S_ERROR bulk return structure
    """
    failed = dict()
    successful = dict()
    for datasetName in datasets:
      result = self.__getDatasetFiles( datasetName, credDict )
      if result['OK']:
        successful[datasetName] = result['Value']
      else:
        failed[datasetName] = result['Message']

    return S_OK( { "Successful": successful, "Failed": failed } )

  def __getDatasetFiles( self, datasetName, credDict ):
    """ Get dataset files
    """
    result = self.__getDatasetParameters( datasetName, credDict )
    if not result['OK']:
      return result
    status = result['Value']['Status']
    datasetID = result['Value']['DatasetID']
    if status in ["Frozen","Static"]:
      return self.__getFrozenDatasetFiles( datasetID, credDict )
    else:
      return self.__getDynamicDatasetFiles( datasetID, credDict )

  def freezeDataset( self, datasets, credDict ):
    """ Freeze the contents of datasets

    :param dict datasets: dictionary describing dataset definitions
    :param credDict:  dictionary of the caller credentials
    :return: S_OK/S_ERROR bulk return structure
    """
    failed = dict()
    successful = dict()
    for datasetName in datasets:
      result = self.__freezeDataset( datasetName, credDict )
      if result['OK']:
        successful[datasetName] = True
      else:
        failed[datasetName] = result['Message']

    return S_OK( { "Successful": successful, "Failed": failed } )

  def __freezeDataset( self, datasetName, credDict ):
    """ Freeze the contents of the dataset
    """
    result = self.__getDatasetParameters( datasetName, credDict )
    if not result['OK']:
      return result
    status = result['Value']['Status']
    if status == "Frozen":
      return S_OK()

    datasetID = result['Value']['DatasetID']
    req = "DELETE FROM FC_MetaDatasetFiles WHERE DatasetID=%d" % datasetID
    result = self.db._update( req )

    result = self.__getDynamicDatasetFiles( datasetID, credDict )

    if not result['OK']:
      return result
    fileIDList = result['FileIDList']
    valueList = []
    for fileID in fileIDList:
      valueList.append( '(%d,%d)' % (datasetID,fileID) )
    valueString = ','.join( valueList )
    req = "INSERT INTO FC_MetaDatasetFiles (DatasetID,FileID) VALUES %s" % valueString
    result = self.db._update( req )
    if not result['OK']:
      return result

    result = self.setDatasetStatus( datasetName, 'Frozen' )
    return result

  def releaseDataset( self, datasets, credDict ):
    """ Unfreeze datasets

    :param dict datasets: dictionary describing dataset definitions
    :param credDict:  dictionary of the caller credentials
    :return: S_OK/S_ERROR bulk return structure
    """
    failed = dict()
    successful = dict()
    for datasetName in datasets:
      result = self.__releaseDataset( datasetName, credDict )
      if result['OK']:
        successful[datasetName] = True
      else:
        failed[datasetName] = result['Message']

    return S_OK( { "Successful": successful, "Failed": failed } )


  def __releaseDataset( self, datasetName, credDict ):
    """ return the dataset to a dynamic state
    """
    result = self.__getDatasetParameters( datasetName, credDict )
    if not result['OK']:
      return result
    status = result['Value']['Status']
    if status == "Dynamic":
      return S_OK()

    datasetID = result['Value']['DatasetID']
    req = "DELETE FROM FC_MetaDatasetFiles WHERE DatasetID=%d" % datasetID
    result = self.db._update( req )

    result = self.setDatasetStatus( datasetName, 'Dynamic' )
    return result

