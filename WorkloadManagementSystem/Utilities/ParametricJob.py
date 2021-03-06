""" Utilities to process parametric job definitions and generate
    bunches of parametric jobs. It exposes the following functions:

    getNumberOfParameters() - to get the total size of the bunch of parametric jobs
    generateParametricJobs() - to get a list of expanded descriptions of all the jobs
"""

__RCSID__ = "$Id$"

import re

from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC.Core.Utilities.ReturnValues import S_OK, S_ERROR
from DIRAC.Core.Utilities.DErrno import EWMSJDL

def __getParameterSequence( nPar, parList = [], parStart = 1, parStep = 0, parFactor = 1 ):

  if parList:
    if nPar != len( parList ):
      return []
    else:
      parameterList = list( parList )
  else:
    # The first parameter must have the same type as the other ones even if not defined explicitly
    parameterList = [ parStart * type( parFactor )( 1 ) + type( parStep )( 0 ) ]
    for np in range( 1, nPar ):
      parameterList.append( parameterList[np - 1] * parFactor + parStep )

  return parameterList

def getNumberOfParameters( jobClassAd ):
  """ Get the number of parameters in the parametric job description

  :param jobClassAd: ClassAd job description object
  :return: int number of parameters, 0 if not a parametric job
  """
  if jobClassAd.lookupAttribute( 'Parameters' ):
    if jobClassAd.isAttributeList( 'Parameters' ):
      parameterList = jobClassAd.getListFromExpression( 'Parameters' )
      return len( parameterList )
    else:
      return jobClassAd.getAttributeInt( 'Parameters' )
  else:
    return None

def __updateAttribute( classAd, attribute, parName, parValue ):

  # If there is something to do:
  pattern = '%%\(%s\)s' % parName
  if parName == '0':
    pattern = "%s"
  expr = classAd.get_expression( attribute )
  if not re.search( pattern, expr ):
    return False

  pattern = '%%(%s)s' % parName
  if parName == '0':
    pattern = "%s"

  parValue = parValue.strip()
  if classAd.isAttributeList( attribute):
    parValue = parValue.strip()
    if parValue.startswith( '{' ):
      parValue = parValue.lstrip( '{' ).rstrip( '}' ).strip()

  expr = classAd.get_expression( attribute )
  newexpr = expr.replace( pattern, str( parValue ) )
  classAd.set_expression( attribute, newexpr )
  return True

def generateParametricJobs( jobClassAd ):
  """ Generate a series of ClassAd job descriptions expanding
      job parameters

  :param jobClassAd: ClassAd job description object
  :return: list of ClassAd job description objects
  """
  if not jobClassAd.lookupAttribute( 'Parameters' ):
    return S_OK( [ jobClassAd.asJDL() ] )

  nParameters = getNumberOfParameters( jobClassAd )
  if nParameters is None:
    return S_ERROR( EWMSJDL, 'Can not determine number of job parameters' )
  if nParameters <= 0:
    return S_ERROR( EWMSJDL, 'Illegal number of job parameters %d' % ( nParameters ) )

  parameterDict = {}
  attributes = jobClassAd.getAttributes()
  for attribute in attributes:
    for key in [ 'Parameters', 'ParameterStart', 'ParameterStep', 'ParameterFactor' ]:
      if attribute.startswith( key ):
        seqID = '0' if not '.' in attribute else attribute.split( '.' )[1]
        parameterDict.setdefault( seqID, {} )
        if key == 'Parameters':
          if jobClassAd.isAttributeList( attribute ):
            parList = jobClassAd.getListFromExpression( attribute )
            if len( parList ) != nParameters:
              return S_ERROR( EWMSJDL, 'Inconsistent parametric job description' )
            parameterDict[seqID]['ParameterList'] = parList
          else:
            if attribute != "Parameters":
              return S_ERROR( EWMSJDL, 'Inconsistent parametric job description' )
            nPar = jobClassAd.getAttributeInt( attribute )
            if nPar is None:
              value = jobClassAd.get_expression( attribute )
              return S_ERROR( EWMSJDL, 'Inconsistent parametric job description: %s=%s' % ( attribute, value ) )
            parameterDict[seqID]['Parameters'] = nPar
        else:
          value = jobClassAd.getAttributeInt( attribute )
          if value is None:
            value = jobClassAd.getAttributeFloat( attribute )
            if value is None:
              value = jobClassAd.get_expression( attribute )
              return S_ERROR( 'Illegal value for %s JDL field: %s' % ( attribute, value ) )
          parameterDict[seqID][key] = value

  if '0' in parameterDict and not parameterDict.get( '0' ):
    parameterDict.pop( '0' )

  parameterLists = {}
  for seqID in parameterDict:
    parList = __getParameterSequence( nParameters,
                                      parList = parameterDict[seqID].get( 'ParameterList', [] ),
                                      parStart = parameterDict[seqID].get( 'ParameterStart', 1 ),
                                      parStep = parameterDict[seqID].get( 'ParameterStep', 0 ),
                                      parFactor = parameterDict[seqID].get( 'ParameterFactor', 1 )
                                    )
    if not parList:
      return S_ERROR( EWMSJDL, 'Inconsistent parametric job description' )

    parameterLists[seqID] = parList

  jobDescList = []
  jobDesc = jobClassAd.asJDL()
  # Width of the sequential parameter number
  zLength = len( str( nParameters - 1 ) )
  for n in range( nParameters ):
    newJobDesc = jobDesc
    newJobDesc = newJobDesc.replace( '%n', str( n ).zfill( zLength ) )
    newClassAd = ClassAd( newJobDesc )
    for seqID in parameterLists:
      parameter = parameterLists[seqID][n]
      for attribute in newClassAd.getAttributes():
        __updateAttribute( newClassAd, attribute, seqID, str( parameter ) )

    for seqID in parameterLists:
      for attribute in ['Parameters', 'ParameterStart', 'ParameterStep', 'ParameterFactor']:
        if seqID == '0':
          newClassAd.deleteAttribute( attribute )
        else:
          newClassAd.deleteAttribute( '%s.%s' % ( attribute, seqID ) )

      parameter = parameterLists[seqID][n]
      if seqID == '0':
        attribute = 'Parameter'
      else:
        attribute = 'Parameter.%s' % seqID
      if isinstance( parameter, basestring) and parameter.startswith( '{' ):
        newClassAd.insertAttributeInt( attribute, str( parameter ) )
      else:
        newClassAd.insertAttributeString( attribute, str( parameter ) )

    newClassAd.insertAttributeInt( 'ParameterNumber', n )
    newJDL = newClassAd.asJDL()
    jobDescList.append( newJDL )

  return S_OK( jobDescList )
