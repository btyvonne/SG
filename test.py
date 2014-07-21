import argparse
import sys
import MySQLdb
import httplib2
from apiclient.discovery import build
from oauth2client import file
import base64 
import pickle 

from apiclient.errors import HttpError
from apiclient import sample_tools
from oauth2client.client import AccessTokenRefreshError

# import the logging library
import logging

# Get an instance of a logger
logger = logging.getLogger(__name__)

def main(argv):
    credential_encoded = credential
    credential = pickle.loads(base64.b64decode(credential_encoded))
    access_token = credential.access_token
      
    if credential.access_token_expired:
        try:
            h = httplib2.Http()
            credential._refresh(h.request)
            access_token = credential.access_token
        except AccessTokenRefreshError:
            message = 'Can not refresh the access token to account: %s.' %(credential)
            logger.error(message)
             
    http = httplib2.Http()
    http = credential.authorize(http)
    service = build('analytics', 'v3', http=http)

    # Try to make a request to the API. Print the results or handle errors.
    try:
        start_index = 1
        max_results = 10000
        results = get_top_keywords(service, start_index, max_results)
        update_database(results)
        totalResults = results.get("totalResults")
        print totalResults
        remainResults = totalResults - max_results
        start_index = start_index + max_results

        if remainResults > 0:
            while remainResults > 0: 
                if remainResults - max_results > 0 :
                    results = get_top_keywords(service, start_index, max_results)
                    update_database(results)
                    start_index = start_index + max_results
                    remainResults = remainResults - max_results
                else :
                    remainResults = remainResults - max_results;
                    results = get_top_keywords(service, start_index, max_results)
                    update_database(results)



    except TypeError, error:
    # Handle errors in constructing a query.
        print ('There was an error in constructing your query : %s' % error)

    except HttpError, error:
    # Handle API errors.
        print ('Arg, there was an API error : %s : %s' %
           (error.resp.status, error._get_reason()))

    except AccessTokenRefreshError:
    # Handle Auth errors.
        print ('The credentials have been revoked or expired, please re-run '
           'the application to re-authorize')


def get_top_keywords(service, start_index, max_results):
    """Executes and returns data from the Core Reporting API.

    Args:
        service: The service object built by the Google API Python client library.
        profile_id: String The profile ID from which to retrieve analytics data.

    Returns:
        The response returned from the Core Reporting API.
    """

    return service.data().ga().get(
        ids='ga:' + id,
        start_date='2014-06-30',
        end_date='2014-07-16',
        metrics='ga:pageviews',
        dimensions='ga:dimension1, ga:browser, ga:browserVersion, ga:operatingSystem, ga:operatingSystemVersion, ga:screenResolution, ga:deviceCategory',
        start_index=start_index,
        max_results=max_results).execute()


def update_database(results):

    print
    print 'Profile Name: %s' % results.get('profileInfo').get('profileName')
    print
    
    conn = MySQLdb.connect(host = host,port = port, user = user, passwd = passwd, db = db)
    
    cursor = conn.cursor()
   

    for row in results.get('rows'):

        ID = row[0]
        Browser_Type = row[1]
        Browser_Version = row[2]
        OS_Type = row[3]
        OS_Version = row[4]
        Screen_Res = row[5]
        Device_Cat = row[6]

        insertstmt = ("INSERT INTO table(Visitor_ID, Browser_Type, Browser_Version, Operation_System, Operation_System_Version, Screen_Resolution, Device_Category) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (ID, Browser_Type,  Browser_Version, OS_Type, OS_Version, Screen_Res, Device_Cat))  

        try:
            cursor.execute(insertstmt)
            conn.commit()
        except TypeError as error:
            print 'Arg, there was an API error : %s ' % (error.message)
            conn.rollback()
                

    
    conn.close()


if __name__ == '__main__':
    main(sys.argv)