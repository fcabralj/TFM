import numpy as np
import uuid
import httplib2
import json

from pprint import pprint

# Imports the Google Cloud client library
from oauth2client.client import GoogleCredentials
from googleapiclient.discovery import build
from google.cloud import bigquery
from datetime import date, timedelta, datetime
from apiclient import discovery

credentials = GoogleCredentials.get_application_default()
service = build('bigquery', 'v2', credentials = credentials)

#credentials = appengine.AppAssertionCredentials(scope=_SCOPE)
http = credentials.authorize(httplib2.Http())
bigquery2 = discovery.build('bigquery', 'v2', http=http)

global client

todayD = date(2017,1,30)
client = bigquery.Client()


def mainFunc ():
	
	buildDecayFactor (todayD, 180, 0.97)

	query = 'Select fecha, total, media from `News.VW_01_avgNews` order by fecha desc' 	

	sync_query(query)

print ('Finished!')
# Build de Decay Factor Vector

def buildDecayFactor (inTodayD, decayObs, decayFactor):

	decayDate = inTodayD 
	global decayArray

	decayArray =[]

	print ('executing decay factor')
	
	for i in range(1, decayObs + 1):

		if i==1:
			decayWeight = 1
		else:

			decayWeight = (decayFactor ** (i))
		
		#decayWeight = (decayFactor ** (i-1)) / (1/(1-decayFactor))
		
		decayDateStr = str(decayDate)
		
		decayArray.append([decayDateStr,i,decayWeight])

		decayDate = todayD - timedelta(days=i)
	
	decayArray = np.asarray(decayArray)

	
# End Decay Factor Function
	
# Recovery values from BigQuery View

def sync_query(query):
	global row

	rowArray =[]

	query_results = client.run_sync_query(query)

	# Use standard SQL syntax for queries.
	# See: https://cloud.google.com/bigquery/sql-reference/

	query_results.use_legacy_sql = False

	query_results.run()

	# Drain the query results by requesting a page at a time.

	page_token = None

	executedTime = str(datetime.now())

	print('executing query')
	
	while True:
		rows, total_rows, page_token = query_results.fetch_data(
			max_results=10,
			page_token=page_token)
#		print(complete)
		
		for row in rows:

			rowArray = np.asarray(row)
			
			if (np.any(decayArray == str(rowArray[0]))):

				i,j = np.where(decayArray == str(rowArray[0]))
				decayValue = float(np.asarray(decayArray[i,2], dtype=np.float))
				
			else:
				decayValue = 0
				
			
			streamData(executedTime, rowArray[0], int(rowArray[1]), float(rowArray[2]), float(rowArray[2]) * decayValue, decayValue)
			#streamData(rowArray[0], '2')
			
		if not page_token:
		
			break

			
			#print ('calculating avg decay')
		
				
#End BigQuery Recovery

def streamData(inexecutedTime, inDate, inTotal, inAverage, inAverageDecay, inDecayValue):
#def streamData(inDate, inTotal):
     
	myProjectID = 'tfm-ieb'
	myDatasetID = 'News'
	myTableName = 'newsDecay'

	data = {u'dateExec':inexecutedTime, u'pubDate':str(inDate), u'total':inTotal, u'media':inAverage, u'decayMedia':inAverageDecay, u'decayFactor':inDecayValue}

	r = service.tables().list(projectId = myProjectID
								  , datasetId = myDatasetID).execute()

	table_exists = [row['tableReference']['tableId'] for row in 
					r['tables'] if 
					row['tableReference']['tableId'] == myTableName]

	if not table_exists:
		print ('Table doesnt existis')
	else:
		 # to do so we'll use the tabledata().insertall() function.
		 body = {
		 			'rows':[
		 				{
		 					'json': data,
		 					'insertId': str(uuid.uuid4())
		 				}

		 			]
		 }

		 response = bigquery2.tabledata().insertAll(
		 	projectId=myProjectID,
		 	datasetId=myDatasetID,
		 	tableId=myTableName,
		 	body=body).execute()

		

mainFunc()