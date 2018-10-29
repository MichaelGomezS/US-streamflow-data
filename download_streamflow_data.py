import pandas as pd
import numpy as np 
#import wget
import urllib.request
import timeout_decorator
import time

# Download data from USGS database
## Import streamflow data from USGS shapefile
gage_data = pd.read_csv('../data/streamflow_USGS/Gaged_streamflow_fixedHUC_removedND.csv', na_values = -99)
## Get site number from streamflow data
sites = gage_data.apply(lambda x:x.NWIS_URL.split('site_no=')[-1], axis=1).tolist()
## URL format found in explorer
url1 = 'https://nwis.waterdata.usgs.gov/usa/nwis/uv/?cb_00060=on&format=rdb&site_no='
url2 = '&period=&begin_date=2012-01-01&end_date=2016-01-01'
## Folder to save downloaded files 
output_dir = '../data/gagedata_USGS/'
## Lists with URLS, filenames and output directories of all streamgages
urls = [url1 + i + url2 for i in sites]
filenames = ['site_no=' + i + '.txt' for i in sites]
outputs = [output_dir + i for i in filenames]
## Define a function with a timer to handle errors when downloading
@timeout_decorator.timeout(60)
def download_data(url, output):
	urllib.request.urlretrieve(url, output)

## Open text files to store errors
file1 = open('hrl_long_wait_time.txt', 'w')
file2 = open('hrl_not_downloaded.txt', 'w')

r = 2458
i = r
for u,f,o in zip(urls[r:], filenames[r:], outputs[r:]):
	try:
		download_data(u, o)
		print(i, f)
		i = i +1
	except TimeoutError:
		print('Downloading file took longer than 20 seconds: '+str(i)+' - '+f)
		file1.write('Downloading file took longer than 20 seconds: '+str(i)+' - '+f)
		i = i +1
	except (KeyboardInterrupt, SystemExit):
		raise
	except:
		print('Error in downloading file # '+str(i)+' - '+f)
		file2.write('Error in downloading file # '+str(i)+' - '+f)
		i = i +1

file1.close()
file2.close()

