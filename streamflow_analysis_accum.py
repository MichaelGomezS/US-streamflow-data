# The difference in this second approach is that instead of using the qth quantile as the streamflow, we use the sum of all the
# streamflows above a threshold defined by the qth quantile as the streamflow value
import pandas as pd
import numpy as np
import os

# Site numbers 
gage_data = pd.read_csv('/gpfs/group/aim127/default/Michael/FEW/vulnerability/data/streamflow_USGS/Gaged_streamflow_fixedHUC_removedND.csv', na_values = -99, dtype={'STATION_NO':str, 'HUC8_1':str})
sites = gage_data.apply(lambda x:x.NWIS_URL.split('site_no=')[-1], axis=1).unique().tolist()

# Import streamflow database
flow_df = pd.read_csv('../output/streamflow_analysis/streamflow_daily_data_allgages.csv')

q = 0.01
path = '../output/streamflow_analysis_accum/'
folder = 'quantile_'+str(q)
# Create directory to store results
try:
	os.mkdir(path+folder)
except OSError:
	print('Creation of the directory failed')

# Aggregate by year and calculate the qth percentile of the flow distribution
## Convert data column to pandas datetime format
flow_df['datepd'] = pd.to_datetime(flow_df['date'])
## Set datetime as index
flow_df = flow_df.set_index(flow_df['datepd']).drop(columns = ['date','datepd'])
## See number of records per year per gage
flow_df_count = flow_df.resample('Y').count()
## Sort columns to see columns with less  observations
flow_df_count_sort = flow_df_count.reindex(flow_df_count.mean().sort_values().index, axis=1)
flow_df_count_sort_t = flow_df_count_sort.T
cols = flow_df_count_sort_t.columns
flow_df_count_sort_t = flow_df_count_sort_t.rename(columns={cols[0]:'Count_2012', cols[1]:'Count_2013',cols[2]:'Count_2014',cols[3]:'Count_2015'}).drop(columns=cols[4])
flow_df_count_sort_t.to_csv(path+folder+'/streamflow_count.csv')

## Resample by year and get qth percentile in the yearly flow distribution
flow_df_sort = flow_df.reindex(flow_df_count.mean().sort_values().index, axis=1)
### define a function to get the sum of all flows that exceed the qth percentile threshold
def accum_flow(df_grouped,quant):
	threshold = df_grouped.apply(lambda x:x.quantile(q))
	accum = df_grouped[df_grouped>threshold].sum()
	accum_SI = accum.apply(lambda x:x*0.3048**3)
	return accum_SI	

#flow_df_sort_quant = flow_df_sort.resample('Y').apply(lambda x:x.quantile(q))
flow_df_sort_accum = flow_df_sort.resample('Y').apply(accum_flow, quant=q)
flow_df_sort_accum_t = flow_df_sort_accum.T
cols = flow_df_sort_accum_t.columns
flow_df_sort_accum_t = flow_df_sort_accum_t.rename(columns={cols[0]:'Flow_p'+str(q)+'_2012', cols[1]:'Flow_p'+str(q)+'_2013', cols[2]:'Flow_p'+str(q)+'_2014', cols[3]:'Flow_p'+str(q)+'_2015'}).drop(columns=cols[4])
#flow_df_sort_accum_t = flow_df_sort_accum_t.replace(0,np.nan)
flow_df_sort_accum_t.to_csv(path+folder+'/streamflow_q'+str(q)+'.csv')

# Final dataframe
## Merge 20th percentile flow and record counts
final_df = flow_df_sort_accum_t.merge(flow_df_count_sort_t, how = 'left', left_index=True, right_index=True).reset_index().rename(columns={'index':'Qsite'})
## Add station number
final_df['STATION_NO'] = final_df.apply(lambda x:x.Qsite[2:], axis =1)
## Add station name, lat, lon, drainage area, URL, HUC8 area code
cols = ['STATION_NM', 'STATION_NO', 'LATDD', 'LONDD', 'P50', 'DRAIN_AREA', 'NWIS_URL', 'HUC8_1', 'NAME']
final_df = final_df.merge(gage_data[cols], how='left', on='STATION_NO')
final_df.to_csv(path+folder+'/streamflow_q'+str(q)+'_complete.csv')
