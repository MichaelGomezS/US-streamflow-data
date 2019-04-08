import pandas as pd
import numpy as np

# Site numbers and filenames
gage_data = pd.read_csv('/gpfs/group/aim127/default/Michael/FEW/vulnerability/data/streamflow_USGS/Gaged_streamflow_fixedHUC_removedND.csv', na_values = -99, dtype={'STATION_NO':str, 'HUC8_1':str})
sites = gage_data.apply(lambda x:x.NWIS_URL.split('site_no=')[-1], axis=1).unique().tolist()
filenames = ['site_no=' + i + '.txt' for i in sites]
file1 = open('errorlog_FlowAnalysis.txt', 'w')
# Import all streamflow files and save into one dataframe
for n,f in enumerate(filenames):
	try:
		df = pd.read_table('/gpfs/group/aim127/default/Michael/FEW/vulnerability/data/gagedata_daily_USGS/'+f, comment='#', header=1, na_values = -99)
		if df.empty == False:
			df = df[['20d', '14n']].rename(index=str, columns={'20d':'date', '14n':'Q_'+sites[n]})
			if df['Q_'+sites[n]].dtype == 'O':
				df['Q_'+sites[n]] = pd.to_numeric(df['Q_'+sites[n]])
			if n == 0:
				flow_df = df
			else:
				flow_df = flow_df.merge(df, how = 'outer', on = 'date')
		else:
			a = file1.write('File found but no data for the years selected: '+f+'\n')			
	except:
		a = file1.write('No file found: '+f+'\n')

file1.close()

## write to csv file
flow_df.to_csv('../output/streamflow_analysis/streamflow_daily_data_allgages.csv')

# Aggregate by year and calculate the 20th percentile of the flow distribution
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
flow_df_count_sort_t.to_csv('streamflow_count.csv')
## Resample by year and get 20th percentile in the yearly flow distribution
flow_df_sort = flow_df.reindex(flow_df_count.mean().sort_values().index, axis=1)
flow_df_sort_q20 = flow_df_sort.resample('Y').apply(lambda x:x.quantile(.2))
flow_df_sort_q20_SI = flow_df_sort_q20.apply(lambda x:x*0.3048**3)
flow_df_sort_q20_SI_t = flow_df_sort_q20_SI.T
cols = flow_df_sort_q20_SI_t.columns
flow_df_sort_q20_SI_t = flow_df_sort_q20_SI_t.rename(columns={cols[0]:'Flow_p20_2012', cols[1]:'Flow_p20_2013', cols[2]:'Flow_p20_2014', cols[3]:'Flow_p20_2015'}).drop(columns=cols[4])
flow_df_sort_q20_SI_t.to_csv('streamflow_q20.csv')

# Final dataframe
## Merge 20th percentile flow and record counts
final_df = flow_df_sort_q20_SI_t.merge(flow_df_count_sort_t, how = 'left', left_index=True, right_index=True).reset_index().rename(columns={'index':'Qsite'})
## Add station number
final_df['STATION_NO'] = final_df.apply(lambda x:x.Qsite[2:], axis =1)
## Add station name, lat, lon, drainage area, URL, HUC8 area code
cols = ['STATION_NM', 'STATION_NO', 'LATDD', 'LONDD', 'P20', 'DRAIN_AREA', 'NWIS_URL', 'HUC8_1', 'NAME']
final_df = final_df.merge(gage_data[cols], how='left', on='STATION_NO')
final_df.to_csv('streamflow_q20_complete.csv')
