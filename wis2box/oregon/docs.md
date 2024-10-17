Oregon uses an ESRI server and you can get all the metadata about each monitoring site by going to: https://gis.wrd.state.or.us/server/rest/services/dynamic/Gaging_Stations_WGS84/FeatureServer/2/query
- to see all data you need to use `where=*`
- to see all fields you need to specify `out_fields=*`
- to filter by station numbers you need to use an `in` clause, e.g. `where=station_nbr in (10371500, 10378500)`

Data for monitoring sites can be downloaded at sites similar to https://apps.wrd.state.or.us/apps/sw/hydro_near_real_time/display_hydro_graph.aspx?station_nbr=10378500

TSV data can be downloaded programmatically by going to sites like the following: (https://apps.wrd.state.or.us/apps/sw/hydro_near_real_time/hydro_download.aspx?station_nbr=10371500&start_date=9/29/2023%2012:00:00%20AM&end_date=10/7/2024%2012:00:00%20AM&dataset=MDF&format=tsv)
- Sometimes you need to specify units in the URL params, but sometimes you do not
- If the query is invalid sometimes the API still returns a success status code but contains an error message in the response
