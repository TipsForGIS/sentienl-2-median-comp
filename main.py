from sentinel2_median_comp import Sentinel2_median_comp as smc
import sys
from time import time


# create a Sentinel2_median_comp object
snt2mc = smc(sys.argv)

# get a pandas df with metadata of most 5 recent images containing the geojson footprint given
recent_5_imgs_mdata_df = snt2mc.get_recent_5_imgs_mdata_df()

# download images using the metadata pandas df 
snt2mc.download_from_mdata_df(recent_5_imgs_mdata_df,'./download/')

# generate xr-rio images list and profile
imgs_lst, imgs_profile = snt2mc.get_xr_darrs_and_profile('./download/')

# get the median composite from imgs_lst
median_comp = snt2mc.get_median_composite(imgs_lst,3)

# save the median composite
snt2mc.save_median_composite_in_tif(median_comp,'./output/median2.tif')
