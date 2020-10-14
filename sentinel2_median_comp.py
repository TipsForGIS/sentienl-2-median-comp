from sentinelsat import SentinelAPI, geojson_to_wkt, read_geojson
import xarray as xr
import rasterio as rio
from zipfile import ZipFile
import os


class Sentinel2_median_comp:

	def __init__(self,conn_args):

		if len(conn_args) == 4:
			self.api = SentinelAPI(conn_args[1],conn_args[2],'https://scihub.copernicus.eu/dhus')
			
			try:
				self.api.query(limit=1,date=('NOW-1HOUR', 'NOW'))
			except Exception as e:
				self.generate_err_msg('Authentication failed! double check your username or password.')

			
			if os.path.exists(conn_args[3]):
				self.footprint_fname = conn_args[3]
			else:
				self.generate_err_msg('Geojson file ({}) does not exist or cannot be accessed!'.format(conn_args[3]))
			
			try:
				self.footprint = geojson_to_wkt(read_geojson(self.footprint_fname))
			except Exception as e:
				self.generate_err_msg('Geojson file ({}) is not structured well!'.format(conn_args[3]))


		else:
			self.generate_err_msg('You should run the program using 3 arguments:\n'
				+'1)username\n2)password\n3)footprint geojson file path\n'
				+'An example is:\npython sentinel2.py <uname> <password> <./footprint.geojson>')


	def generate_err_msg(self,msg):
		print(msg)
		exit()


	def get_recent_5_imgs_mdata_df(self):

		print('getting images metadata from sentinel2 server 📥  📥  📥')
		query_result =  self.api.query(area = self.footprint,
						date = ('NOW-99DAYS', 'NOW'),
						platformname = 'Sentinel-2',
						producttype = 'S2MSI1C',
						area_relation = 'Contains',
						limit = 5,
						order_by = '-datatakesensingstart')

		print('metadata collected successfully ✅')
		return self.api.to_dataframe(query_result)


	def download_from_mdata_df(self, mdata_df, d_path):

		print('downloading images from sentinel2 server 📥  📥  📥')
		self.api.download_all(mdata_df.index, d_path)

		for file in os.listdir(d_path):
			if file.endswith('.zip'):
				with ZipFile(os.path.join(d_path,file), 'r') as zipf:
					zipf.extractall(d_path)
				os.remove(os.path.join(d_path,file))
		
		print('images downloaded successfully ✅')

	
	def get_xr_darrs_and_profile(self,d_path):
		
		print('creating xarray dataarrays and the images profile 🛠  🧰  🛠  🧰')
		tci_images = []
		self.profile = None

		for root, dirs, files in os.walk(d_path, topdown=True):
			for fname in files:
				if fname.endswith('_TCI.jp2'):
					if self.profile is None:
						with rio.open(os.path.join(root, fname)) as img:
							self.profile = img.profile
					tci_images.append(xr.open_rasterio(os.path.join(root, fname) , chunks={'x': 1024, 'y': 1024}))

		print('xarray dataarrays & images profile created successfully ✅')
		return (tci_images, self.profile)

	
	def get_median_composite(self,imgs_lst,no_of_bands):

		print('creating median composite 🛠  🧰  🛠  🧰')
		bands_medians = []
		for b in range(no_of_bands):
			bands = [img.sel(band=b+1) for img in imgs_lst]
			bands_comp = xr.concat(bands, dim='band')
			bands_medians.append(bands_comp.median(dim='band', skipna=True))

		print('median composite created successfully ✅')
		return xr.concat(bands_medians,dim='band')


	def save_median_composite_in_tif(self,median_comp,save_path):

		print('saving median composite 💾')

		with rio.Env():
			self.profile.update(driver='GTiff')
			with rio.open(save_path, 'w', **self.profile) as dst:
				dst.write(median_comp.astype(rio.uint8))

		print('median composite saved successfully in {} ✅'.format(save_path))

		
