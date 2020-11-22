"""
utils for working with s3
"""
import re
import datetime
import pandas as pd
import tempfile
import numpy as np
import boto3
from botocore.handlers import disable_signing
from netCDF4 import Dataset


class GoesAwsBucket:
    """
    list and download GOES products
    """
    def __init__(self,bucket_name='noaa-goes16'):
        resource = boto3.resource('s3')
        resource.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)
        self.bucket=resource.Bucket(bucket_name)
    
    def get(self,product,time,channel='',fileprefix=''):
        if isinstance(time,(str,)):
            time=datetime.datetime.strptime(time,"%Y-%m-%dT%H:%M:%S")
        year=time.year
        hour=time.hour
        day=int(time.strftime('%j'))
        df=self.list_df(product,year,day,hour,
                        channel=channel,fileprefix=fileprefix)
        if df.empty:
            return None
        # find closest time
        idx = np.argmin(np.abs(df.end_time - time))
        data = self.read(df.iloc[idx])
        return data

    def list(self,
             product='ABI-L2-CMIPC',
             year=2020,    
             day='',       
             hour='',
             channel='',  
             fileprefix=''
            ):
        # Returns list of files
        if product[-1]!='/':
            product+='/'
        year='%s/' % year if year else ''
        day='%.3d/' % day if day else ''
        hour='%.2d/' % hour if hour else ''
        obs=self.bucket.objects.filter(Prefix='%s%s%s%s%s' % (product,year,day,hour,fileprefix))
        files = [o.key for o in obs]
        if channel: # select target channel
            c='C%.2d' % channel
            files=[f for f in files if c in f]
        return files
    
    def list_df(self,product='ABI-L2-CMIPC',
             year=2020, day='', hour='',channel='',fileprefix=''):
        # Returns df containing metadata
        files = self.list(product,year,day,hour,channel,fileprefix)
        return pd.DataFrame([aws_path_meta(f) for f in files])
    
    def download(self,s,dest):
        # download file s to destination dest.
        # can also pass a row (series) of the dataframe returned by list_df
        if isinstance(s,(str,)):
            print('Downloading s')
            self.bucket.download_file(s,dest)
        else:
            print('Downloading %s/%s/%s/%s/%s' % \
                (s['product'],s.year,s.jd,s.hour,s.filename))
            self.bucket.download_file('%s/%s/%s/%s/%s' % \
                (s['product'],s.year,s.jd,s.hour,s.filename),dest)
    
    def read(self, s):
        # given row of data frame returned by list_df, reads data
        if isinstance(s,(pd.Series,)):
            with tempfile.NamedTemporaryFile() as f:
                self.download(s,f.name)
                data=goes_extract(f.name)
            data.update(s.to_dict())
            return data
        else:
            data=goes_extract(s)
            return data
            
           
def goes_extract(ncfile):
    data={}
    with Dataset(ncfile, 'r') as nc:
        data['key'] = list(nc.variables.keys())[0]
        data['array'] = nc.variables[data['key']][:]
        data['lon0']=nc.variables['goes_imager_projection'].longitude_of_projection_origin
        data['h']=nc.variables['goes_imager_projection'].perspective_point_height
        data['xlim']=data['h']*nc.variables['x_image_bounds'][:].data
        data['ylim']=data['h']*np.flipud(nc.variables['y_image_bounds'][:])
        data['semi_major_axis']=nc.variables['goes_imager_projection'].semi_major_axis
        data['semi_minor_axis']=nc.variables['goes_imager_projection'].semi_minor_axis
    return data
 
    
def goes_proj(data):
    import cartopy.crs as crs
    from cartopy.crs import Globe
    globe=Globe(datum=None,ellipse='WGS84',
                semimajor_axis=data['semi_major_axis'],
                semiminor_axis=data['semi_minor_axis'])  
    proj=crs.Geostationary(central_longitude=data['lon0'],
                            satellite_height=data['h'], globe=globe,
                            sweep_axis='y')
    return proj
    

def quick_plot(data):
    """
    A quick and dirty plot
    """
    import matplotlib.pyplot as plt
    import cartopy.feature as cfeature
    proj = goes_proj(data)
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1, projection=proj)
    ax.set_xlim(data['xlim'])
    ax.set_ylim(data['ylim'])
    ax.add_feature(cfeature.STATES)
    ax.add_feature(cfeature.LAND)
    ax.add_feature(cfeature.OCEAN)
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.BORDERS )
    ax.add_feature(cfeature.LAKES, alpha=0.5)
    ax.add_feature(cfeature.RIVERS)
    img_extent = (*data['xlim'],*data['ylim'])
    return ax.imshow(data['array'],origin='upper',
                   extent=img_extent,transform=proj,interpolation='nearest' )
  
def plot_goes(data):
    import cartopy.crs as crs
    from cartopy.crs import Globe
    globe=Globe(datum=None,ellipse='WGS84',
                semimajor_axis=data['semi_major_axis'],
                semiminor_axis=data['semi_minor_axis'])  
    proj=crs.Geostationary(central_longitude=data['lon0'],
                            satellite_height=data['h'], globe=globe,
                            sweep_axis='y')
    ax = plt.subplot(1,1, projection=proj)
    ax.set_xlim(data['xlim'])
    ax.set_ylim(data['ylim'])
    im = ax.imshow(data['array'],origin='lower',
                   extent=img_extent,transform=proj )
    return ax

def make_globe(proj_dict):
    """
    Crates a cartopy Globe object
    """
    
    a=proj_dict.get('a',None)
    b=proj_dict.get('b',None)
    ellps=proj_dict.get('ellps','WGS84')
    datum=proj_dict.get('datum',None)
    return Globe(datum=datum,ellipse=ellps,semimajor_axis=a,semiminor_axis=b)        

def aws_path_meta(f):
    """
    Extracts various metadata from GOES filename
    e.g. 'ABI-L2-CMIPC/2020/123/03/OR_ABI-L2-CMIPC-M6C01_G16_s20201230301116_e20201230303489_c20201230303587.nc'
    -->
    {'product':'ABI-L2-CMIPC',
     'year':'2020',
     'jd':'123',
     'hour':'03',
     'filename':'OR_ABI-L2-CMIPC-M6C01_G16_s20201230301116_e20201230303489_c20201230303587.nc',
     'start_time':datetime.datetime(2020, 5, 2, 3, 1, 11, 600000),
     'end_time':datetime.datetime(...),
     'creation_time':datetime.datetime(...)    
     }
    """
    parts = f.split('/')
    out={}
    out['product']=parts[0]
    out['year']=parts[1]
    out['jd']=parts[2]
    out['hour']=parts[3]
    out['filename']=parts[4]
    times = re.match(".+_s(\d+)_e(\d+)_c(\d+).nc",f)
    lam = lambda s: datetime.datetime.strptime(s,'%Y%j%H%M%S%f')
    out['start_time']=lam(times.groups()[0])
    out['end_time']=lam(times.groups()[1])
    out['creation_time']=lam(times.groups()[2])
    return out


def goes_products():
    return """
        ABI-L1b-RadC/
        ABI-L1b-RadF/
        ABI-L1b-RadM/
        ABI-L2-ACHAC/
        ABI-L2-ACHAF/
        ABI-L2-ACHAM/
        ABI-L2-ACHTF/
        ABI-L2-ACHTM/
        ABI-L2-ACMC/
        ABI-L2-ACMF/
        ABI-L2-ACMM/
        ABI-L2-ACTPC/
        ABI-L2-ACTPF/
        ABI-L2-ACTPM/
        ABI-L2-ADPC/
        ABI-L2-ADPF/
        ABI-L2-ADPM/
        ABI-L2-AODC/
        ABI-L2-AODF/
        ABI-L2-CMIPC/
        ABI-L2-CMIPF/
        ABI-L2-CMIPM/
        ABI-L2-CODC/
        ABI-L2-CODF/
        ABI-L2-CPSC/
        ABI-L2-CPSF/
        ABI-L2-CPSM/
        ABI-L2-CTPC/
        ABI-L2-CTPF/
        ABI-L2-DMWC/
        ABI-L2-DMWF/
        ABI-L2-DMWM/
        ABI-L2-DSIC/
        ABI-L2-DSIF/
        ABI-L2-DSIM/
        ABI-L2-DSRC/
        ABI-L2-DSRF/
        ABI-L2-DSRM/
        ABI-L2-FDCC/
        ABI-L2-FDCF/
        ABI-L2-LSTC/
        ABI-L2-LSTF/
        ABI-L2-LSTM/
        ABI-L2-LVMPC/
        ABI-L2-LVMPF/
        ABI-L2-LVMPM/
        ABI-L2-LVTPC/
        ABI-L2-LVTPF/
        ABI-L2-LVTPM/
        ABI-L2-MCMIPC/
        ABI-L2-MCMIPF/
        ABI-L2-MCMIPM/
        ABI-L2-RRQPEF/
        ABI-L2-RSRC/
        ABI-L2-RSRF/
        ABI-L2-SSTF/
        ABI-L2-TPWC/
        ABI-L2-TPWF/
        ABI-L2-TPWM/
        ABI-L2-VAAF/
        GLM-L2-LCFA/
        SUVI-L1b-Fe093/
        SUVI-L1b-Fe13/
        SUVI-L1b-Fe131/
        SUVI-L1b-Fe17/
        SUVI-L1b-Fe171/
        SUVI-L1b-Fe195/
        SUVI-L1b-Fe284/
        SUVI-L1b-He303/"""


