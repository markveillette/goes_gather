
import os
import numpy as np
import matplotlib.pyplot as plt
import itertools
import datetime
from aws_utils import GoesAwsBucket, goes_proj, quick_plot

gather_config = {
    #'ACTP':{'product':'ABI-L2-ACTPF','scale':1,'offset':0}, # Cloud phase
    #'ACM':{'product':'ABI-L2-ACMF','scale':1,'offset':0},  # Clear sky mask
    #'CODC':{'product':'ABI-L2-CODC','scale':100,'offset':0},  # Clear sky mask
    'C01':{'product':'ABI-L2-CMIPF','channel':1,'scale':1e4,'offset':0},
    'C02':{'product':'ABI-L2-CMIPF','channel':2,'scale':1e4,'offset':0},
    'C03':{'product':'ABI-L2-CMIPF','channel':3,'scale':1e4,'offset':0},
    'C04':{'product':'ABI-L2-CMIPF','channel':4,'scale':1e4,'offset':0},
    'C05':{'product':'ABI-L2-CMIPF','channel':5,'scale':1e4,'offset':0},
    'C06':{'product':'ABI-L2-CMIPF','channel':6,'scale':1e4,'offset':0},
    'C07':{'product':'ABI-L2-CMIPF','channel':7,'scale':1e2,'offset':273.15},
    'C08':{'product':'ABI-L2-CMIPF','channel':8,'scale':1e2,'offset':273.15},
    'C09':{'product':'ABI-L2-CMIPF','channel':9,'scale':1e2,'offset':273.15},
    'C10':{'product':'ABI-L2-CMIPF','channel':10,'scale':1e2,'offset':273.15},
    'C11':{'product':'ABI-L2-CMIPF','channel':11,'scale':1e2,'offset':273.15},
    'C12':{'product':'ABI-L2-CMIPF','channel':12,'scale':1e2,'offset':273.15},
    'C13':{'product':'ABI-L2-CMIPF','channel':13,'scale':1e2,'offset':273.15},
    'C14':{'product':'ABI-L2-CMIPF','channel':14,'scale':1e2,'offset':273.15},
    'C15':{'product':'ABI-L2-CMIPF','channel':15,'scale':1e2,'offset':273.15},
    'C16':{'product':'ABI-L2-CMIPF','channel':16,'scale':1e2,'offset':273.15},
}

output_file = 'G16_all_channels.h5'

def main():
    aws = GoesAwsBucket('noaa-goes16')
    import h5py
    # BE CAREFUL!!  This will overwrite the file if run again
    if os.path.exists(output_file):
        raise ValueError('%s exists, please remove' % output_file)

    # gather first batch
    with h5py.File(output_file, 'w') as hf:
        patches=get_batch(gather_config)
        for k,g in gather_config.items():
            hf.create_dataset(k,data=patches[k],maxshape=(None,patches[k].shape[1],patches[k].shape[2]))

    n_samples = 499
    with h5py.File(output_file, 'a') as hf:
        for i in range(n_samples):
            print('----------------------')
            print('PROCESSING BATCH %d/%d' % (i,n_samples))
            print('----------------------')
            patches=get_batch(gather_config)
            for k,g in gather_config.items():
                hf[k].resize((hf[k].shape[0] + patches[k].shape[0]), axis = 0)
                hf[k][-patches[k].shape[0]:]  = patches[k]
    

def random_time(start_date=datetime.datetime(2019,12,1), # ABI-L2-ACMF only avail after ~dec 2019?
                end_date=datetime.datetime(2020,11,1)):
    d = (end_date-start_date)
    return (start_date + datetime.timedelta(seconds=np.random.randint(int(d.total_seconds()))))
    

def extract_patches( data, scale=100, offset=273.15 ):
    """
    Given full disk GOES image, extracts patches of size 256x256 from the interior of the image.
    """
    i=range(900,4400,256) # bottom corners of the patches
    sz = 256 # length of patches in pixels
    arr = data['array'].data
    corners = list(itertools.product(i,i)) 
    N = len(corners)
    patches = np.zeros((N,sz,sz),dtype=np.int16)
    for k,(x0,y0) in enumerate(corners):
        patches[k,:,:] = np.int16(scale*(arr[y0:y0+sz,x0:x0+sz]-offset))
    return patches



def get_batch(gather_config,n_subsample=50):
    while True:
        time = random_time()
        patches_dict={}
        for k,g in gather_config.items():
            data = aws.get(g['product'],time,g.get('channel',''))
            if data is None:
                print('%s not found from time %s!' % (g['product'],time))
                stop=False
                break
            else:
                patches_dict[k]=extract_patches(data,scale=g.get('scale',100))
                stop=True
        if stop:
            break
    mask = np.random.choice( patches_dict[k].shape[0], n_subsample)
    for k in patches_dict:
        patches_dict[k] = patches_dict[k][mask,:,:]
    return patches_dict




if __name__=="__main__":
    main()
