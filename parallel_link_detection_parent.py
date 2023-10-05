import multiprocessing
import multiprocessing.pool
import parallel_link_extraction_child_timeout
import os
import tqdm
import boto3
from bs4 import BeautifulSoup
import tarfile
import gzip
import shutil
import time

from multiprocessing import set_start_method


    
ARXIV_BUCKET = 'arxiv'
OUTPUT_DIR = '../../arxiv_data/'
aws_attribs = {'RequestPayer': 'requester'}
Tar_done_file = '../../arxiv_manifest_file/TarDone.txt'
year_range = ['11']  # range of years to execute on
#year_range = ['19']
#year_range = ['1902']   
    
def batch_open_arxiv_dump(arxiv_dump_file: str, raw_output_dir: str):
    """
    Untar and unzip arxiv dump file locally
    :param arxiv_dump_file:
    :param raw_output_dir:
    :return:
    """
    # untar arxiv dump locally
    with tarfile.open(arxiv_dump_file) as tar:
        for member in tar.getmembers():
            if member.isreg():  # skip if the TarInfo is not files
                member.name = os.path.basename(member.name)  # remove the path by reset it
                tar.extract(member, raw_output_dir)
                
        
#s3_arxiv_tar = 'src/arXiv_src_1805_005.tar'
#ListofFiles = ['src/arXiv_src_1805_006.tar','src/arXiv_src_1805_007.tar']

def process_tar(s3_arxiv_tar):
    f_done = open(Tar_done_file, "r")
    if s3_arxiv_tar in f_done.read():
        print ('skip as have already been processed: ' + s3_arxiv_tar)
        f_done.close()
        pass
    else:
        # check whether s3_arxiv_tar has been processed or not
        _, fname = s3_arxiv_tar.split('/')
        print ('downloading ' + fname)
        #s3_arxiv_tar = 'src/arXiv_src_1401_001.tar'
        local_arxiv_tar = os.path.join(OUTPUT_DIR, fname)
        bucket.download_file(s3_arxiv_tar, local_arxiv_tar, aws_attribs)
        print ('downloading ' + fname + ' completed')

        # need to save the folder name as a column so as to get publication year
        print ('untarring ' + fname )
        batch_open_arxiv_dump(local_arxiv_tar, os.path.join(OUTPUT_DIR, fname.split('.')[0]))
        FileNum = len(os.listdir(OUTPUT_DIR + fname.split('.')[0]))
        print ('untarring ' + fname + ' completed')

        # remove the tar file 
        print ('removing ' + fname )
        os.remove(local_arxiv_tar)
        print ('removing ' + fname + ' completed')

        # call the url extraction module 
        print ('start url detection for ' + fname)
        parallel_link_extraction_child_timeout.parallel_link_extract(fname.split('.')[0])
        print ('done url detection ' + fname)
        f_done.close()
        f_done = open(Tar_done_file, "a")
        f_done.write(s3_arxiv_tar)
        f_done.write('\t')
        f_done.write(str(FileNum))
        f_done.write('\n')
        f_done.close()
        
#def work():
#    with multiprocessing.pool.ThreadPool(processes=NUM_PROCESSES) as p:
#        p.map(process_tar, ListofFiles)
#    p.close()
#    p.join()

#    print('done.')

    
if __name__ == '__main__':    
    # create s3 resource
    #set_start_method("spawn")
    NUM_PROCESSES = 8
    s3 = boto3.resource('s3',
             aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
             aws_secret_access_key= os.getenv("AWS_SECRET_KEY"))
    bucket = s3.Bucket(ARXIV_BUCKET)

    # download manifest file if it is not already downloaded
    manifest_file = os.path.join('src', 'arXiv_src_manifest.xml')
    local_manifest_file = os.path.join('arxiv_manifest_file', 'arXiv_src_manifest.xml')

    if os.path.isfile(local_manifest_file) == False:
        bucket.download_file(manifest_file, local_manifest_file, aws_attribs)

    # read manifest, and prepare list of files to execute on

    ListofFiles = []
    with open(local_manifest_file, 'r') as f:
        xml = f.read()
    soup = BeautifulSoup(xml, 'xml')
    for prefix in tqdm.tqdm(soup.find_all('filename')):
        _, fname = prefix.text.split('/')
        s3_arxiv_tar = prefix.text
        if s3_arxiv_tar.split('_')[2][:2] in year_range:
            ListofFiles.append(prefix.text)
    #process_tar(s3_arxiv_tar)
    
    for file in ListofFiles:
        process_tar(file)
        #shutil.rmtree('temp_dir/', ignore_errors=True)