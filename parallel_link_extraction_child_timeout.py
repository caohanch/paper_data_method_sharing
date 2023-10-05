#import multiprocessing

#def test(info):
#    print ('TEST', info[0], info[1])

#def run(proc_id):
#    pool = multiprocessing.Pool(processes=8)
#    pool.map(test, [(proc_id, i) for i in range(8)])
#    pool.close()
#    pool.join()
    
    
import json
import re
import pandas as pd
import os
from urlextract import URLExtract
import tldextract
import scispacy
import spacy
import multiprocessing
nlp = spacy.load("en_core_sci_sm", disable=["tagger", "parser", "textcat"])
nlp.add_pipe('sentencizer')
import subprocess
import os
import sys
from pathlib import Path
from doc2json.tex2json.process_tex import process_tex_file
from itertools import repeat
import shutil
import time
from multiprocessing import set_start_method
#extractor = URLExtract()
import signal

class TimeoutException(Exception):   # Custom exception class
    pass

def timeout_handler(signum, frame):   # Custom signal handler
    raise TimeoutException

# Change the behavior of SIGALRM
signal.signal(signal.SIGALRM, timeout_handler)


## detect all footnote label in a string 
# if footnotes exist in a string, return all footnotes detected, and original string 
def detect_footnote(text):
    regex=r'\bFOOTREF[0-9]*\b'
    matches = re.findall(regex, text)
    return text.strip(), matches

## detect all urls in a string 
# if urls exist in a string, return all urls detected, and original string 

def detect_url(text):
    regex=r"\b((?:https?://)?(?:(?:www\.)?(?:[\da-z\.-]+)\.(?:[a-z]{2,6})|(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)|(?:(?:[0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,7}:|(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,5}(?::[0-9a-fA-F]{1,4}){1,2}|(?:[0-9a-fA-F]{1,4}:){1,4}(?::[0-9a-fA-F]{1,4}){1,3}|(?:[0-9a-fA-F]{1,4}:){1,3}(?::[0-9a-fA-F]{1,4}){1,4}|(?:[0-9a-fA-F]{1,4}:){1,2}(?::[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:(?:(?::[0-9a-fA-F]{1,4}){1,6})|:(?:(?::[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(?::[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(?:ffff(?::0{1,4}){0,1}:){0,1}(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])|(?:[0-9a-fA-F]{1,4}:){1,4}:(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])))(?::[0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])?(?:/[\w\.-]*)*/?)\b"
    matches = re.findall(regex, text)
    #extractor = URLExtract()
    #matches = extractor.find_urls(text)
    matches = [x for x in matches if x.startswith('www') or x.startswith('http') or x.startswith('https')]
    #if len(matches)==0:
    #    return False, text.strip(), matches
    #else:
    return text.strip(), matches


# loop through all texts (abstract + ) in a paper, and detect whether there are urls and footnotes
# to make tracing back to original file easier, use SEC_INDEX, AND POS_INDEX indicating the positsions of the text_dict
def get_url_info(paper_tex):
    PAPER_ID = []
    INDEX = [] 
    SEC_INDEX = []  
    IsFOOTNOTE = []
    FOOTNOTE = []
    CONTEXT = []
    CONTEXT_paragraph = []
    URLS = []
    SECTION = []
    paper_id = paper_tex['paper_id'].strip()
    
    for latex_tag in ['abstract', 'body_text']:
        id_count = -1
        for text_dict in paper_tex['latex_parse'][latex_tag]:
            id_count += 1
            text = text_dict['text']
            section = text_dict['section']
            
            _, matches_url = detect_url(text)
            _, matches_footnote = detect_footnote(text)
            if len(matches_url)>0 or len(matches_footnote)>0: # first check whether the paragraph contains urls or footnotes before breaking into sentences
                sentences = [i for i in nlp(text).sents] # break into sentences
                for sent_id in range(len(sentences)):
                    try:
                        sent = sentences[sent_id]
                        sent1, matches_url = detect_url(str(sent))
                        if len(matches_url)>0: # if url in text has been detected, save it
                            for m in matches_url:
                                PAPER_ID.append(paper_id)
                                INDEX.append(id_count)
                                SEC_INDEX.append(latex_tag)
                                IsFOOTNOTE.append(0)
                                FOOTNOTE.append('NA')
                                CONTEXT.append(sent1)
                                CONTEXT_paragraph.append(text)
                                URLS.append(m)
                                SECTION.append(section)
                        sent1, matches_footnote = detect_footnote(str(sent))
                        if len(matches_footnote)>0: # if footnote has been detected, check whether it contains url, if so save it
                            for m in matches_footnote:
                                footnote, urls = detect_url(paper_tex['latex_parse']['ref_entries'][m]['text'])
                                if len(urls)>0:
                                    for url in urls:
                                        PAPER_ID.append(paper_id)
                                        INDEX.append(id_count)
                                        SEC_INDEX.append(latex_tag)
                                        IsFOOTNOTE.append(1)
                                        FOOTNOTE.append(footnote)
                                        if sent1.startswith('FOOTREF'):  # sometimes the sentenciser has issues and parse out only footnote label, if so correct it with prior sentence as context
                                            CONTEXT.append(str(sentences[sent_id-1]) + ' '+ sent1)
                                        else:
                                            CONTEXT.append(sent1)
                                        CONTEXT_paragraph.append(text)
                                        URLS.append(url)  
                                        SECTION.append(section)
                    except:
                        pass
    df_temp = pd.DataFrame({'PAPER_ID':PAPER_ID, 'SEC_INDEX':SEC_INDEX, 'POS_INDEX':INDEX, 'SECTION':SECTION, 'IsFOOTNOTE':IsFOOTNOTE, 'FOOTNOTE':FOOTNOTE, 'CONTEXT':CONTEXT, 'CONTEXT_PAR':CONTEXT_paragraph, 'URLS':URLS})
    if len(df_temp)>0:
        domain_temp = df_temp['URLS'].apply(tldextract.extract)
        df_temp['subdomain'] = domain_temp.str[0]
        df_temp['domain'] = domain_temp.str[1]
        df_temp['suffix'] = domain_temp.str[2]
    return df_temp

        
def process_zipped_paper(file0, tar_file):
    

    DirRawFile = '../../arxiv_data/' + tar_file + '/' # directory of the raw zipped full text
    DirJsonFile = '../../arxiv_data/' + tar_file + '_json_v2/' # parsed json through doc2json
    
    DirDF = '../../detected_url_arxiv/' + tar_file # detected url file for each paper
    
    file = '.'.join(file0.split('.')[:-1])
    TIMEOUT = 30
    signal.alarm(TIMEOUT)
    try:
        print ('doc2json ' + file + ' started')
        process_tex_file(DirRawFile + file + '.gz','temp_dir/',DirJsonFile, keep_flag = True)
        print ('doc2json ' + file + ' done!!')
        #process_tex_file(DirRawFile + file + '.gz','temp_dir/',DirJsonFile, keep_flag = False)
            #process_tex_file('/net/nfs2.s2-research/hanchengc/paper-artifact/code/s2orc/tests/latex/2206.11022.gz','temp_dir/','output_dir/', keep_flag = True)

            # then run link detection
        paper_tex = json.load(open(DirJsonFile + file + '.json')) 
        
        print ('get_url_info ' + file + ' started')
        df = get_url_info(paper_tex)
        print ('get_url_info ' + file + ' done!!')

        print ('save result from ' + file)
        df.to_csv(DirDF  + 'url_' + file + '.tsv', sep = '\t' , index = False)
    except:
        print ('error ' + file)



def parallel_link_extract(tar_file):

    #Bulk = '2101'
    set_start_method("spawn", force=True)
    TIMEOUT = 30

    DirRawFile = '../../arxiv_data/' + tar_file + '/' # directory of the raw zipped full text
    DirJsonFile = '../../arxiv_data/' + tar_file + '_json/' # parsed json through doc2json
    
    DirDF = '../../detected_url_arxiv/' + tar_file # detected url file for each paper
    
    if not os.path.exists(DirDF):
        os.makedirs(DirDF)
    #NUM_PROCESSES = multiprocessing.cpu_count()
    NUM_PROCESSES = 8

    ListofFiles = os.listdir(DirRawFile)
    with multiprocessing.Pool(processes=NUM_PROCESSES) as p:
        #p.daemon = True
        p.starmap(process_zipped_paper, list(zip(ListofFiles, repeat(tar_file))))
        #p.close()
        #p.join(TIMEOUT)
        #if p.is_alive():
        #    print ('timeout, kill session...')
        #    p.terminate()
        #    p.join()
    #pool = multiprocessing.Pool(processes=NUM_PROCESSES)
    
    #pool.apply_async(process_zipped_paper, args=()) 
    print ('tar file '+ tar_file + ' multiprocessing finished... joining')  

    
    p.close()
    p.join()

    #p.join(TIMEOUT)
    

    
    # remove the untared file and directory
    shutil.rmtree(DirRawFile, ignore_errors=True)
    
    print('done.')
