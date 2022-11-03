import pandas as pd
import requests
import multiprocessing
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime
import time


Uniprot_API='https://rest.uniprot.org/uniprotkb/'

def UniProt_data_extraction(UniProt_ID):
    gene_names_=''
    seq_len=''
    mol_wt=''
    KEGG_IDs=[]
    family_class=''
    PDB_IDs=[]

#   Code snipt for retrying X times in case of connection error with a delay of X ms

    session = requests.Session()
    # retry = Retry(connect=3, backoff_factor=0.5)
    retry = Retry(total=3,connect=4, read=4,backoff_factor=0.5, allowed_methods=None, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    response = session.get(Uniprot_API+UniProt_ID)  # request to uniprot
    uniprot_API_json=response.json()

    try: 
        if 'genes' in uniprot_API_json:   # all these comming if conditions on returned datastructure inside 'uniprot_API_json'
            gene_names_=uniprot_API_json['genes'][0]['geneName']['value']
        if 'sequence' in uniprot_API_json:
            seq_len=uniprot_API_json['sequence']['length']
            mol_wt=uniprot_API_json['sequence']['molWeight']
        if 'uniProtKBCrossReferences' in  uniprot_API_json:  # some of these might be missing, so i put if condition on header
            for k in range (len(uniprot_API_json['uniProtKBCrossReferences'])):  # find KEGG if any
                if uniprot_API_json['uniProtKBCrossReferences'][k]['database']=='KEGG':
                    KEGG_IDs.append(uniprot_API_json['uniProtKBCrossReferences'][k]['id'])

            for k in range (len(uniprot_API_json['uniProtKBCrossReferences'])): 
                if uniprot_API_json['uniProtKBCrossReferences'][k]['database']=='Pfam':# some of these might be missing , so i put if condition on header
                    family_class=uniprot_API_json['uniProtKBCrossReferences'][k]['properties'][0]['value']
                    break

            for k in range (len(uniprot_API_json['uniProtKBCrossReferences'])): 
                if uniprot_API_json['uniProtKBCrossReferences'][k]['database']=='PDB': # some of these might be missing, so i put if condition on header
                    PDB_IDs.append(uniprot_API_json['uniProtKBCrossReferences'][k]['id'])
    except:
        print("Some values for columns are missing")
    
    Combined_list=[UniProt_ID,gene_names_,seq_len,mol_wt,family_class,KEGG_IDs,PDB_IDs]
    return Combined_list


# To do it for 100
# You will need to write a script to make it parallel for 100 entries at a time.
# We can add a file len checker to avoid the maximum tries error

def multiprocessing_requests_n_save(filename):
    output_file = pd.DataFrame(columns=['Uniprot_id','Gene','Sequence_length','Mol_weight_Delton','Family_class','KEGG_Pathways','Associated_PDBs'])

    Uniprot_IDS=pd.read_csv(filename,sep=";")
    lenth_check= len(Uniprot_IDS)

    if lenth_check>=4000:
        half_ids = len(Uniprot_IDS)//2
        print(f"File is large so we divided it into 2 halves of {half_ids}")
        for i in range(0, half_ids//100 + 1):
            # doing for the first 100 ids
            ids = list(Uniprot_IDS.iloc[i*100:i*100+100,0])
            print(ids)
            if (i*100+100)%2000==0:
                time.sleep(0.05)
                print("waiting... for 0.05 sec")
            # creating  a pool of processes
            with multiprocessing.Pool(processes=100) as pool:
                print(f"Sending {len(ids)} requests...")
                # map the function on our ids arary
                Uniprot_data_list = pool.map(UniProt_data_extraction,ids)
            print(f"Accessed {len(ids)} done !")
            print(f"Our Uniprot_data_list which we will write to our DataFrame")
            # print(Uniprot_data_list)
            for j in range(len(ids)):
                output_file.loc[len(output_file)] = Uniprot_data_list[j]
            
            print(f"Our Uniprot_data_list has been written to our DataFrame")
            print(f"Total {i*100+100}  Entries written\n\n")
        print("half done\n")
        time.sleep(0.05)
        print("Sleep for 0.05")
        ## For other half
        for i in range(half_ids//100+1, len(Uniprot_IDS)//100 + 1):
            # doing for the first 100 ids
            ids = list(Uniprot_IDS.iloc[i*100:i*100+100,0])
            print(ids)
            if (i*100+100)%2000==0:
                time.sleep(0.05)
                print("waiting... for 0.05 sec")
            # creating  a pool of processes
            with multiprocessing.Pool(processes=100) as pool:
                print(f"Sending {len(ids)} requests...")
                # map the function on our ids arary
                Uniprot_data_list = pool.map(UniProt_data_extraction,ids)
            print(f"Accessed {len(ids)} done !")
            print(f"Our Uniprot_data_list which we will write to our DataFrame")
            # print(Uniprot_data_list)
            for j in range(len(ids)):
                output_file.loc[len(output_file)] = Uniprot_data_list[j]
            
            print(f"Our Uniprot_data_list has been written to our DataFrame")
            print(f"Total {i*100+100}  Entries written\n\n")
        print("All done")
    else:
        print(f"File len is {len(Uniprot_IDS)}, so we did not divide it...")
        for i in range(0, len(Uniprot_IDS)//100 + 1):
            # doing for the first 100 ids
            ids = list(Uniprot_IDS.iloc[i*100:i*100+100,0])
            print(ids)
            if (i*100+100)%2000==0:
                time.sleep(0.05)
                print("waiting... for 0.05 sec")
            # creating  a pool of processes
            with multiprocessing.Pool(processes=100) as pool:
                print(f"Sending {len(ids)} requests...")
                # map the function on our ids arary
                Uniprot_data_list = pool.map(UniProt_data_extraction,ids)
            print(f"Accessed {len(ids)} done !")
            print(f"Our Uniprot_data_list which we will write to our DataFrame")
            # print(Uniprot_data_list)
            for j in range(len(ids)):
                output_file.loc[len(output_file)] = Uniprot_data_list[j]
            
            print(f"Our Uniprot_data_list has been written to our DataFrame")
            print(f"Total {i*100+100}  Entries written\n\n")
           
    # # Testing for just 200 ids
    # for i in range(0,1):
    #     # doing for the first 100 ids
    #     ids = list(Uniprot_IDS.iloc[i*10:i*10+10,0])
    #     print(ids)
    #     # creating  a pool of processes
    #     if (i*10+10)%10==0:
    #         time.sleep(0.05)
    #         print("waiting... for 0.05 sec")
    #     with multiprocessing.Pool(processes=100) as pool:
    #         print(f"Sending {len(ids)} requests...")
    #         # map the function on our ids arary
    #         Uniprot_data_list = pool.map(UniProt_data_extraction,ids)
    #     print(f"Accessed {len(ids)} done !")
    #     print(f"Our Uniprot_data_list which we will write to our DataFrame")
    #     # print(Uniprot_data_list)
    #     for j in range(len(ids)):
    #         output_file.loc[len(output_file)] = Uniprot_data_list[j]
    #     print(f"Our Uniprot_data_list has been written to our DataFrame")
    #     print(f"Total {i*10+10}  Entries written\n\n")

    # Saving file to csv format which we will later download 
    # file_name = f'output/{datetime.now():%Y-%m-%d_%H-%M-%S}.csv'
    # output_file.to_csv(file_name, encoding='utf-8' )
    # return file_name
    return output_file.to_csv(f'output/{datetime.now():%Y-%m-%d_%H-%M-%S}.csv', encoding='utf-8' )