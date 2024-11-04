import sys
import glob
import pandas as pd
import os
import re
import json
#checking
def get_column_names(file,branch):
    return (list(map(lambda x:x['column_name'],file[branch])))

def read_csv(name,schema):
    file_details=re.split('[/\\\]',name)
    ds_name=file_details[-2]
    column_name=get_column_names(schema,ds_name)
    df=pd.read_csv(name,names = column_name)
    return df

def read_json(name):
    file_details=re.split('[/\\\]',name)
    data = []
    with open(name) as f:
        for line in f:
            try:
                data.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
    df = pd.DataFrame(data)
    return df
    
def convert_json(df,base,ds_name,end_file):
    json_file_path=f'{base}/{ds_name}/{end_file}.json'
    os.makedirs(f'{base}/{ds_name}',exist_ok=True)
    df.to_json(json_file_path,
               orient='records',
               lines=True
    )

def convert_csv(df,base,ds_name,end_file):
    csv_file_path=f'{base}/{ds_name}/{end_file}'
    os.makedirs(f'{base}/{ds_name}',exist_ok=True)
    df.to_csv(csv_file_path,header=False)

def file_converter(src_base_dir,tar_base_dir,ds_name,convert):
    schema=json.load(open(f'schemas.json'))
    files=glob.glob(f'{src_base_dir}/{ds_name}/part-*')

    if len(files)==0:
        raise NameError(f'No files found for {ds_name}')
    for file in files:
        if convert==0:
            df=read_csv(file,schema)
            file_name=re.split('[/\\\]',file)[-1]
            convert_json(df,tar_base_dir,ds_name,file_name)
        else:
            df=read_json(file)
            file_name=(re.split('[/\\\]',file)[-1])[:-5]
            print(file_name)
            convert_csv(df,tar_base_dir,ds_name,file_name)

def process_files(convert,ds_name=None):
    src_base_dir=os.getenv('SRC_DIR')
    tar_base_dir=os.getenv('TAR_DIR')
    schema=json.load(open(f'schemas.json'))
    if ds_name is None:
        ds_name=schema.keys()
    for ds_names in ds_name:
        try:
            print(f'processing {ds_names}')
            if(convert==0):
                file_converter(src_base_dir,tar_base_dir,ds_names,convert)
            else:
                file_converter(tar_base_dir,src_base_dir,ds_names,convert)
        except NameError as ne:
            print(ne)
            print(f'Error processing {ds_names}')
            pass


if __name__ == '__main__':
    convert=int(sys.argv[-1])
    print(convert)
    if len(sys.argv)==3:
        ds_name=json.loads(sys.argv[1])
        process_files(convert,ds_name)
    else:
        process_files(convert)

        