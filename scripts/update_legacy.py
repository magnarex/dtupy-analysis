from muTel.dqm.classes.MuData import MuData
import pathlib
import pyarrow as pa
import pyarrow.parquet as pq
import argparse

def update_datafile(path, run = 'NA', date = 'NA', filename = None):
    path = pathlib.Path(path)
    if not filename:
        file_path = path.with_suffix('.parquet')
    elif isinstance(filename, str):
        file_path = path.with_name(filename).with_suffix('.parquet')
    


    custom_metadata = {
        b'run'  : bytes(str(run),'utf-8'),
        b'date' : bytes(str(date),'utf-8')
    }

    df = MuData.from_jrun(run).df
    table = pa.Table.from_pandas(df)
    fixed_table = table.replace_schema_metadata({**custom_metadata,**table.schema.metadata})
    pqwriter = pq.ParquetWriter(file_path, fixed_table.schema)
    pqwriter.write_table(fixed_table)
    pqwriter.close()



if __name__ == '__main__':
    # TODO: argparse
    update_datafile('/afs/ciemat.es/user/m/martialc/public/muTel_v4/muTel/data/MuonData_588.parquet',
            run = 588,
            filename= 'run_588'
        )
