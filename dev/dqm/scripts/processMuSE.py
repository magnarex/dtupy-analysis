import pandas as pd
from time import time
import concurrent.futures
from threading import Lock
import os
import shutil
import pathlib

import pyarrow as pa
import pyarrow.parquet as pq

import muTel.utils.meta as meta
from muTel.utils.units import Time
from muTel.dqm import MuEvent, MuData


type_dict = meta.data_type_dict | {'MuSEId'    : 'category'}
schema = pa.Schema.from_pandas(pd.DataFrame(columns=type_dict.keys()).astype(type_dict))



def process_task(grp, i = 0, run = None, date = None, t0 = 0):

    return MuEvent.from_data(grp,run = run, date = date, do_muses=True)

def io_task(table, pqwriter : pq.ParquetWriter):
    pqwriter.write_table(table)
    pqwriter.close()
        


class MuSEPool(object):
    type_dict = meta.data_type_dict | {'MuSEId'    : 'category'}
    def __init__(
            self,
            log_step = 1000,
            output_dir = None,
            temp_dir = None,
            muse_per_dump = 10000,
            max_workers = 4,
            overwrite = False,
            do_cleanup = False
        ):

        # Comprobamos que exista el directorio temporal
        if temp_dir is None:
            self.temp_dir = None
        elif isinstance(temp_dir, str | pathlib.Path):
            self.temp_dir = temp_dir
        else:
            raise TypeError(f'"temp_dir" type is not valid. Input type was {type(temp_dir)}; str or pathlib.Path were expected.')
        
        
        # Comprobamos que exista el directorio de salida
        if output_dir is None:
            self.output_dir = pathlib.Path(meta.parent) / pathlib.Path('data/muses')
            print(f'No output_dir selected, reverting to default: {self.output_dir}')
        elif isinstance(output_dir, str | pathlib.Path):
            self.output_dir = output_dir
        else:
            raise TypeError(f'"output_dir" type is not valid. Input type was {type(temp_dir)}; str or pathlib.Path were expected.')
        # finally
        try:
            os.mkdir(self.output_dir)
        except FileExistsError:
            pass
        
        
        # Guardamos los argumentos
        self.overwrite = overwrite
        self.log_step = log_step
        self.muse_per_dump = muse_per_dump
        self.max_workers = max_workers
        self.do_cleanup = do_cleanup
        
        # Iniciamos la pool
        self.pool = concurrent.futures.ProcessPoolExecutor(max_workers = max_workers)
        # self.dump_pool = concurrent.futures.ThreadPoolExecutor()
        self.dump_pool = self.pool
        
        # Iniciamos los atributos necesarios
        self.muse_list = []
        self.pending_pairing_jobs = {}
        self.pending_dumping_jobs = []
        self.nsubmited=0
        self.nfinished = 0
        self.ndumped = 0
        self._run = None
        self._date = None
        self.output_name = None
        self.chunk_alias = None
        
        # Iniciamos los temporizadores a 0 para que no den errores
        self.t0 = 0
        self.t_submitted = 0
                
    def run(self, mudata : MuData, output_name):
        
        self.output_name = output_name
        
        # Intentamos crear el directorio temporal
        if self.temp_dir is None: self.temp_dir = pathlib.Path(meta.parent) / pathlib.Path(f'tmp/muse/{output_name}')
        try:
            os.mkdir(self.temp_dir)
        except FileExistsError:
            print('Temporal files for this output name already exist!')
            if self.overwrite:
                print('Overwriting temporal files...')
                shutil.rmtree(self.temp_dir)
                os.mkdir(self.temp_dir)
            else:
                print('"overwrite" is set to False. Aborting...')
                raise RuntimeError('Please, set "overwrite" to True or change the output name to proceed.')
            
        # Extraemos metadatos de MuData
        self._run = mudata.run
        self._date = mudata.date
        
        # Preparamos los datos para enviarlos a los jobs
        event_gb = list(mudata.df.groupby('EventNr'))
        
        # Metemos todo en un bloque try para handle los KeyboardInterrupt
        try:
            # Iniciamos el temporizador y comenzamos a enviar los jobs
            self.t0 = time()
            [self._submit_pairing_job(*event_gb.pop(0)) for i in range(len(event_gb))]

            # Todos los trabajos han sido enviados a la pool con éxito.
            self.t_submitted = time()
            print(f'({str(Time(time()-self.t0)):>10})  All tasks have been issued!', flush=True)
            
        except KeyboardInterrupt:
            print('Interrumpiendo los procesos...')
            self.pool.shutdown(wait=False,cancel_futures=True)
            raise KeyboardInterrupt
        
        try:
            self.chunk_alias = self._chunkalias_generator()
            [self._as_completed(future) for future in concurrent.futures.as_completed(self.pending_pairing_jobs)]
        
            concurrent.futures.wait(self.pending_pairing_jobs)
            concurrent.futures.wait(self.pending_dumping_jobs)
            if len(self.muse_list) > 0: self._submit_dumping_job()
        except KeyboardInterrupt:
            print('Interrumpiendo los procesos...')
            self.pool.shutdown(wait=False,cancel_futures=True)
            self.cleanup()
        
        # Esperar a los procesos y después liberar los recursos.
        self.pool.shutdown()
        print(f'({str(Time(time()-self.t0)):>10})  Success! All tasks have been finished! :)', flush=True)
        
        # Leemos los archivos temporales y los volcamos en el output.
        self.collect_muse()
        
        
        
    def cleanup(self):
        print('Limpiando los archivos temporales...')
        shutil.rmtree(self.temp_dir)

    def collect_muse(self, output_name = None):
        # Si se indica un output_name, override el que se había dado previamente en run.
        if not self.output_name and not output_name: raise ValueError('No output name was assigned.')
        elif output_name:
            pass
        else:
            output_name = self.output_name
        
        print(f'Collecting muse from {self.temp_dir}...')
        
        # Iniciamos las variables
        pqwriter = None
        temp_files = [temp_file for temp_file in self.temp_dir.glob('**/*.parquet') if temp_file.is_file()]
        
        for temp_file in temp_files:
            print(f'Collecting {temp_file.stem}')
            
            table = pq.read_table(temp_file)
            if not pqwriter: pqwriter = pq.ParquetWriter(self.output_dir / pathlib.Path(f'{output_name}.parquet'), table.schema)
            pqwriter.write_table(table)
        
        if pqwriter: pqwriter.close()
        print(f'All muse collected!')
        
        
        if self.do_cleanup: self.cleanup()
        return

        
        
    #=====================================================================
    # OTHER METHODS
    #=====================================================================
      
    def _as_completed(self, future):
        if self.nfinished % self.log_step == 0:
            print(f'({str(Time(time()-self.t0)):>10})[{str(Time(time()-self.t_submitted)):<10}]  Terminado análisis del evento #{self.nfinished}.',flush=True)
        
        self.muse_list += [muse.full_data for muse in future.result().all_muses]
        
        # if len(self.muse_list) > self.muse_per_dump: self._submit_dumping_job()
        if len(self.muse_list) > self.muse_per_dump: self.dump_muses()
        
        self.nfinished += 1
        return
    
    #=====================================================================
    # PAIRING METHODS
    #=====================================================================
    @staticmethod
    def _process_pairing_job(grp, run, date):
        '''
        Method that will be pickled by the pool, needs to be static.
        '''
        return MuEvent.from_data(grp, run = run, date = date, do_muses=True)
    
    def _submit_pairing_job(self,eventnr,df):
        if self.nsubmited % self.log_step == 0:
            print(f'({str(Time(time()-self.t0)):>10})  Iniciando análisis del evento #{self.nsubmited}...', flush=True)
        
        job = self.pool.submit(
            self._process_pairing_job,
            df,
            run = self._run,
            date = self._date
        )
        self.pending_pairing_jobs[job] = eventnr
        self.nsubmited += 1
        return

    
    #=====================================================================
    # I/O METHODS
    #=====================================================================

    @staticmethod
    def _process_dumping_job(muse_list, path):
        table = pa.Table.from_pandas(pd.concat(muse_list).astype(MuSEPool.type_dict))
        pqwriter = pq.ParquetWriter(path, table.schema)
        pqwriter.write_table(table)
        pqwriter.close()
        return
    
    def _submit_dumping_job(self):
        print(f'({str(Time(time()-self.t0)):>10})[{str(Time(time()-self.t_submitted)):<10}]  Escribiendo {len(self.muse_list)} MuSEs...',flush=True)
                
        
        # Enviamos el trabajo y lo guardamos.
        job = self.dump_pool.submit(
            self._process_dumping_job,
            muse_list = self.muse_list,
            path = self.temp_dir / pathlib.Path(f'{self.get_chunk_alias()}.parquet')
        )
        self.pending_dumping_jobs.append(job)
        
        # Reiniciamos la lista de MuSEs y actualizamos el valor de ndumped.
        self.ndumped += 1
        self.muse_list = []
        
        return
    
    def dump_muses(self):
        print(f'({str(Time(time()-self.t0)):>10})[{str(Time(time()-self.t_submitted)):<10}]  Escribiendo {len(self.muse_list)} MuSEs...',flush=True)
        
        # Creamos la tabla y el escritor.
        table = pa.Table.from_pandas(pd.concat(self.muse_list).astype(self.type_dict))
        pqwriter = pq.ParquetWriter(self.temp_dir / pathlib.Path(f'{self.get_chunk_alias()}.parquet'), table.schema)

        pqwriter.write_table(table)
        pqwriter.close()

        # Reiniciamos la lista de MuSEs y actualizamos el valor de ndumped.
        self.ndumped += 1
        self.muse_list = []
        
        return
    
    @staticmethod
    def _chunkalias_generator():
        nchunk = 0
        while True:
            yield f'chunk_{str(nchunk).zfill(2)}'
            nchunk += 1

    def get_chunk_alias(self):
        return next(self.chunk_alias)

if __name__ == '__main__':
    
    mudata = MuData.from_datadir('run_588')
    
    MuSEPool(
            output_dir = '/afs/ciemat.es/user/m/martialc/public/muTel_v4/muTel/dev/dqm/muses',
            max_workers=4,
            muse_per_dump = 10000,
            log_step=1000,
            overwrite=True
        ).run(mudata,'run_588_test')
    
    
    # Usando de I/O el thread principal (con GIL) y 6 workers: 22m 37.89s para ~50k eventos