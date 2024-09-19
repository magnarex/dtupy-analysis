import concurrent.futures
import pandas as pd

from muTel.dqm.classes.MuEvent import MuEvent
from muTel.dqm.classes.MuData import MuData



def data_to_events(data : MuData):
    event_df = data.df.reset_index()
    event_gb = event_df.groupby('EventNr')


    event_dict = {}
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = {executor.submit(
                MuEvent.from_df,
                event_df.iloc[idx],
                do_muses = True,
            ) : eventnr
            for eventnr, idx in event_gb.groups.items()
        }
        print('All tasks have been issued!')

        for future in concurrent.futures.as_completed(futures):
            eventnr = futures[future]
            event = future.result()
            event_dict[eventnr] = []
    
    return pd.Series(event_dict).rename('MuEvent').to_frame().sort_index()