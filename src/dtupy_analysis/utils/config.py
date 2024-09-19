from muTel.utils.meta import parent
import json
import itertools

config_path = {
    'filter'   :   parent+'src/muTel/dqm/config/filters.json'
}

def update_cfg_path(object_name,new_path):
    global config_path
    config_path[object_name] = new_path


# Función para cargar la configuración guardada de un objeto
def load_cfg(target,config_name="default"):
    with open(config_path[target.__cfg__],'r') as cfg_file:
        params = json.load(cfg_file)['filters'][target.__name__][config_name]
    return target(**params)

def gen_lat_json():
    comb = itertools.product('LR',repeat=4)
    lat_to_coef = lambda key: dict(L=-1,R=1)[key]

    lat_dict = {''.join(comb_i) : list(map(lat_to_coef,comb_i)) for comb_i in comb}


    for i,e in lat_dict.items():
        print(f'{i} : {e}')
    path = '/home/nfs/user/martialc/muTel_work/muTel_v02/muTel/src/muTel/dqm/config/laterality.json'
    with open(path, 'w+') as file:
        json.dump(lat_dict,file,indent=4)

