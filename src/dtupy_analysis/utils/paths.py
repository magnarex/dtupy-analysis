import pathlib, inspect
from collections.abc import Iterable
from typing import Any
import yaml

here = pathlib.Path(inspect.getfile(inspect.currentframe())).parent

def get_src():
    where_src = [i for i, part in enumerate(here.parts) if part == 'src'][0]
    return pathlib.Path('').joinpath(*here.parts[:where_src])

parent_directory = get_src()
data_directory   = parent_directory / pathlib.Path('data')
config_directory = parent_directory / pathlib.Path('cfg')
macros_directory = parent_directory / pathlib.Path('macros')
condor_directory = parent_directory / pathlib.Path('condor')


def is_valid(path, debug = False):
    if isinstance(path,str):
        path = pathlib.Path(path)
    elif isinstance(path, pathlib.Path):
        pass
    else:
        raise TypeError(f"Type of path is not valid. Expected str or pathlib.Path, not {type(path)}")
    
    if path.exists():
        return True
    else:
        try:
            if debug: print(f'Trying to touch {path}')
            path.touch()
            path.unlink()
            return True
        except OSError as e:
            print(f"Can't touch {path}: {e}")
            return False

def get_with_default(path,default_dir,debug=False):
    if default_dir:
        if is_valid(default_dir / path,debug=debug):
            return default_dir / path
        
    if is_valid(path,debug=debug):
        return path
    else:
        return None

       
def get_file(path, default_dir = None, suffix = None, debug=False):
    path = pathlib.Path(path)    
    
    if suffix is None:
        return get_with_default(path,default_dir,debug=debug)
    elif isinstance(suffix, str):
        return get_with_default(path.with_suffix(suffix), default_dir,debug=debug)
    elif isinstance(suffix, Iterable):
        for sfx_i in suffix:
            path = get_with_default(path.with_suffix(sfx_i), default_dir,debug=debug)
            if path.exists(): return path
            
        raise FileNotFoundError(f"Can't find a file with name {path} and extension in {suffix}!")




def get_yaml(path, default_dir = None):
    return get_file(path,default_dir,['.yaml', '.yml'])


def get_root(path, default_dir = None):
    return get_file(path,default_dir,'.root') 

def load_yaml(path, default_dir = None) -> 'dict[str, Any]':
    """Read .yaml file and import it as a dictionary.
    """
    path = get_yaml(path, default_dir)
    
    with open(path,'r') as file:
        return yaml.safe_load(file)

    
    

if __name__ == '__main__':
    print(get_src())
    print(get_yaml('DYto2Mu',data_directory))
    print(get_root('test_1',data_directory))