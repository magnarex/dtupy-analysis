import pathlib, inspect
import yaml
here = pathlib.Path(inspect.getfile(inspect.currentframe())).parent

def get_src():
    where_src = [i for i, part in enumerate(here.parts) if part == 'src']
    return pathlib.Path('').joinpath(*here.parts[:where_src[0]])

parent_directory = get_src()
data_directory   = parent_directory / pathlib.Path('data')
config_directory = parent_directory / pathlib.Path('cfg')


def to_path(path):
    if isinstance(path,str):
        return pathlib.Path(path)
    elif isinstance(path, pathlib.Path):
        return path
    else:
        raise TypeError(f"Type of path is not valid. Expected str or pathlib.Path, not {type(path)}")


def is_valid(path):
    
    path = to_path(path)
    
    if path.exists():
        return True
    else:
        try:
            # print(f'Trying to touch {path}')
            path.touch()
            path.unlink()
            return True
        except OSError:
            print(f"Can't touch {path}")
            return False
            
def to_path_with_suffix(path,extensions):
    path = to_path(path)
    if path.suffix in extensions:
        return path
    elif path.suffix == '':
        for ext in extensions:
            if is_valid(path.with_suffix(ext)):
                return path.with_suffix(ext)
            # else:
            #     print(f'{path.with_suffix(ext)} not valid.')
        raise IOError(f'No path was found for a YAML file with path {path}')
    else:
        raise NotImplementedError(f'Extension {path.suffix} is not supported for this method.')

def load_yaml(path):
    path = to_path_with_suffix(path,['.yaml','.yml'])
    
    with open(path,'r') as file:
        return yaml.safe_load(file)

def load_json(path):
    path = to_path_with_suffix(path,['.json'])
    
    with open(path,'r') as file:
        return json.load(file)


if __name__ == '__main__':
    print(get_src())