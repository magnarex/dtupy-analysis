def jin2int(obdt_type, obdt_ctr):
    if obdt_type == 'theta':
        # Suponiendo que obdt_ctr sea de la forma '*[1-N][a-b]'
        return 2*(int(obdt_ctr[-2]) - 1) + (ord(obdt_ctr[-1].lower()) - ord('a'))
    elif obdt_type == 'phi':
        return int(obdt_ctr[3:]) - 1
    else:
        raise ValueError('Invalid obdt_type value, only "theta" or "phi" allowed.')

def int2jin(obdt_type : int, obdt_ctr_int : int , upper : bool = False):
    if obdt_type == 0:
        jin_str = chr(obdt_ctr_int % 2 + ord("a"))
        if upper: jin_str = jin_str.upper()
        return f'jin{obdt_ctr_int//2+1}{jin_str}'
    
    elif obdt_type == 1:
        return f'jin{obdt_ctr_int + 1}'
    
    else:
        raise ValueError('Invalid obdt_type value, only 0 or 1 allowed.')


def obdtc2int(obdt_type, obdt_ctr):
    # Convert connector to integer
    try:
        return int(obdt_ctr) - 1
     
    except ValueError as err:
        # jin1a -> 2*(1 - 1) + 0 = 0
        # jin1b -> 2*(1 - 1) + 1 = 1
        # ...
        if 'jin' in obdt_ctr:
            return jin2int(obdt_type, obdt_ctr)
        else:
            raise err

def int2obdtc(obdt_type_int, obdt_ctr_int, jin = True):
    # Convert connector to integer
    if jin:
        return int2jin(obdt_type_int, obdt_ctr_int)
    else:
        return obdt_ctr_int + 1

def obdt2int(obdt_type, obdt_ctr = None):
    # Convert connector to integer
    obdt_type_int = 0 if obdt_type == 'theta' else\
                    1 if obdt_type == 'phi'   else\
                    None
    
    if obdt_type_int is None:
        raise ValueError('Invalid obdt_type value, only "theta" or "phi" allowed.')
    
    if obdt_ctr is None:
        return obdt_type_int
    else:
        return obdt_type_int, obdtc2int(obdt_type, obdt_ctr)

def int2obdt(obdt_type_int, obdt_ctr_int = None, jin = True):
    # Convert connector to integer
    obdt_type = 'theta' if obdt_type_int == 0 else\
                'phi'   if obdt_type_int == 1  else\
                None
    
    if obdt_type_int is None:
        raise ValueError('Invalid obdt_type_int value, only 0 or 1 allowed.')
    
    return obdt_type, int2obdtc(obdt_type_int, obdt_ctr_int, jin = jin)