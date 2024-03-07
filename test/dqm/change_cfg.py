import muTel.utils.config as cfg_utils

# print(cfg_utils.config_path)

# cfg_utils.update_cfg_path('filter','C:/')

# print(cfg_utils.config_path)

from muTel.dqm.classes.filters import Drop
# filter = cfg_utils.load_cfg(Drop)
filter = Drop(['a'])
print(type(filter))
print(filter)