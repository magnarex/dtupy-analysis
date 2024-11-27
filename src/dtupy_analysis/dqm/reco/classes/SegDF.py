import pandas as pd

class SegDF(pd.DataFrame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # TODO: No efficient way to compute this
        self['min_hit'] = self.seg.apply(min)
        self['max_hit'] = self.seg.apply(max)
        
    @classmethod
    def read_parquet(self, output):
        from ....utils.paths import get_file, data_directory
        return SegDF(pd.read_parquet(get_file(output, data_directory, ['.seg'])))
    
    def get_segs(self, data):
        return SegDF(self[(self.min_hit >= data.index.min()) & (self.max_hit <= data.index.max())].copy(deep=True))