import matplotlib as mpl; mpl.use('Agg')
import matplotlib.pyplot as plt
import pandas

from ..Plot import CMSPlot


class TDCstd_vs_ch(CMSPlot):
    def plot(self, df : 'pandas.DataFrame', hist_cfg_dict, debug = False):
        # Compute the standard deviation per channel
        std_by_ch = df.groupby('channel')['tdc'].std()
        
        # Plot the histogram
        self.ax.bar(std_by_ch.index, std_by_ch.values)
        
        # Compute global standard deviation and the mean of the std for each channel
        glob_std = df.tdc.std()
        mean_std = std_by_ch.mean()
        
        self.ax.axhline(mean_std,zorder=5,color='r',linestyle='dashed', label='Mean TDC deviation per channel')
        self.ax.axhline(glob_std,zorder=5,color='k',linestyle='dashed', label = 'TDC deviation')
        
        if debug:
            from termcolor import colored, cprint

            print('\n'
                '#====================================#\n'
                '| TDC NOISY CHANNELS REPORT: LINK {l:2d} |\n'.format(l=df.link.values[0])+
                '#====================================#\n'
            )
            noisy_ch = std_by_ch[std_by_ch/mean_std >= 1]
            if len(noisy_ch) > 0:
                print(f'Global std           : {glob_std:3.2f}')
                print(f'Mean std per channel : {mean_std:3.2f}')
                
                print(f'\nChannels with higher-than-average std (sorted by TDC std):')
                for ch, std in noisy_ch.sort_values(ascending=False).items():
                    rel_std = std/mean_std
                    
                    cprint(
                        f'  link {df[df.channel == ch].link.values[0]:2d} ch {ch:3d} : {std:5.2f} ({rel_std:3.1f}̄σ)',
                        'yellow' if rel_std < 2 else 'red' if rel_std < 3 else None,
                        None                               if rel_std < 3 else 'on_red'
                    )
            else:
                print('\nNo noisy channels in sample! :D')
            
        self.ax.set_xlim(std_by_ch.index.min(), std_by_ch.index.max())
        self.ax.set_xlabel('Channel')
        self.ax.set_ylabel(r'Width of TDC distribution, $\sigma (TDC)$ (arb. units.)')
        
        plt.tight_layout()
        
        plt.legend()

