from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


class Image_Factory(object):
    '''
    The Image_Factory takes a Composite_Frame object and a directory to save images
    in and populates the directory with heat map images derived from network resolutions
    in the Composite_Frame
    '''
    def __init__(self, csv_path: Path, img_path: Path):
        '''
        Instance Variables:
            @self.img_path -> path object to write heat maps too
            @self.label -> label to use in naming images
            @self.img_limit -> how many images to create 
        '''
        self.img_path = img_path
        self.label = str(img_path).split('/')[-1]
        self.img_count = 0

        self._create_heatmaps(csv_path)

    def _create_heatmaps(self, csv_path: Path):
        _, ax = plt.subplots(1, 1, figsize=(1, 1), dpi=23)

        for file in csv_path.iterdir():
            frame = pd.read_csv(file, usecols=['saddr', 'stime', 'ltime', 'tbytes'],

                                dtype={'saddr': str, 
                                        'stime': np.float, 
                                        'ltime': np.float,
                                        'tbytes': np.int
                                        }
                                )
            frame = frame.filter(['saddr', 'tbytes']).groupby('saddr').sum().reset_index()

            frame = pd.pivot_table(frame, values='tbytes', index='saddr', columns=None)

            _ = sns.heatmap(frame, xticklabels=False, yticklabels=False, cbar=False)

            ax.set_ylabel('')
            ax.set_xlabel('')

            img_name = self.label + str(self.img_count)
            plt.savefig(self.img_path / img_name)

            self.img_count += 1
