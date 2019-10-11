from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from composite_frame import Composite_Frame


class Image_Factory(object):
    '''
    The Image_Factory takes a Composite_Frame object and a directory to save images
    in and populates the directory with heat map images derived from network resolutions
    in the Composite_Frame
    '''

    def __init__(self, composite_frame: Composite_Frame, img_path: Path, img_limit: int = -1):
        '''
        Instance Variables:
            @self.img_path -> path object to write heat maps too
            @self.label -> label to use in naming images
            @self.img_limit -> how many images to create 
        '''
        self.img_path = img_path
        self.label = str(img_path).split('/')[-1]

        if img_limit < 0:
            self.img_limit = len(composite_frame.items)
        else:
            self.img_limit = img_limit

        self._create_heatmaps(composite_frame)

    def _create_heatmaps(self, composite_frame: Composite_Frame):
        _, ax = plt.subplots(1, 1, figsize=(1, 1), dpi=23)
        img_id = 0

        for frame in composite_frame.items:
            # Extract necessary information and eliminate duplicate ip addresses
            frame = frame.filter(['saddr', 'TnBPSrcIP']).groupby(
                'saddr').sum().reset_index()

            frame = pd.pivot_table(
                frame, values='TnBPSrcIP', index='saddr', columns=None)

            _ = sns.heatmap(frame, xticklabels=False,
                            yticklabels=False, cbar=False)

            ax.set_ylabel('')
            ax.set_xlabel('')

            img_name = self.label + str(img_id)
            plt.savefig(self.img_path / img_name)

            img_id += 1

            if img_id > self.img_limit:
                break
