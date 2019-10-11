import sys
sys.path.append('./components')
from pathlib import Path

import pandas as pd
import numpy as np

from components.composite_frame import Composite_Frame
from components.image_factory import Image_Factory


def get_frame():
    csv_path = Path.cwd() / 'All features'

    li = []
    for file in csv_path.iterdir():
        flow_df = pd.read_csv(file, usecols=['pkSeqID', 'stime', 'proto', 'saddr',
                                            'sport', 'daddr', 'dport', 'attack',
                                            'category', 'subcategory', 'TnBPSrcIP',
                                            'ltime'],

                            dtype={'pkSeqID': np.int, 'stime': np.float, 'proto': str,
                                    'saddr': str, 'sport': str, 'daddr': str, 'dport': str,
                                    'attack': bool, 'category': str, 'subcategory': str,
                                    'TnBPSrcIP': np.int64, 'ltime': np.float}
                            )

        li.append(flow_df)

    df = pd.concat(li, axis=0, ignore_index=True)

    return df

def main():
    df = get_frame()

    dirty_traffic = df[df.attack == True]
    dirty_traffic = dirty_traffic[dirty_traffic.category == 'DDoS']
    dirty_traffic = dirty_traffic[dirty_traffic.subcategory == 'TCP']

    dirty_composite = Composite_Frame(dirty_traffic, 60, 150)

    img_path = Path.cwd() / 'heat_maps' / 'ddos_tcp'
    img_path.mkdir(exist_ok=True)
    _ = Image_Factory(dirty_composite, img_path, 150)

if __name__ == '__main__':
    main()
