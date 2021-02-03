import os

dirpath = './'
import pandas as pd

combine = pd.DataFrame()

for root, dirs, files in os.walk(dirpath):
    for file in files:
        # ʹ��join�������ļ����ƺ��ļ����ڸ�Ŀ¼��������
        file_dir = os.path.join(root, file)
        file_dir = file_dir.replace("\\", "/")

        if file_dir[-4:] == 'xlsx':
            dat = pd.read_excel(file_dir, sheet_name=0)
            dat['file_dir'] = file_dir
            combine = combine.append(dat)
            print(file_dir)
        else:
            continue

combine.to_csv('combine.csv', sep='|', index=False, quoting=1, encoding='GB18030')