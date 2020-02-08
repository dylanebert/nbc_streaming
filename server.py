import os
import socket
import pandas as pd
import numpy as np
import json

def get_frame(data):
    try:
        frame_rows = [json.loads(line) for line in data.split('\n')[:-1]]
        frame = pd.DataFrame(frame_rows)
        buffer.append(frame)
    except:
        print('Error processing frame: {}'.format(data))

def get_response(buffer):
    df = pd.concat(buffer)
    df = df[(df['dynamic'] == True) & ~(df['name'].isin(['LeftHand', 'RightHand', 'Head']))]
    df['speedY'] = df.apply(lambda row: np.abs(row['velY']), axis=1)
    motion = df.groupby('name').mean()['speedY']
    obj = motion.idxmax()
    if motion.loc[obj] < 1e-3:
        return 'na'
    return obj

if __name__ == '__main__':
    HOST = ''
    PORT = 8000
    buffer = []
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(1)

    print('Listening...')

    conn, addr = s.accept()
    print('Connection from {}'.format(addr))

    while 1:
        data = conn.recv(65536)
        if not data:
            break
        data = data.decode('utf-8')
        if data[0] == 'f':
            get_frame(data[1:])
        elif data[0] == 'q':
            res = get_response(buffer)
            conn.sendto(res.encode(), (HOST, PORT))
            buffer = []
        else:
            print(data)

    conn.close()
