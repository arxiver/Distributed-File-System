import pylab
import imageio
import os 
import numpy as np
import cv2 as cv
#import skimage as sk 

def video_reader(path='simb.mp4'):
    filename = os.getcwd()+'/'+'simb.mp4'
    vid = imageio.get_reader(filename,  'ffmpeg')
    return vid

def video_writer(vid):
    fps = vid.get_meta_data()['fps']
    writer = imageio.get_writer('./cockatoo_gray.mp4', fps=fps)
    for frame in vid:
        writer.append_data(frame[:, :, :])
    writer.close()
    print(vid.get_length())
    print(np.array(vid.get_data(10)).shape)

if (__name__ == '__main__'):
    print("Hello Utils")