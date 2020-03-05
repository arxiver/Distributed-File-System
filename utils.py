import imageio
import os 
import numpy as np
#import cv2 as cv
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

def withdraw(balance, lock):     
    for _ in range(10000): 
        lock.acquire() 
        balance.value = balance.value - 1
        lock.release() 
  
# function to deposit to account 
def deposit(balance, lock):     
    for _ in range(10000): 
        lock.acquire() 
        balance.value = balance.value + 1
        lock.release() 
  
def perform_transactions(): 
  
    # initial balance (in shared memory) 
    balance = multiprocessing.Value('i', 100) 

    # creating a lock object 
    lock = multiprocessing.Lock() 

    # creating new processes 
    p1 = multiprocessing.Process(target=withdraw, args=(balance,lock)) 
    p2 = multiprocessing.Process(target=deposit, args=(balance,lock)) 
  
    # starting processes 
    p1.start() 
    p2.start() 
  
    # wait until processes are finished 
    p1.join() 
    p2.join() 
  
    # print final balance 
    print("Final balance = {}".format(balance.value)) 
    # initial balance (in shared memory) 
    balance = multiprocessing.Value('i', 100) 

    # creating a lock object 
    lock = multiprocessing.Lock() 


if (__name__ == '__main__'):
    print("Hello Utils")
