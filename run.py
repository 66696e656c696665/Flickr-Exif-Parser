# from exif import Image
import base64
import multiprocessing
import pickle
import re
import time
import urllib
from multiprocessing import Process, Queue

import numpy as np
import piexif
import requests
from bs4 import BeautifulSoup



"""
method how to write saved data in new image
with open('saved_exif.txt', 'r', encoding='ascii') as f:
    path_to_image = './original (11).jpg'
    lines = f.readlines()
    # lines[365] a specific exif string is specified, it is better to put a random string
    exif_saved_data = pickle.loads(base64.b64decode(lines[365]))
    exif_dict = piexif.load(path_to_image)
    # check orientation on original image
    if piexif.ImageIFD.Orientation in exif_dict['0th']:
        orientation = exif_dict['0th'][piexif.ImageIFD.Orientation]
        exif_saved_data['0th'][piexif.ImageIFD.Orientation] = orientation
    else:
        # set default flag orientation
        exif_saved_data['0th'][piexif.ImageIFD.Orientation] = 1
    piexif.insert(piexif.dump(exif_saved_data), path_to_image)"""



def split(arr, size):
    arrs = []
    while len(arr) > size:
        pice = arr[:size]
        arrs.append(pice)
        arr = arr[size:]
    arrs.append(arr)
    return arrs


def update_soup(page):
    return BeautifulSoup(page.text, 'lxml')


def req_to_url(url):
    page = requests.get(url)
    if page.status_code != 200:
        exit('Server error')
    return page


def parse_exif(data):
    exif_dict = piexif.load(data)
    # del software name, because it can be photo editors
    if piexif.ImageIFD.Software in exif_dict['0th']:
        del exif_dict['0th'][piexif.ImageIFD.Software]
    # del MakerNote
    if piexif.ExifIFD.MakerNote in exif_dict['Exif']:
        del exif_dict['Exif'][piexif.ExifIFD.MakerNote]
    # if piexif.ImageIFD.Orientation in exif_dict['0th']:
    # need to replace orientation from original
    # exif_dict['0th'][piexif.ImageIFD.Orientation] = 1

    del exif_dict['GPS']
    del exif_dict['thumbnail']
    # del exif_dict['1st']
    del exif_dict['Interop']
    base64_string = base64.b64encode(pickle.dumps(exif_dict)).decode('ascii') + "\n"
    return base64_string


def get_links_photo(links):
    pic_links = []
    for l in links:
        page = req_to_url(l)
        soup = update_soup(page)
        for tag in soup.select('div.hover-target > div.thumb > span.photo_container > a[data-track="photo-click"]'):
            pic_links.append(urllib.parse.urljoin(l, tag.attrs['href']))
    return pic_links


def get_original_photo(q, links):
    for l in links:
        try:
            page = req_to_url(l)
            match = re.search(r'(?<=\"o\":\{\"displayUrl\":\").+?(?=\",\"width\":)', page.text)
            if match:
                image_url = "https:" + match[0].replace("\\", "")
                # u = urllib.parse.urlparse(image_url)
                u = requests.get(image_url)
                # f = io.BytesIO()
                # f.write(u.content)
                encode_exif = parse_exif(u.content)
                q.put(encode_exif)
        except Exception:
            pass


def save_data_from_queue(q):
    # If the queue is empty, queue.get() will block until the queue has data
    i = 0
    with open('./saved_exif.txt', 'a', encoding='ascii') as f:
        while True:
            try:
                if (i / 100).is_integer():
                    print('Parsed: ' + str(i))
            except Exception:
                pass
            _exif = q.get()
            f.write(_exif)
            i = i + 1


if __name__ == '__main__':
    cameras = 'https://www.flickr.com/cameras/'
    page = req_to_url(cameras)
    soup = update_soup(page)

    # Parse and save url brands cameras
    cameras_arr = []
    for link in soup.select('table#all-brands > tr > td:nth-of-type(2) > a'):
        cameras_arr.append(urllib.parse.urljoin(cameras, link.attrs['href']))

    # Parse and save url CameraPhones
    device_arr = []
    for cam in cameras_arr:
        page = req_to_url(cam)
        soup = update_soup(page)
        items = soup.select('table#all-cameras > tr > td:-soup-contains("Cameraphone")')
        try:
            for i in items:
                href = i.parent.select_one('td:nth-of-type(2) > a').attrs['href']
                device_arr.append(urllib.parse.urljoin(cam, href))
        except Exception as e:
            print(e)
        print('Parsed devices from: ' + cam)
        time.sleep(1)

    # split the array into parts
    device_arr = np.array_split(np.array(device_arr), 4)

    # start pool processes for parse links of photo
    pool = multiprocessing.Pool()
    res = pool.map(get_links_photo, device_arr)

    # merge array results
    photo_arr = np.concatenate([np.array(i) for i in res])

    # split the array into parts
    photo_arr = np.array_split(np.array(photo_arr), 4)

    # Create the Queue object
    queue = Queue()

    producers = []
    consumers = []

    # start pool processes for parse photo
    for piece in photo_arr:
        producers.append(Process(target=get_original_photo, args=(queue, piece)))

    # Create consumer processes
    p = Process(target=save_data_from_queue, args=(queue,))
    p.daemon = True
    consumers.append(p)

    for p in producers:
        p.start()

    for c in consumers:
        c.start()

    # join() for sync
    for p in producers:
        p.join()

    print('Parent process exiting...')
