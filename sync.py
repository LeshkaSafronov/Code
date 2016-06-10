import os
import argparse
import time
from minio import Minio
arg = argparse.ArgumentParser()
arg.add_argument("--s3", type = str)
arg.add_argument("--access_key", type = str)
arg.add_argument("--secret_key", type = str)
arg.add_argument("--dir", type = str, default = [])
options = vars(arg.parse_args())
def get_folders():
	fl = []
	try:
		with open('database.txt') as input_data:
			for line in input_data:
				fl.append(line)
			input_data.close()
	finally:
		return fl
if (len(options['dir'])):
	if (not (options['dir']+'\n' in get_folders())):
		f = open('database.txt','a')
		f.write(options['dir']+'\n')
		f.close()
else:
	input_paths = []
	with open('database.txt') as input_data:
		for line in input_data:
			input_paths.append(line)
	paths = []
	for path in input_paths:
		paths.append(path[:len(path)-1])
client = Minio(options['s3'],
               options['access_key'],
               options['secret_key'],
               secure=False)
to_new, to_delete, to_update = [], [], []
def get_all_source(path, list_of_files):
	data = os.listdir(path)
	for item in data:
		if os.path.isdir(path+'/'+item):
			get_all_source(path+'/'+item, list_of_files)
		else:
			list_of_files.append(path+'/'+item)
def update_time(file, arr):
	i, len_arr = 0, len(arr)
	while (i < len_arr):
		if (arr[i][arr[i].find('/')+1:] == file[file.find('/')+1:]):
			try:
				if (os.path.getmtime(arr[i]) < os.path.getmtime(file)):
					arr[i] = file
			except Exception:
				pass
			return
		i+=1 
	arr.append(file)
def prepare_to_sync(paths):
	folders = []
	to_new.clear()
	for path in paths:
		a = []
		get_all_source(path,a)
		folders.append(a)
	sf = client.list_objects('alexey', recursive = True)
	server_files = []
	for file in sf:
		server_files.append(file.object_name)
	cnt = 0
	for folder in folders:
		name_of_dir = paths[cnt]
		for file in server_files:
			if (not (name_of_dir+'/'+file in folder)):
				to_delete.append(file)
			else:
				update_time(name_of_dir+'/'+file, to_update)
		cnt+=1
	cnt = 0
	for folder in folders:
		name_of_dir = paths[cnt]
		for file in folder:
			if (not (file[file.find('/')+1:] in server_files)):
				print("Hello")
				update_time(file, to_new)
		cnt+=1
def sync(paths):
	prepare_to_sync(paths)
	print(to_new)
	#first - update
	for file in to_update:
		try:
			if (client.stat_object('alexey',file[file.find('/')+1:]).last_modified < os.path.getmtime(file)):
				print("upload")
				client.fput_object('alexey', file[file.find('/')+1:], file)
				time = client.stat_object('alexey',file[file.find('/')+1:])
				os.utime(file,(time.last_modified,time.last_modified))
		except Exception:
			pass
	#second - delete
	for file in to_delete:
		client.remove_object('alexey', file)
	#third - new
	for file in to_new:
		try:
			client.fput_object('alexey', file[file.find('/')+1:], file)
			time = client.stat_object('alexey',file[file.find('/')+1:])
			os.utime(file,(time.last_modified,time.last_modified))
		except Exception:
			pass
	folders = []
	for path in paths:
		a = []
		get_all_source(path,a)
		folders.append(a)
	sf = client.list_objects('alexey', recursive = True)
	server_files = []
	for file in sf:
		server_files.append(file.object_name)
	for folder in folders:
		for file in folder:
			if (not (file[file.find('/')+1:] in server_files)):
				try:
					os.remove(file)
				except Exception:
					pass
			else:
				print(client.stat_object('alexey',file[file.find('/')+1:]).last_modified, os.path.getmtime(file))
				if (client.stat_object('alexey',file[file.find('/')+1:]).last_modified > os.path.getmtime(file)):
					print("download")
					client.fget_object('alexey',file[file.find('/')+1:], file)						
					time = client.stat_object('alexey',file[file.find('/')+1:])
					os.utime(file,(time.last_modified,time.last_modified))
	for path in paths:
		for file in to_new:
			try:
				client.fget_object('alexey',file[file.find('/')+1:], path+'/'+file[file.find('/')+1:])
				time = client.stat_object('alexey',file[file.find('/')+1:])
				os.utime(path+'/'+file[file.find('/')+1:],(time.last_modified,time.last_modified))
			except Exception:
				pass
if (len(options['dir']) == 0):
	while True:
		sync(paths)
		time.sleep(5)
