import os
import argparse
from minio import Minio
arg = argparse.ArgumentParser()
arg.add_argument("--s3", type = str)
arg.add_argument("--access_key", type = str)
arg.add_argument("--secret_key", type = str)
arg.add_argument("--dir", type = str, default = '')

options = vars(arg.parse_args())
f = open('database.txt','a')
if (options['dir'] != ''):
	f.write(options['dir']+'\n')
f.close()
input_paths = []
with open('database.txt') as input_data:
	for line in input_data:
		input_paths.append(line)
f.close()
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
			if (os.path.getmtime(arr[i]) < os.path.getmtime(file)):
				arr[i] = file
			return
		i+=1 
	arr.append(file)
def prepare_to_sync(paths):
	folders = []
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
				update_time(file, to_new)
		cnt+=1

def sync(paths):
	prepare_to_sync(paths)
	#first - update
	for file in to_update:
		client.fput_object('alexey', file[file.find('/')+1:], file)
	#second - delete
	for file in to_delete:
		client.remove_object('alexey', file)
	#third - new
	for file in to_new:
		client.fput_object('alexey', file[file.find('/')+1:], file)
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
				os.remove(file)
			else:
				client.fget_object('alexey',file[file.find('/')+1:], file)
	for path in paths:
		for file in to_new:
			client.fget_object('alexey',file[file.find('/')+1:], path+'/'+file[file.find('/')+1:])
sync(paths)