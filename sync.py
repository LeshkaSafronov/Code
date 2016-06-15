import os, time, argparse
from minio import Minio
arg = argparse.ArgumentParser()
arg.add_argument("--s3", type = str, default = "")
arg.add_argument("--access_key", type = str, default = "")
arg.add_argument("--secret_key", type = str, default = "")
arg.add_argument("--dir", type = str, default = "")
options = vars(arg.parse_args())
for option in options.items():
	if (not len(option[1])):
		print("Пользователь не передал обязательный параметр командной строки")
		exit(1)
history = {}
client = Minio(options['s3'],
   	           options['access_key'],
       	       options['secret_key'],
           	   secure=False)
try:
	client.bucket_exists("alexey")
except Exception:
	print("Проверьте s3|access_key|secret_key")
	exit(2)
if (not client.bucket_exists("alexey")):
	client.make_bucket("alexey")
def get_list_of_files(path, list_of_files):
	files = os.listdir(path)
	for file in files:
		if os.path.isdir(path+'/'+file):
			get_list_of_files(path+'/'+file,list_of_files)
		else:
			list_of_files.append(path+'/'+file)
def sync_time(path, path_server):
	time = client.stat_object('alexey',path_server).last_modified
	os.utime(path,(time,time))
def sync(path):
	folder = []
	get_list_of_files(path, folder)
	server_files = client.list_objects('alexey', recursive=True)
	for file in server_files:
		if (path+'/'+file.object_name in folder):
			try:
				if (time.mktime(file.last_modified.timetuple()) > os.path.getmtime(path+'/'+file.object_name)):
					print('download')
					client.fget_object('alexey', file.object_name, path+'/'+file.object_name)
					sync_time(path+'/'+file.object_name, file.object_name)
				else:
					if (time.mktime(file.last_modified.timetuple()) < os.path.getmtime(path+'/'+file.object_name)):
						print('upload')
						client.fput_object('alexey', file.object_name, path+'/'+file.object_name)
						sync_time(path+'/'+file.object_name, file.object_name)
			except Exception:
				pass
			history[path+'/'+file.object_name] = True
		else:
			if (history.get(path+'/'+file.object_name)):
				try:
					print('remove')
					client.remove_object('alexey',file.object_name)
					os.remove(path+'/'+file.object_name)
				except Exception:
					pass
				history.pop(path+'/'+file.object_name)
			else:
				try:
					print('download')
					client.fget_object('alexey', file.object_name, path+'/'+file.object_name)
					sync_time(path+'/'+file.object_name, file.object_name)
				except Exception:
					pass
				history[path+'/'+file.object_name] = True
		try:
			folder.remove(path+'/'+file.object_name)
		except Exception:
			pass
	for file in folder:
		if (history.get(file)):
			try:
				print("remove")
				os.remove(file)
				history.pop(file)
			except Exception:
				pass
		else:
			try:
				print('upload')
				client.fput_object('alexey',file[len(path)+1:], file)
				sync_time(file,file[len(path)+1:])
				history[file] = True
			except Exception:
				pass
while 1:
	sync(options['dir'])
	time.sleep(5)
#develop by Leshka