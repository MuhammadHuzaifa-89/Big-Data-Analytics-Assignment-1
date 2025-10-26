from hdfs import InsecureClient
from dotenv import load_dotenv
import os

# Loading environment variables
load_dotenv()

# Getting HDFS connection settings
host = os.getenv("HDFS_HOST")
port = os.getenv("HDFS_PORT")
user = os.getenv("HDFS_USER")

# Connecting to HDFS
client = InsecureClient(f"http://{host}:9870", user=user)
print("Connected to HDFS!")

# Directory for storing files
dir_path = "/data_files"
client.makedirs(dir_path)


# CRUD Operations

# CREATE
def create_file(filename, content):
    file_path = f"{dir_path}/{filename}"
    with client.write(file_path, encoding="utf-8", overwrite=True) as writer:
        writer.write(content)
    print(f"File created: {file_path}")


# READ
def read_file(filename):
    file_path = f"{dir_path}/{filename}"
    try:
        with client.read(file_path, encoding="utf-8") as reader:
            data = reader.read()
            print(f"\nFile Content ({filename}):\n{data}")
    except Exception as e:
        print(f"Error reading file: {e}")


# UPDATE
def update_file(filename, new_content):
    file_path = f"{dir_path}/{filename}"
    if not client.status(file_path, strict=False):
        print(f"File {filename} does not exist.")
        return
    with client.write(file_path, encoding="utf-8", overwrite=True) as writer:
        writer.write(new_content)
    print(f"File updated: {file_path}")


# DELETE
def delete_file(filename):
    file_path = f"{dir_path}/{filename}"
    try:
        client.delete(file_path)
        print(f"File deleted: {file_path}")
    except Exception as e:
        print(f"Error deleting file: {e}")


# Test CRUD
if __name__ == "__main__":
    create_file("test1.txt", "This is a test file.")
    create_file("test2.txt", "Second file content.")

    read_file("test1.txt")

    update_file("test1.txt", "Updated content for test1.txt")
    read_file("test1.txt")

    delete_file("test2.txt")

    print("\nFinal Files in HDFS Directory:")
    for f in client.list(dir_path):
        print(" -", f)
